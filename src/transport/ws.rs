//! WebSocket transport (feature `websocket`): wraps an already-connected
//! stream in `WsByteStream`, an `AsyncRead`/`AsyncWrite` adapter over
//! `tokio_tungstenite::WebSocketStream`, so `framing::read_packet` and
//! `write_packet` need not know WebSocket exists.
//!
//! `wss://` is not this module's concern: `transport::connect` TLS-wraps the
//! stream first (feature `tls`) and hands the result in here already
//! connected, so this module only ever performs the WebSocket half of
//! either combination.

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use futures_util::{Sink, Stream as _};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_tungstenite::tungstenite::client::{ClientRequestBuilder, IntoClientRequest};
use tokio_tungstenite::tungstenite::http::Uri;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::WebSocketStream;

use crate::cli::ConnectionArgs;
use crate::error::{Error, Result};

/// The MQTT WebSocket subprotocol name (MQTT-6.0): a server rejects the
/// HTTP Upgrade outright if it's missing, before any MQTT packet exists.
const MQTT_SUBPROTOCOL: &str = "mqtt";

/// The Upgrade request path. Not user-configurable yet: no broker in this
/// project's own test matrix needs anything but the common default, and
/// adding a flag nothing exercises is exactly the speculative surface
/// CLAUDE.md asks this crate to avoid.
const WS_PATH: &str = "/mqtt";

/// Perform the WebSocket Upgrade handshake over an already-connected
/// stream (plain TCP, or already TLS-wrapped for `wss://`).
pub async fn wrap<S>(stream: S, args: &ConnectionArgs) -> Result<WsByteStream<S>>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    let uri: Uri = format!("ws://{}:{}{WS_PATH}", args.broker, args.port())
        .parse()
        .map_err(|e| Error::Usage(format!("--broker {:?}: {e}", args.broker)))?;
    let request = ClientRequestBuilder::new(uri)
        .with_sub_protocol(MQTT_SUBPROTOCOL)
        .into_client_request()
        .map_err(|e| Error::Usage(format!("building the WebSocket Upgrade request: {e}")))?;

    let (ws, _response) = tokio_tungstenite::client_async(request, stream)
        .await
        .map_err(|e| Error::Usage(format!("WebSocket Upgrade failed: {e}")))?;

    Ok(WsByteStream::new(ws))
}

fn to_io_error(e: tokio_tungstenite::tungstenite::Error) -> io::Error {
    io::Error::other(e)
}

/// Adapts a `WebSocketStream` (a `Stream`/`Sink` of discrete `Message`s) to
/// `AsyncRead`/`AsyncWrite` (a byte stream), on the assumption every caller
/// already makes: one MQTT control packet is one write followed by one
/// flush (`framing::write_packet`). That assumption is what makes "one
/// flushed write = one WebSocket Binary frame" the right mapping — the
/// framing layer already treats a flush as a packet boundary, so this
/// adapter just carries that boundary onto the wire as a frame boundary.
#[derive(Debug)]
pub struct WsByteStream<S> {
    inner: WebSocketStream<S>,
    read_buf: Vec<u8>,
    read_pos: usize,
    write_buf: Vec<u8>,
    sending: bool,
}

impl<S> WsByteStream<S> {
    fn new(inner: WebSocketStream<S>) -> Self {
        WsByteStream {
            inner,
            read_buf: Vec::new(),
            read_pos: 0,
            write_buf: Vec::new(),
            sending: false,
        }
    }
}

impl<S> AsyncRead for WsByteStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        loop {
            if this.read_pos < this.read_buf.len() {
                let n = (this.read_buf.len() - this.read_pos).min(buf.remaining());
                buf.put_slice(&this.read_buf[this.read_pos..this.read_pos + n]);
                this.read_pos += n;
                return Poll::Ready(Ok(()));
            }
            this.read_buf.clear();
            this.read_pos = 0;

            match futures_util::ready!(Pin::new(&mut this.inner).poll_next(cx)) {
                Some(Ok(Message::Binary(bytes))) => {
                    this.read_buf = bytes;
                    // loop back around to drain it into `buf`
                }
                // A Close frame ends the stream like a clean TCP close would;
                // Text/Ping/Pong/Frame carry no MQTT bytes and are not this
                // adapter's job to act on (tungstenite already auto-queues
                // the Pong reply to a Ping internally — see its `read()`).
                Some(Ok(Message::Close(_))) | None => return Poll::Ready(Ok(())),
                Some(Ok(_)) => continue,
                Some(Err(e)) => return Poll::Ready(Err(to_io_error(e))),
            }
        }
    }
}

impl<S> AsyncWrite for WsByteStream<S>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    fn poll_write(
        self: Pin<&mut Self>,
        _cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        // Buffered only: a WebSocket frame boundary is decided at flush, not
        // at write, so accepting bytes here never blocks.
        self.get_mut().write_buf.extend_from_slice(buf);
        Poll::Ready(Ok(buf.len()))
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        let this = self.get_mut();
        loop {
            if this.sending {
                match Pin::new(&mut this.inner).poll_flush(cx) {
                    Poll::Ready(Ok(())) => {
                        this.sending = false;
                        return Poll::Ready(Ok(()));
                    }
                    Poll::Ready(Err(e)) => return Poll::Ready(Err(to_io_error(e))),
                    Poll::Pending => return Poll::Pending,
                }
            }
            if this.write_buf.is_empty() {
                // Nothing of ours to send, but still drive the sink's own
                // flush (e.g. an auto-queued Pong from a received Ping).
                return Pin::new(&mut this.inner)
                    .poll_flush(cx)
                    .map_err(to_io_error);
            }
            match Pin::new(&mut this.inner).poll_ready(cx) {
                Poll::Ready(Ok(())) => {
                    let bytes = std::mem::take(&mut this.write_buf);
                    Pin::new(&mut this.inner)
                        .start_send(Message::Binary(bytes))
                        .map_err(to_io_error)?;
                    this.sending = true;
                    // loop: drive the flush started above
                }
                Poll::Ready(Err(e)) => return Poll::Ready(Err(to_io_error(e))),
                Poll::Pending => return Poll::Pending,
            }
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner)
            .poll_close(cx)
            .map_err(to_io_error)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{duplex, AsyncReadExt, AsyncWriteExt};

    /// The Upgrade request must carry the `mqtt` subprotocol, or a
    /// spec-conformant broker rejects it at the HTTP layer before any MQTT
    /// packet is exchanged.
    #[test]
    fn the_upgrade_request_carries_the_mqtt_subprotocol() {
        let uri: Uri = "ws://localhost:1883/mqtt".parse().unwrap();
        let request = ClientRequestBuilder::new(uri)
            .with_sub_protocol(MQTT_SUBPROTOCOL)
            .into_client_request()
            .unwrap();
        let header = request
            .headers()
            .get("sec-websocket-protocol")
            .expect("Sec-WebSocket-Protocol header is present");
        assert_eq!(header, "mqtt");
    }

    /// A round trip through a real client/server WebSocket handshake and
    /// frame exchange, over an in-memory duplex pair standing in for a
    /// socket — proves the byte-stream adapter carries bytes intact in both
    /// directions, across a write/flush boundary that spans more than the
    /// payload of a single `poll_write` call.
    // `tungstenite::handshake::server::Callback::on_request`'s `Result`
    // shape (large `ErrorResponse` variant) is the library's, not this
    // crate's to shrink — same accommodation `src/mqtt/mod.rs` makes for the
    // CONNECT rejection path.
    #[allow(clippy::result_large_err)]
    #[tokio::test]
    async fn bytes_survive_a_websocket_round_trip() {
        let (client_io, server_io) = duplex(4096);

        let uri: Uri = "ws://localhost/mqtt".parse().unwrap();
        let request = ClientRequestBuilder::new(uri)
            .with_sub_protocol(MQTT_SUBPROTOCOL)
            .into_client_request()
            .unwrap();

        let server = tokio::spawn(async move {
            // A real MQTT-supporting broker echoes the subprotocol back;
            // tungstenite's client rejects the handshake otherwise, so a
            // server stub that omits this doesn't stand in for one.
            let ws = tokio_tungstenite::accept_hdr_async(server_io, |_req: &_, mut response: tokio_tungstenite::tungstenite::handshake::server::Response| {
                response.headers_mut().insert(
                    "sec-websocket-protocol",
                    MQTT_SUBPROTOCOL.parse().unwrap(),
                );
                Ok(response)
            })
                .await
                .expect("server-side handshake");
            let mut stream = WsByteStream::new(ws);
            let mut buf = [0u8; 5];
            stream.read_exact(&mut buf).await.expect("read hello");
            assert_eq!(&buf, b"hello");
            stream.write_all(b"world").await.expect("write world");
            stream.flush().await.expect("flush world");
        });

        let (ws, _response) = tokio_tungstenite::client_async(request, client_io)
            .await
            .expect("client-side handshake");
        let mut stream = WsByteStream::new(ws);
        stream.write_all(b"hello").await.expect("write hello");
        stream.flush().await.expect("flush hello");
        let mut buf = [0u8; 5];
        stream.read_exact(&mut buf).await.expect("read world");
        assert_eq!(&buf, b"world");

        server.await.expect("server task");
    }
}
