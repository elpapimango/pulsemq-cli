//! The byte stream a `Client` or a `bench` connection actually talks over:
//! plain TCP, TLS / mutual TLS (feature `tls`), WebSocket (feature
//! `websocket`), or both together for `wss://`.
//!
//! `Client` and `bench` both used to dial a bare `TcpStream` directly. This
//! module is the one place that dials now, so a transport added here serves
//! both call sites instead of being wired into each separately (TODO.md's
//! WebSocket item asked for exactly this).

use std::io;
use std::pin::Pin;
use std::task::{Context, Poll};

use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;

use crate::cli::ConnectionArgs;
use crate::error::Result;

#[cfg(feature = "tls")]
mod tls;
#[cfg(feature = "websocket")]
mod ws;

/// A connected transport, hiding which one behind a uniform
/// `AsyncRead`/`AsyncWrite`. Every variant wraps an already-connected,
/// already-`Unpin` inner stream, so the enum itself is `Unpin` for free —
/// no manual pin projection needed beyond the delegating poll methods below.
#[derive(Debug)]
pub enum Stream {
    Tcp(TcpStream),
    #[cfg(feature = "tls")]
    Tls(Box<tokio_rustls::client::TlsStream<TcpStream>>),
    #[cfg(feature = "websocket")]
    Ws(Box<ws::WsByteStream<TcpStream>>),
    #[cfg(all(feature = "tls", feature = "websocket"))]
    Wss(Box<ws::WsByteStream<tokio_rustls::client::TlsStream<TcpStream>>>),
}

impl AsyncRead for Stream {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Stream::Tcp(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(feature = "tls")]
            Stream::Tls(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(feature = "websocket")]
            Stream::Ws(s) => Pin::new(s).poll_read(cx, buf),
            #[cfg(all(feature = "tls", feature = "websocket"))]
            Stream::Wss(s) => Pin::new(s).poll_read(cx, buf),
        }
    }
}

impl AsyncWrite for Stream {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        match self.get_mut() {
            Stream::Tcp(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(feature = "tls")]
            Stream::Tls(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(feature = "websocket")]
            Stream::Ws(s) => Pin::new(s).poll_write(cx, buf),
            #[cfg(all(feature = "tls", feature = "websocket"))]
            Stream::Wss(s) => Pin::new(s).poll_write(cx, buf),
        }
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Stream::Tcp(s) => Pin::new(s).poll_flush(cx),
            #[cfg(feature = "tls")]
            Stream::Tls(s) => Pin::new(s).poll_flush(cx),
            #[cfg(feature = "websocket")]
            Stream::Ws(s) => Pin::new(s).poll_flush(cx),
            #[cfg(all(feature = "tls", feature = "websocket"))]
            Stream::Wss(s) => Pin::new(s).poll_flush(cx),
        }
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self.get_mut() {
            Stream::Tcp(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(feature = "tls")]
            Stream::Tls(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(feature = "websocket")]
            Stream::Ws(s) => Pin::new(s).poll_shutdown(cx),
            #[cfg(all(feature = "tls", feature = "websocket"))]
            Stream::Wss(s) => Pin::new(s).poll_shutdown(cx),
        }
    }
}

impl Stream {
    /// Split into independently-usable read and write halves — `bench`
    /// needs to write and read acknowledgements concurrently.
    ///
    /// The plain-TCP arm keeps `TcpStream::into_split()`'s zero-cost path
    /// (two handles sharing one duplicated file descriptor, no
    /// synchronization) rather than routing every build through the
    /// generic `tokio::io::split` (an `Arc<Mutex<_>>` under the hood):
    /// `bench` measures the broker, and CLAUDE.md is explicit that the
    /// harness must not add overhead of its own to what it measures. TLS
    /// has no equivalent zero-cost split, so it pays the generic path.
    pub fn into_split(
        self,
    ) -> (
        Box<dyn AsyncRead + Unpin + Send>,
        Box<dyn AsyncWrite + Unpin + Send>,
    ) {
        match self {
            Stream::Tcp(s) => {
                let (r, w) = s.into_split();
                (Box::new(r), Box::new(w))
            }
            #[cfg(feature = "tls")]
            Stream::Tls(s) => {
                let (r, w) = tokio::io::split(*s);
                (Box::new(r), Box::new(w))
            }
            #[cfg(feature = "websocket")]
            Stream::Ws(s) => {
                let (r, w) = tokio::io::split(*s);
                (Box::new(r), Box::new(w))
            }
            #[cfg(all(feature = "tls", feature = "websocket"))]
            Stream::Wss(s) => {
                let (r, w) = tokio::io::split(*s);
                (Box::new(r), Box::new(w))
            }
        }
    }
}

/// Dial the broker and return a connected transport, composing `--tls` and
/// `--websocket` as the flags say. The one place `Client` and `bench` both
/// call to connect.
pub async fn connect(args: &ConnectionArgs) -> Result<Stream> {
    #[cfg(feature = "tls")]
    args.warn_if_plaintext_password();

    let stream = TcpStream::connect((args.broker.as_str(), args.port())).await?;
    // One small packet at a time is this tool's whole traffic pattern;
    // Nagle would add up to 40 ms to every request/reply round trip. Set on
    // the raw socket before any wrapping, since TLS/WebSocket layer over it
    // rather than replace it.
    stream.set_nodelay(true)?;

    // wss:// first: --tls wraps the socket, then --websocket upgrades over
    // that already-TLS-wrapped stream — WebSocket never dials or verifies a
    // certificate itself, it only ever sees a stream this function already
    // finished connecting.
    #[cfg(all(feature = "tls", feature = "websocket"))]
    if args.tls && args.websocket {
        let tls_stream = tls::wrap(stream, args).await?;
        return ws::wrap(tls_stream, args)
            .await
            .map(Box::new)
            .map(Stream::Wss);
    }

    #[cfg(feature = "tls")]
    if args.tls {
        return tls::wrap(stream, args).await.map(Box::new).map(Stream::Tls);
    }

    #[cfg(feature = "websocket")]
    if args.websocket {
        return ws::wrap(stream, args).await.map(Box::new).map(Stream::Ws);
    }

    Ok(Stream::Tcp(stream))
}

// Cert-loading edge cases (missing file, empty file) are covered in
// `transport::tls`'s own tests. These cover the thing only `connect()` can:
// a real TLS handshake actually verifying (or, with --insecure, not
// verifying) a certificate.
#[cfg(all(test, feature = "tls"))]
mod tls_tests {
    use std::sync::Arc;

    use clap::Parser;
    use rcgen::{generate_simple_self_signed, CertifiedKey};
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::TcpListener;
    use tokio_rustls::rustls::ServerConfig;
    use tokio_rustls::TlsAcceptor;

    use super::*;
    use crate::cli::{Cli, Command};

    /// Regression: these tests run concurrently (the default test runner),
    /// and each spawns its own self-signed cert — a filename keyed only on
    /// the process id let two tests clobber the same path, so one test's
    /// client loaded another's cert as `--cafile` and failed signature
    /// verification against the server it actually talked to.
    fn write_temp(name: &str, contents: &str) -> std::path::PathBuf {
        static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let n = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let path = std::env::temp_dir().join(format!(
            "pulsemq-cli-test-{}-{n}-{name}",
            std::process::id()
        ));
        std::fs::write(&path, contents).expect("write temp file");
        path
    }

    fn conn_args(argv: &[&str]) -> ConnectionArgs {
        match Cli::parse_from(argv).command {
            Command::Pub(args) => args.conn,
            _ => panic!("expected the pub subcommand"),
        }
    }

    /// A TLS server on a self-signed cert for "localhost", listening on an
    /// OS-assigned port. Echoes back whatever it reads once, so a test can
    /// prove bytes actually cross the encrypted connection, not just that
    /// the handshake completed.
    async fn spawn_echo_server() -> (u16, std::path::PathBuf, tokio::task::JoinHandle<()>) {
        let CertifiedKey { cert, key_pair } =
            generate_simple_self_signed(vec!["localhost".to_string()])
                .expect("self-signed cert generates");
        let cert_path = write_temp("cert.pem", &cert.pem());

        let server_certs: Vec<_> = rustls_pemfile::certs(&mut cert.pem().as_bytes())
            .collect::<std::result::Result<_, _>>()
            .expect("parse the cert we just generated");
        let server_key = rustls_pemfile::private_key(&mut key_pair.serialize_pem().as_bytes())
            .expect("parse the key we just generated")
            .expect("a key is present");
        let server_config = ServerConfig::builder()
            .with_no_client_auth()
            .with_single_cert(server_certs, server_key)
            .expect("server config builds");
        let acceptor = TlsAcceptor::from(Arc::new(server_config));

        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind a scratch port");
        let port = listener.local_addr().expect("addr").port();

        let handle = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.expect("accept");
            let mut tls = acceptor
                .accept(stream)
                .await
                .expect("server-side handshake");
            let mut buf = [0u8; 5];
            tls.read_exact(&mut buf)
                .await
                .expect("read the client's bytes");
            assert_eq!(&buf, b"hello");
            tls.write_all(b"world").await.expect("echo back");
        });

        (port, cert_path, handle)
    }

    #[tokio::test]
    async fn tls_with_a_matching_cafile_verifies_and_carries_bytes() {
        let (port, cert_path, server) = spawn_echo_server().await;
        let port_s = port.to_string();
        let args = conn_args(&[
            "pulsemq-cli",
            "pub",
            "-t",
            "x",
            "-b",
            "localhost",
            "-p",
            &port_s,
            "--tls",
            "--cafile",
            cert_path.to_str().unwrap(),
        ]);

        let mut stream = connect(&args)
            .await
            .expect("TLS connects when --cafile matches the server's cert");
        assert!(matches!(stream, Stream::Tls(_)));
        stream.write_all(b"hello").await.expect("write over TLS");
        let mut buf = [0u8; 5];
        stream.read_exact(&mut buf).await.expect("read over TLS");
        assert_eq!(&buf, b"world");

        server.await.expect("server task");
        let _ = std::fs::remove_file(&cert_path);
    }

    /// Without a matching --cafile and without --insecure, a self-signed
    /// certificate the OS trust store has never heard of must be rejected —
    /// otherwise --tls would be verifying nothing by default.
    #[tokio::test]
    async fn tls_without_a_matching_cafile_or_insecure_is_rejected() {
        let (port, cert_path, server) = spawn_echo_server().await;
        let port_s = port.to_string();
        let args = conn_args(&[
            "pulsemq-cli",
            "pub",
            "-t",
            "x",
            "-b",
            "localhost",
            "-p",
            &port_s,
            "--tls",
        ]);

        connect(&args)
            .await
            .expect_err("an untrusted self-signed cert must fail verification");
        // The server's own handshake fails too, since the client aborted
        // mid-handshake on the rejected cert — that's expected here, not a
        // bug to assert on; just drain the task so it isn't left running.
        let _ = server.await;
        let _ = std::fs::remove_file(&cert_path);
    }

    #[tokio::test]
    async fn tls_insecure_accepts_an_unverified_certificate() {
        let (port, cert_path, server) = spawn_echo_server().await;
        let port_s = port.to_string();
        let args = conn_args(&[
            "pulsemq-cli",
            "pub",
            "-t",
            "x",
            "-b",
            "localhost",
            "-p",
            &port_s,
            "--tls",
            "--insecure",
        ]);

        let mut stream = connect(&args)
            .await
            .expect("--insecure connects despite the untrusted cert");
        stream.write_all(b"hello").await.expect("write over TLS");
        let mut buf = [0u8; 5];
        stream.read_exact(&mut buf).await.expect("read over TLS");
        assert_eq!(&buf, b"world");

        server.await.expect("server task");
        let _ = std::fs::remove_file(&cert_path);
    }
}
