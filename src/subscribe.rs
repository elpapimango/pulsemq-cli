//! `wispmq-cli sub` — subscribe, then print messages until interrupted or
//! until `--count` of them have arrived.

use std::io::Write;

use wispmq_protocol::codec::Properties;
use wispmq_protocol::packet::{Packet, PubAck, Publish, Subscribe, TopicFilter};
use wispmq_protocol::types::{QoS, ReasonCode};

use crate::cli::SubArgs;
use crate::client::Client;
use crate::error::{Error, Result};

pub async fn run(args: SubArgs) -> Result<()> {
    if args.topic.is_empty() {
        return Err(Error::Usage("sub needs at least one --topic".into()));
    }
    let qos = QoS::from_u8(args.qos)?;
    let limit = args.effective_limit();

    let mut client = Client::connect(&args.conn).await?;
    subscribe(&mut client, &args.topic, qos).await?;

    // An already-satisfied limit (`-n 0`, or `--max-messages` left at 0's
    // sibling case) must exit before the first `recv`, not after it — a
    // limit checked only post-receipt still blocks for one message.
    if limit == Some(0) {
        return client.disconnect().await;
    }

    let mut seen = 0u64;
    loop {
        match client.recv_keepalive().await? {
            Packet::Publish(p) => {
                acknowledge(&mut client, &p).await?;
                print_message(&p, args.show_topic);
                seen += 1;
                if limit.is_some_and(|c| seen >= c) {
                    break;
                }
            }
            // QoS 2 delivery from the broker: PUBREL closes it out.
            Packet::Pubrel(rel) => {
                let comp = PubAck::new(rel.packet_id, ReasonCode::Success);
                client.send(&Packet::Pubcomp(comp)).await?;
            }
            Packet::Disconnect(d) => {
                return Err(Error::Rejected {
                    what: "subscription".into(),
                    code: d.reason_code,
                })
            }
            _ => {}
        }
    }

    client.disconnect().await
}

/// Send one SUBSCRIBE carrying every filter and check the SUBACK. The broker
/// returns one Reason Code per filter, in order (3.9.3).
pub async fn subscribe(client: &mut Client, filters: &[String], qos: QoS) -> Result<()> {
    let packet_id = client.next_packet_id();
    let subscribe = Subscribe {
        packet_id,
        properties: Default::default(),
        filters: filters
            .iter()
            .map(|f| TopicFilter {
                filter: f.clone(),
                qos,
                no_local: false,
                retain_as_published: false,
                retain_handling: wispmq_protocol::packet::RetainHandling::SendAtSubscribe,
            })
            .collect(),
    };
    client.send(&Packet::Subscribe(subscribe)).await?;

    loop {
        match client.recv().await? {
            Packet::Suback(ack) => {
                for (filter, code) in filters.iter().zip(ack.reason_codes.iter()) {
                    if code.is_error() {
                        return Err(Error::Rejected {
                            what: format!("subscription to {filter}"),
                            code: *code,
                        });
                    }
                }
                return Ok(());
            }
            Packet::Disconnect(d) => {
                return Err(Error::Rejected {
                    what: "subscription".into(),
                    code: d.reason_code,
                })
            }
            // A retained message can reach us before the SUBACK does.
            _ => continue,
        }
    }
}

/// Acknowledge an inbound PUBLISH per its QoS. QoS 2 is completed on the
/// PUBREL that follows.
pub async fn acknowledge(client: &mut Client, publish: &Publish) -> Result<()> {
    let Some(id) = publish.packet_id else {
        return Ok(());
    };
    let ack = PubAck::new(id, ReasonCode::Success);
    match publish.qos {
        QoS::AtMostOnce => Ok(()),
        QoS::AtLeastOnce => client.send(&Packet::Puback(ack)).await,
        QoS::ExactlyOnce => client.send(&Packet::Pubrec(ack)).await,
    }
}

/// Write the payload as received. Payloads are arbitrary bytes, so they go to
/// stdout unmodified rather than through a lossy UTF-8 conversion.
pub fn print_message(publish: &Publish, show_topic: bool) {
    use std::io::IsTerminal;

    let stdout = std::io::stdout();
    let to_terminal = stdout.is_terminal();
    let mut out = stdout.lock();
    if show_topic {
        // Properties are broker-controlled strings too (content type, user
        // property keys/values), so they go through the same escaping as
        // the topic and payload before reaching a terminal.
        if let Some(line) = format_properties(&publish.properties) {
            write_payload(&mut out, &line, to_terminal);
            let _ = out.write_all(b"\n");
        }
        // The topic is broker-controlled too, and reaches the terminal on the
        // same line as the payload.
        write_payload(&mut out, publish.topic.as_bytes(), to_terminal);
        let _ = out.write_all(b" ");
    }
    write_payload(&mut out, &publish.payload, to_terminal);
    let _ = out.write_all(b"\n");
    let _ = out.flush();
}

/// Render the v5 properties `--show-topic` prints, one `key=value` per
/// property, space-separated. `None` when the PUBLISH carries none of them
/// (always the case on v3.x, which has no properties at all).
fn format_properties(properties: &Properties) -> Option<Vec<u8>> {
    let mut parts: Vec<String> = Vec::new();
    if let Some(v) = &properties.content_type {
        parts.push(format!("content-type={v}"));
    }
    if let Some(v) = properties.message_expiry_interval {
        parts.push(format!("message-expiry-interval={v}"));
    }
    if let Some(v) = properties.payload_format_indicator {
        parts.push(format!("payload-format-indicator={v}"));
    }
    for (k, v) in &properties.user_properties {
        parts.push(format!("user:{k}={v}"));
    }
    if parts.is_empty() {
        return None;
    }
    Some(parts.join(" ").into_bytes())
}

/// Write broker-supplied bytes to stdout, escaping control characters only
/// when the destination is a terminal.
///
/// Payloads are arbitrary bytes and this tool writes them through unmodified —
/// a pipe or a redirect gets exactly what arrived, so `sub > file` and
/// `sub | wc -c` stay honest, and no UTF-8 conversion happens anywhere.
///
/// A terminal is the exception, because there the bytes are not just data: a
/// hostile or compromised broker can emit ANSI escape sequences that reposition
/// the cursor, recolour the screen, rewrite earlier output, or in some
/// terminals stuff input into the user's shell. Escaping the C0 controls and
/// DEL removes that without touching printable text, including multi-byte
/// UTF-8, which passes through byte for byte.
fn write_payload<W: Write>(out: &mut W, bytes: &[u8], escape: bool) {
    if !escape {
        let _ = out.write_all(bytes);
        return;
    }
    let mut plain_from = 0;
    for (index, &byte) in bytes.iter().enumerate() {
        // Tab survives: it is a layout character, not a control sequence, and
        // mangling it would corrupt ordinary tab-separated payloads. Newline
        // and carriage return do not: both let a payload forge what looks like
        // a separate line of output.
        let printable = byte == b'\t' || (0x20..0x7f).contains(&byte) || byte >= 0x80;
        if printable {
            continue;
        }
        let _ = out.write_all(&bytes[plain_from..index]);
        let _ = write!(out, "\\x{byte:02x}");
        plain_from = index + 1;
    }
    let _ = out.write_all(&bytes[plain_from..]);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rendered(bytes: &[u8], escape: bool) -> String {
        let mut out = Vec::new();
        write_payload(&mut out, bytes, escape);
        String::from_utf8(out).expect("the escaped form is ASCII-safe")
    }

    #[test]
    fn a_pipe_receives_the_payload_byte_for_byte() {
        // The escape sequence a hostile broker would send to clear the screen.
        let hostile = b"\x1b[2J\x1b[Hgotcha";
        let mut out = Vec::new();
        write_payload(&mut out, hostile, false);
        assert_eq!(out, hostile, "redirected output must not be rewritten");
    }

    #[test]
    fn a_terminal_never_receives_an_escape_character() {
        let rendered = rendered(b"\x1b[2Jcleared", true);
        assert!(
            !rendered.contains('\x1b'),
            "ESC reached the terminal: {rendered}"
        );
        assert_eq!(rendered, "\\x1b[2Jcleared");
    }

    #[test]
    fn printable_text_survives_escaping_unchanged() {
        assert_eq!(rendered(b"temperature=21.5", true), "temperature=21.5");
    }

    /// Multi-byte UTF-8 is printable text, not control data: escaping it would
    /// make ordinary payloads unreadable in the terminal for no security gain.
    #[test]
    fn multibyte_utf8_passes_through_a_terminal_intact() {
        assert_eq!(rendered("café ☕".as_bytes(), true), "café ☕");
    }

    /// Tab is layout and stays; newline and carriage return do not, because
    /// either lets a payload forge what looks like a separate line of output.
    #[test]
    fn tab_survives_but_line_breaks_are_escaped() {
        assert_eq!(rendered(b"a\tb", true), "a\tb");
        assert_eq!(rendered(b"a\nb\rc", true), "a\\x0ab\\x0dc");
    }

    /// Bytes at or above 0x80 pass through so that UTF-8 survives, which means
    /// the escaped output is not guaranteed to be valid UTF-8 itself. That is
    /// the right trade: a stray 0xff renders as a replacement character, while
    /// an unescaped ESC would still be a live control sequence.
    #[test]
    fn a_payload_that_is_not_utf8_at_all_is_still_stripped_of_controls() {
        let mut out = Vec::new();
        write_payload(&mut out, &[0x00, 0xff, 0x1b, 0x7f], true);
        assert_eq!(out, b"\\x00\xff\\x1b\\x7f");
    }

    #[test]
    fn no_properties_formats_to_nothing() {
        assert!(format_properties(&Properties::new()).is_none());
    }

    #[test]
    fn every_supported_property_renders_as_key_equals_value() {
        let mut properties = Properties::new();
        properties.content_type = Some("application/json".into());
        properties.message_expiry_interval = Some(60);
        properties.payload_format_indicator = Some(1);
        properties.user_properties = vec![("room".into(), "kitchen".into())];

        let line = format_properties(&properties).expect("properties render");
        assert_eq!(
            String::from_utf8(line).unwrap(),
            "content-type=application/json message-expiry-interval=60 \
             payload-format-indicator=1 user:room=kitchen"
        );
    }

    /// A property other than the four `sub` prints (e.g. `response_topic`,
    /// which `request` uses) must not produce a stray properties line.
    #[test]
    fn an_unrelated_property_alone_formats_to_nothing() {
        let mut properties = Properties::new();
        properties.response_topic = Some("replies/here".into());
        assert!(format_properties(&properties).is_none());
    }
}
