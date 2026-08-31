//! `wispmq-cli request` — publish a request, wait for the reply.
//!
//! The subscribe must precede the publish, or a fast responder can answer
//! before the subscription exists and the reply is lost.

use crate::mqtt::codec::Properties;
use crate::mqtt::packet::{Packet, PubAck, Publish};
use crate::mqtt::types::{ProtocolVersion, QoS, ReasonCode};

use crate::cli::RequestArgs;
use crate::client::Client;
use crate::error::{Error, Result};
use crate::subscribe::{acknowledge, print_message, subscribe};

pub async fn run(args: RequestArgs) -> Result<()> {
    if args.conn.version() == ProtocolVersion::V3_1 {
        return Err(Error::Unsupported(
            "request requires MQTT v5.0 or v3.1.1: v3.1 has no Response Topic property \
             and no way to correlate a reply"
                .into(),
        ));
    }
    let qos = QoS::from_u8(args.qos)?;
    let payload: Vec<u8> = args.message.as_deref().unwrap_or("").as_bytes().to_vec();

    let mut client = Client::connect(&args.conn).await?;
    subscribe(&mut client, std::slice::from_ref(&args.reply_topic), qos).await?;

    // v5 carries the reply topic in a property, so the responder can find it
    // without out-of-band agreement. v3.1.1 has no properties: there, both
    // sides must already agree on the topic named by --reply-topic.
    let mut properties = Properties::new();
    if args.conn.version().has_properties() {
        properties.response_topic = Some(args.reply_topic.clone());
    }

    let packet_id = match qos {
        QoS::AtMostOnce => None,
        _ => Some(client.next_packet_id()),
    };
    let request = Publish {
        dup: false,
        qos,
        retain: false,
        topic: args.topic.clone(),
        packet_id,
        properties,
        payload: payload.into(),
    };
    client.send(&Packet::Publish(request)).await?;

    let mut seen = 0u64;
    while seen < args.count {
        match client.recv_keepalive().await? {
            Packet::Publish(p) => {
                acknowledge(&mut client, &p).await?;
                print_message(&p, args.show_topic);
                seen += 1;
            }
            Packet::Pubrel(rel) => {
                let comp = PubAck::new(rel.packet_id, ReasonCode::Success);
                client.send(&Packet::Pubcomp(comp)).await?;
            }
            // The acknowledgements for our own request, on the way through.
            Packet::Puback(_) | Packet::Pubcomp(_) | Packet::Pingresp => {}
            Packet::Pubrec(rec) => {
                let rel = PubAck::new(rec.packet_id, ReasonCode::Success);
                client.send(&Packet::Pubrel(rel)).await?;
            }
            Packet::Disconnect(d) => {
                return Err(Error::Rejected {
                    what: "request".into(),
                    code: d.reason_code,
                })
            }
            _ => {}
        }
    }

    client.disconnect().await
}
