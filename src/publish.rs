//! `pulsemq-cli pub` — publish one message, complete its QoS handshake, exit.

use pulsemq::packet::{Packet, Publish};
use pulsemq::types::{QoS, ReasonCode};

use crate::cli::PubArgs;
use crate::client::Client;
use crate::error::{Error, Result};

pub async fn run(args: PubArgs) -> Result<()> {
    let qos = QoS::from_u8(args.qos)?;
    let payload: Vec<u8> = args.message.as_deref().unwrap_or("").as_bytes().to_vec();

    let mut client = Client::connect(&args.conn).await?;

    let packet_id = match qos {
        QoS::AtMostOnce => None,
        _ => Some(client.next_packet_id()),
    };
    let publish = Publish {
        dup: false,
        qos,
        retain: args.retain,
        topic: args.topic.clone(),
        packet_id,
        properties: Default::default(),
        payload: payload.into(),
    };
    client.send(&Packet::Publish(publish)).await?;

    // QoS 0 is fire-and-forget; 1 and 2 must finish their handshake before the
    // process exits, or the broker sees an incomplete delivery.
    match qos {
        QoS::AtMostOnce => {}
        QoS::AtLeastOnce => {
            let ack = expect_ack(&mut client, "publish").await?;
            check(ack, "publish")?;
        }
        QoS::ExactlyOnce => {
            let rec = expect_ack(&mut client, "publish").await?;
            check(rec, "publish")?;
            let id = packet_id.expect("QoS 2 always allocates a packet identifier");
            let pubrel = pulsemq::packet::PubAck::new(id, ReasonCode::Success);
            client.send(&Packet::Pubrel(pubrel)).await?;
            let comp = expect_ack(&mut client, "publish").await?;
            check(comp, "publish")?;
        }
    }

    client.disconnect().await
}

/// Read until the next acknowledgement packet, ignoring anything the broker
/// sends in the meantime.
async fn expect_ack(client: &mut Client, what: &str) -> Result<ReasonCode> {
    loop {
        match client.recv().await? {
            Packet::Puback(a) | Packet::Pubrec(a) | Packet::Pubcomp(a) => return Ok(a.reason_code),
            Packet::Pingresp => continue,
            Packet::Disconnect(d) => {
                return Err(Error::Rejected {
                    what: what.into(),
                    code: d.reason_code,
                })
            }
            other => {
                return Err(Error::Mqtt(pulsemq::error::protocol(format!(
                    "expected an acknowledgement, got {}",
                    other.name()
                ))))
            }
        }
    }
}

fn check(code: ReasonCode, what: &str) -> Result<()> {
    // NoMatchingSubscribers means the message was accepted and had nowhere to
    // go — not a failure (3.4.2.1).
    if code.is_error() {
        Err(Error::Rejected {
            what: what.into(),
            code,
        })
    } else {
        Ok(())
    }
}
