//! `pulsemq-cli sub` — subscribe, then print messages until interrupted or
//! until `--count` of them have arrived.

use std::io::Write;

use crate::mqtt::packet::{Packet, PubAck, Publish, Subscribe, TopicFilter};
use crate::mqtt::types::{QoS, ReasonCode};

use crate::cli::SubArgs;
use crate::client::Client;
use crate::error::{Error, Result};

pub async fn run(args: SubArgs) -> Result<()> {
    if args.topic.is_empty() {
        return Err(Error::Usage("sub needs at least one --topic".into()));
    }
    let qos = QoS::from_u8(args.qos)?;

    let mut client = Client::connect(&args.conn).await?;
    subscribe(&mut client, &args.topic, qos).await?;

    let mut seen = 0u64;
    loop {
        match client.recv_keepalive().await? {
            Packet::Publish(p) => {
                acknowledge(&mut client, &p).await?;
                print_message(&p, args.show_topic);
                seen += 1;
                if args.count.is_some_and(|c| seen >= c) {
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
                retain_handling: crate::mqtt::packet::RetainHandling::SendAtSubscribe,
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
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    if show_topic {
        let _ = write!(out, "{} ", publish.topic);
    }
    let _ = out.write_all(&publish.payload);
    let _ = out.write_all(b"\n");
    let _ = out.flush();
}
