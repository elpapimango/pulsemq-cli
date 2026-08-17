//! `pulsemq-cli` — a command-line MQTT client.
//!
//! The protocol layer is the `pulsemq` crate's codec (`packet`, `codec`,
//! `framing`, `types`), used as a library; nothing here re-implements the wire
//! format. What lives in this crate is the client half: dialling and the
//! CONNECT handshake (`client`), the argument surface (`cli`), and one module
//! per subcommand (`publish`, `subscribe`, `request`).
//!
//! Anything shared by two subcommands belongs in `client` or is re-used
//! directly from `subscribe` — `request` drives the same subscribe / print /
//! acknowledge path that `sub` does.

pub mod bench;
pub mod cli;
pub mod client;
pub mod error;
pub mod publish;
pub mod request;
pub mod subscribe;
