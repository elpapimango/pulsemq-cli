//! `wispmq-cli` — a command-line MQTT client.
//!
//! The wire format lives in `mqtt` (`packet`, `codec`, `framing`, `types`) and
//! knows nothing about the client above it. The rest of the crate is the
//! client half: dialling and the CONNECT handshake (`client`), the argument
//! surface (`cli`), and one module per subcommand (`publish`, `subscribe`,
//! `request`, `bench`).
//!
//! Anything shared by two subcommands belongs in `client` or is re-used
//! directly from `subscribe` — `request` drives the same subscribe / print /
//! acknowledge path that `sub` does.

pub mod bench;
pub mod cli;
pub mod client;
pub mod error;
pub mod mqtt;
pub mod publish;
pub mod request;
pub mod subscribe;
pub mod transport;
