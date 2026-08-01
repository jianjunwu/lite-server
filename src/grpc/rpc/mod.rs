//! The five LiteServer RPC implementations, one file per RPC. Each holds the
//! inherent `*_impl` method doing the real work; the thin `impl LiteServer
//! for GrpcService` dispatch wrappers (admission / span / metrics / header
//! echo) stay together in `grpc::mod` — a trait impl cannot be split across
//! files (E0119).

mod batch;
mod bidi;
mod decoupled;
mod infer;
mod stream;
