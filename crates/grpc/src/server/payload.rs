//! Raw payload serialization and service dispatch contracts.
use grpc_core::{Codec, Status};

use crate::stream::{RequestStream, ResponseSink};

/// Raw payload codec for custom service dispatchers.
#[derive(Default)]
pub struct PayloadCodec;
impl Codec for PayloadCodec {
    type EncodeItem = Vec<u8>;
    type DecodeItem = Vec<u8>;
    type Error = Status;
    const CONTENT_TYPE: &'static str = "application/grpc";
    fn encode(&mut self, item: &Vec<u8>, destination: &mut Vec<u8>) -> Result<(), Status> {
        destination.extend_from_slice(item);
        Ok(())
    }
    fn decode(&mut self, payload: &[u8]) -> Result<Vec<u8>, Status> {
        Ok(payload.to_vec())
    }
}

/// Custom dispatcher receiving raw request payloads and a raw response sink.
/// Implementations can bridge generated service methods using their descriptors.
pub trait ServiceHandler {
    /// Runs one routed RPC. The server sends terminal status after this returns.
    /// # Errors
    /// Returns the RPC status to send to the caller.
    fn handle(
        &mut self,
        requests: RequestStream<Vec<u8>>,
        responses: &mut ResponseSink<Vec<u8>>,
    ) -> Result<(), Status>;
}
impl<F> ServiceHandler for F
where
    F: FnMut(RequestStream<Vec<u8>>, &mut ResponseSink<Vec<u8>>) -> Result<(), Status>,
{
    fn handle(
        &mut self,
        requests: RequestStream<Vec<u8>>,
        responses: &mut ResponseSink<Vec<u8>>,
    ) -> Result<(), Status> {
        self(requests, responses)
    }
}
