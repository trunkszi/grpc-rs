//! Reclamation of server streams that never transferred to a request consumer.
use super::GrpcServerSession;
use crate::Status;

impl GrpcServerSession {
    /// Releases safely closed streams that were never handed to a request consumer.
    /// Dispatched streams retain their terminal state until explicitly removed.
    ///
    /// # Errors
    /// Returns a stream-removal error if its closure state is inconsistent.
    pub fn reap_closed_undispatched_streams(&mut self) -> Result<usize, Status> {
        let closed: Vec<_> = self
            .streams
            .iter()
            .filter(|(id, stream)| {
                !stream.dispatched && stream.closed && self.stream_outbound_complete(**id)
            })
            .map(|(id, _)| *id)
            .collect();
        for id in &closed {
            self.remove_closed_stream(*id)?;
        }
        Ok(closed.len())
    }
}
