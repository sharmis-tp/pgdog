//! Cancel requests in flight.

use fnv::FnvHashMap as HashMap;
use tokio::time::Instant;

use crate::backend::{Server, stats::Counts as BackendCounts};
use crate::net::BackendPid;

/// Check-in held back until every cancel request for the server is done.
#[derive(Debug)]
pub(super) struct DeferredCheckIn {
    pub(super) server: Box<Server>,
    pub(super) now: Instant,
    pub(super) stats: BackendCounts,
    pub(super) moving: bool,
}

#[derive(Debug, Default)]
struct InFlight {
    requests: usize,
    check_in: Option<DeferredCheckIn>,
}

/// Server connections with a cancel request in flight.
///
/// Postgres cancels whatever query the backend is running when the cancel
/// signal arrives. In transaction mode, the client can finish its
/// transaction and the server can be given to another client while
/// the cancel request is still on its way, so it would cancel the other
/// client's query instead.
///
/// To prevent this, the check-in of a server with a cancel in flight is
/// deferred until Postgres has processed all cancel requests sent to it.
#[derive(Debug, Default)]
pub(super) struct Cancels {
    in_flight: HashMap<BackendPid, InFlight>,
}

impl Cancels {
    /// Record a cancel request for this server. Returns false if the client
    /// has already returned the server, so there is nothing to cancel.
    pub(super) fn start(&mut self, backend: BackendPid) -> bool {
        let in_flight = self.in_flight.entry(backend).or_default();

        if in_flight.check_in.is_some() {
            return false;
        }

        in_flight.requests += 1;
        true
    }

    /// Cancel request for this server is done. Returns the deferred check-in,
    /// if any, once no more cancel requests are in flight.
    pub(super) fn finish(&mut self, backend: BackendPid) -> Option<DeferredCheckIn> {
        let in_flight = self.in_flight.get_mut(&backend)?;
        in_flight.requests = in_flight.requests.saturating_sub(1);

        if in_flight.requests > 0 {
            return None;
        }

        self.in_flight
            .remove(&backend)
            .and_then(|in_flight| in_flight.check_in)
    }

    /// A cancel request is in flight for this server.
    pub(super) fn in_flight(&self, backend: BackendPid) -> bool {
        self.in_flight.contains_key(&backend)
    }

    /// Hold the check-in until all cancel requests for the server are done.
    /// The caller must check [`Self::in_flight`] first.
    pub(super) fn defer(&mut self, check_in: DeferredCheckIn) {
        if let Some(in_flight) = self.in_flight.get_mut(&check_in.server.id()) {
            in_flight.check_in = Some(check_in);
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    fn check_in(server: Server) -> DeferredCheckIn {
        DeferredCheckIn {
            server: Box::new(server),
            now: Instant::now(),
            stats: BackendCounts::default(),
            moving: false,
        }
    }

    #[test]
    fn test_cancel_finishes_before_check_in() {
        let mut cancels = Cancels::default();
        let backend = BackendPid::for_test(1);

        assert!(!cancels.in_flight(backend));
        assert!(cancels.finish(backend).is_none());

        assert!(cancels.start(backend));
        assert!(cancels.in_flight(backend));
        assert!(cancels.finish(backend).is_none());
        assert!(!cancels.in_flight(backend));
    }

    #[test]
    fn test_check_in_waits_for_all_cancels() {
        let mut cancels = Cancels::default();
        let server = Server::default();
        let backend = server.id();

        assert!(cancels.start(backend));
        assert!(cancels.start(backend));
        cancels.defer(check_in(server));

        // Client returned the server, nothing left to cancel.
        assert!(!cancels.start(backend));

        assert!(cancels.finish(backend).is_none());
        let deferred = cancels
            .finish(backend)
            .expect("last cancel releases the check-in");
        assert_eq!(deferred.server.id(), backend);
        assert!(!cancels.in_flight(backend));
    }
}
