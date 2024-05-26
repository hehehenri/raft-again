use std::{collections::HashMap, sync::Arc, time::Duration};

use async_recursion::async_recursion;
use bytes::Bytes;
use rand::Rng;
use tokio::sync::{
    mpsc::{self, Receiver},
    Mutex,
};

#[derive(Default, Debug)]
struct Follower {
    voted_for: Option<usize>,
}

#[derive(Default, Debug)]
struct Candidate {
    votes: usize,
}

impl Candidate {
    /// Already starts with one vote, since the node votes for itself
    fn new() -> Self {
        Self { votes: 1 }
    }
}

#[derive(Default, Debug)]
struct Leader {
    /// Index of the next log entry to send to each node.
    next_index: HashMap<NodeId, usize>,
    /// Index of the highest log entry known to be applied on node.
    match_index: HashMap<NodeId, usize>,
}

#[derive(Debug)]
enum Role {
    Follower(Follower),
    Candidate(Candidate),
    Leader(Leader),
}

#[derive(Clone, Copy, Debug)]
struct NodeId(usize);

type Logs = Vec<Bytes>;

#[derive(Clone, Copy, Debug)]
struct Term(usize);

impl Term {
    pub fn increment(&mut self) {
        self.0 = self.0.wrapping_add(1);
    }
}

type Rx = Arc<Mutex<Receiver<Option<Bytes>>>>;

#[derive(Debug)]
struct Node {
    id: NodeId,
    term: Term,
    role: Role,
    state: Bytes,
    logs: Logs,
    commit_index: usize,
    last_applied: usize,
    receiver: Rx,
}

fn election_timeout_duration() -> Duration {
    const MIN_DURATION_IN_MS: u16 = 150;
    const MAX_DURATION_IN_MS: u16 = 300;

    let mut rng = rand::thread_rng();
    let rand_duration_ms = rng.gen_range(MIN_DURATION_IN_MS..MAX_DURATION_IN_MS);

    Duration::from_millis(rand_duration_ms as u64)
}

impl Node {
    fn new(id: NodeId, receiver: Rx) -> Node {
        Self {
            id,
            term: Term(0),
            role: Role::Follower(Follower::default()),
            state: Bytes::default(),
            logs: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            receiver,
        }
    }

    fn convert_to_candidate(&self) -> Node {
        Node {
            id: self.id,
            term: self.term,
            role: Role::Candidate(Candidate::new()),
            state: self.state.clone(),
            logs: self.logs.clone(),
            commit_index: self.commit_index,
            last_applied: self.last_applied,
            receiver: self.receiver.clone(),
        }
    }

    #[async_recursion]
    pub async fn start_election_timeout(&self) -> Node {
        let duration = election_timeout_duration();

        let mut rx = self.receiver.lock().await;

        match tokio::time::timeout(duration, rx.recv()).await {
            Ok(Some(_)) => self.start_election_timeout().await,
            // the channel was closed
            Ok(None) => self.convert_to_candidate(),
            // no message was received after the elapsed the timeout
            Err(_) => self.convert_to_candidate(),
        }
    }
}

mod router {
    use crate::Node;
    use axum::{http::StatusCode, response::IntoResponse, routing, Router};
    use std::sync::Arc;

    pub fn configure_router() -> Router<Arc<Node>> {
        Router::new().route("/heartbeat", routing::post(heartbeat))
    }

    pub async fn heartbeat() -> Result<impl IntoResponse, AppError> {
        Ok(())
    }

    pub struct AppError(anyhow::Error);

    impl IntoResponse for AppError {
        fn into_response(self) -> axum::response::Response {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                format!("Something went wrong: {}", self.0),
            )
                .into_response()
        }
    }

    impl<E> From<E> for AppError
    where
        E: Into<anyhow::Error>,
    {
        fn from(err: E) -> Self {
            Self(err.into())
        }
    }
}

#[tokio::main]
async fn main() {
    // TODO: properly set the buffer len
    let (tx, rx) = mpsc::channel(100);
    let rx = Arc::new(Mutex::new(rx));
    let node = Node::new(NodeId(0), rx);

    let listener = tokio::net::TcpListener::bind("0.0.0.0:6969")
        .await
        .expect("failed to start tcp listener");
    let app = router::configure_router().with_state(Arc::new(node));

    axum::serve(listener, app)
        .await
        .expect("failed to start app");
}
