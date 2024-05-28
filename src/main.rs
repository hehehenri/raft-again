use std::{collections::HashMap, sync::Arc, time::Duration};

use async_recursion::async_recursion;
use bytes::Bytes;
use rand::Rng;
use tokio::sync::{
    mpsc::{self, Receiver, Sender},
    Mutex,
};

#[derive(Default, Debug)]
struct Follower {
    voted_for: Option<NodeId>,
}

impl Follower {
    pub fn grant_vote(&mut self, candidate_id: NodeId) -> bool {
        match self.voted_for {
            Some(_candidate_id) => false,
            None => {
                self.voted_for = Some(candidate_id);
                true
            }
        }
    }
}

#[derive(Default, Debug)]
struct Candidate {
    votes: usize,
}

impl Candidate {
    /// Candidate starts with one vote, since it votes for itself
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

type Tx = Sender<Option<Bytes>>;
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
    channel: (Tx, Rx),
}

fn election_timeout_duration() -> Duration {
    const MIN_DURATION_IN_MS: u16 = 150;
    const MAX_DURATION_IN_MS: u16 = 300;

    let mut rng = rand::thread_rng();
    let rand_duration_ms = rng.gen_range(MIN_DURATION_IN_MS..MAX_DURATION_IN_MS);

    Duration::from_millis(rand_duration_ms as u64)
}

impl Node {
    fn new(id: NodeId, channel: (Tx, Rx)) -> Node {
        Self {
            id,
            term: Term(0),
            role: Role::Follower(Follower::default()),
            state: Bytes::default(),
            logs: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            channel,
        }
    }

    fn convert_to_candidate(&mut self) {
        self.role = Role::Candidate(Candidate::new());
    }

    #[async_recursion]
    pub async fn start_election_timeout(&mut self) {
        let duration = election_timeout_duration();

        let mut rx = self.channel.1.lock().await;

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
    use super::*;
    use crate::Node;
    use axum::{extract::State, http::StatusCode, response::IntoResponse, routing, Json, Router};
    use std::sync::Arc;

    type AppState = Arc<Mutex<Node>>;

    pub fn configure_router() -> Router<AppState> {
        Router::new()
            .route("/appendEntries", routing::post(append_entries))
            .route("/requestVote", routing::post(request_vote))
    }

    // TODO: serde derive deserialize
    pub struct AppendEntriesPayload {
        /// Leader's term
        term: usize,
        /// So followers can redirect client messages to leader
        leader_id: NodeId,
        /// Term of previous log index entry
        previous_log_term: usize,
        /// Log entries to be stored (None for heartbeat)
        entries: Option<Logs>,
        /// Leader's commit index
        leader_commit_index: usize,
    }

    // TODO: serde derive serialize
    pub struct AppendEntriesResponse {
        /// Current term, so candidate can update itself
        term: Term,
        /// True if follower contained entry matching prev_log_index and prev_log_term
        success: bool,
    }

    pub async fn append_entries(
        State(node): State<AppState>,
        Json(payload): Json<AppendEntriesPayload>,
    ) -> Result<Json<AppendEntriesResponse>, AppError> {
        todo!()
    }

    // TODO: serde derive deserialize
    pub struct RequestVotePayload {
        /// Candidate's term
        term: Term,
        /// Candidate requesting vote
        candidate_id: NodeId,
        /// Index of candidate's last log
        last_log_index: usize,
        /// Term of candidate's last log
        last_log_term: usize,
    }

    // TODO: serde derive serialize
    pub struct RequestVoteResponse {
        /// Current term, so candidate can update itself
        current_term: Term,
        /// True means candidate received vote
        vote_granted: bool,
    }

    pub async fn request_vote(
        State(node): State<AppState>,
        Json(payload): Json<RequestVotePayload>,
    ) -> Result<Json<RequestVoteResponse>, AppError> {
        let mut node = node.lock().await;

        let vote_granted = match &mut node.role {
            Role::Follower(follower) => follower.grant_vote(payload.candidate_id),
            _ => todo!(),
        };

        Ok(Json(RequestVoteResponse {
            vote_granted,
            current_term: node.term,
        }))
    }

    pub struct AppError(anyhow::Error);

    impl IntoResponse for AppError {
        fn into_response(self) -> axum::response::Response {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                // TODO: use serde to serialize error message in a valid json format
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
    // TODO: what's an appropriate buf len?
    let (tx, rx) = mpsc::channel(100);
    let rx = Arc::new(Mutex::new(rx));
    let node = Node::new(NodeId(0), (tx, rx));
    let node = Arc::new(Mutex::new(node));

    let listener = tokio::net::TcpListener::bind("0.0.0.0:6969")
        .await
        .expect("failed to start tcp listener");
    let app = router::configure_router().with_state(node.clone());

    tokio::spawn(async move {
        let pog = node.start_election_timeout().await;
    });

    axum::serve(listener, app)
        .await
        .expect("failed to start app");
}
