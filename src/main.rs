use std::{collections::HashMap, sync::Arc, time::Duration};

use async_recursion::async_recursion;
use rand::Rng;
use tokio::sync::{
    mpsc::{self, Receiver},
    Mutex,
};

#[derive(Default)]
struct Follower {
    voted_for: Option<usize>,
}

struct Candidate {
    votes: usize,
}

impl Candidate {
    /// Already starts with one vote, since the node votes for itself
    fn new() -> Self {
        Self { votes: 1 }
    }
}

struct Leader {
    /// Index of the next log entry to send to each node.
    next_index: HashMap<NodeId, usize>,
    /// Index of the highest log entry known to be applied on node.
    match_index: HashMap<NodeId, usize>,
}

#[derive(Clone, Copy)]
struct NodeId(usize);

type Logs<T> = Vec<T>;

#[derive(Clone, Copy)]
struct Term(usize);

impl Term {
    pub fn increment(&mut self) {
        self.0 = self.0.wrapping_add(1);
    }
}

type Rx<T> = Arc<Mutex<Receiver<T>>>;

struct Node<T, R>
where
    T: Sync,
    R: Sync,
{
    id: NodeId,
    term: Term,
    role: R,
    state: T,
    logs: Logs<T>,
    commit_index: usize,
    last_applied: usize,
    receiver: Rx<Option<T>>,
}

fn election_timeout_duration() -> Duration {
    const MIN_DURATION_IN_MS: u16 = 150;
    const MAX_DURATION_IN_MS: u16 = 300;

    let mut rng = rand::thread_rng();
    let rand_duration_ms = rng.gen_range(MIN_DURATION_IN_MS..MAX_DURATION_IN_MS);

    Duration::from_millis(rand_duration_ms as u64)
}

impl<T: Default + Clone + Send + Sync> Node<T, Follower> {
    fn new(id: NodeId, receiver: Rx<Option<T>>) -> Node<T, Follower> {
        Self {
            id,
            term: Term(0),
            role: Follower::default(),
            state: T::default(),
            logs: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            receiver,
        }
    }

    fn convert_to_candidate(&self) -> Node<T, Candidate> {
        Node {
            id: self.id,
            term: self.term,
            role: Candidate::new(),
            state: self.state.clone(),
            logs: self.logs.clone(),
            commit_index: self.commit_index,
            last_applied: self.last_applied,
            receiver: self.receiver.clone(),
        }
    }

    #[async_recursion]
    pub async fn start_election_timeout(&self) -> Node<T, Candidate> {
        let duration = election_timeout_duration();

        let mut rx = self.receiver.lock().await;

        match tokio::time::timeout(duration, rx.recv()).await {
            Ok(Some(a)) => self.start_election_timeout().await,
            // the channel was closed
            Ok(None) => self.convert_to_candidate(),
            // no message was received after the elapsed the timeout
            Err(_) => self.convert_to_candidate(),
        }
    }
}

#[tokio::main]
async fn main() {
    // TODO: properly set the buffer len
    let (tx, rx) = mpsc::channel::<Option<usize>>(100);
    let rx = Arc::new(Mutex::new(rx));
    let server = Node::new(NodeId(0), rx);
}
