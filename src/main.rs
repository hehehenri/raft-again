use rand::Rng;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, sync::Arc, time::Duration};
use tokio::sync::Mutex;

// Core types
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq, Hash)]
struct NodeId(usize);

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, PartialOrd)]
struct Term(usize);

#[derive(Debug, Clone, Serialize, Deserialize)]
struct LogEntry {
    term: Term,
    data: String,
}

// Node states
#[derive(Debug, Clone)]
enum Role {
    Follower {
        voted_for: Option<NodeId>,
        last_heartbeat: std::time::Instant,
    },
    Candidate {
        votes: usize,
        election_start: std::time::Instant,
    },
    Leader {
        next_index: HashMap<NodeId, usize>,
        match_index: HashMap<NodeId, usize>,
    },
}

// RPC messages
#[derive(Serialize, Deserialize, Clone)]
struct AppendEntriesRpc {
    term: Term,
    leader_id: NodeId,
    prev_log_index: usize,
    prev_log_term: Term,
    entries: Option<Vec<LogEntry>>,
    leader_commit: usize,
}

#[derive(Serialize, Deserialize)]
struct AppendEntriesResponse {
    term: Term,
    success: bool,
}

#[derive(Serialize, Deserialize, Clone)]
struct RequestVoteRpc {
    term: Term,
    candidate_id: NodeId,
    last_log_index: usize,
    last_log_term: Term,
}

#[derive(Serialize, Deserialize)]
struct RequestVoteResponse {
    term: Term,
    vote_granted: bool,
}

// Node implementation
#[derive(Debug, Clone)]
struct Node {
    id: NodeId,
    term: Term,
    role: Role,
    logs: Vec<LogEntry>,
    commit_index: usize,
    last_applied: usize,
    peers: Vec<NodeId>,
    client: reqwest::Client,
}

impl Node {
    fn new(id: NodeId, peers: Vec<NodeId>) -> Self {
        Self {
            id,
            term: Term(0),
            role: Role::Follower {
                voted_for: None,
                last_heartbeat: std::time::Instant::now(),
            },
            logs: Vec::new(),
            commit_index: 0,
            last_applied: 0,
            peers,
            client: reqwest::Client::new(),
        }
    }

    async fn start(&mut self) {
        println!("Node {} starting...", self.id.0);
        loop {
            match &self.role {
                Role::Follower { last_heartbeat, .. } => {
                    if last_heartbeat.elapsed() > election_timeout_duration() {
                        self.start_election().await;
                    }
                }
                Role::Candidate { election_start, .. } => {
                    if election_start.elapsed() > election_timeout_duration() {
                        self.start_election().await;
                    }
                }
                Role::Leader { .. } => {
                    self.send_heartbeats().await;
                    tokio::time::sleep(Duration::from_millis(50)).await;
                }
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }

    async fn start_election(&mut self) {
        self.term.0 += 1;
        println!(
            "Node {} starting election for term {}",
            self.id.0, self.term.0
        );
        self.role = Role::Candidate {
            votes: 1, // Vote for self
            election_start: std::time::Instant::now(),
        };

        let request = RequestVoteRpc {
            term: self.term,
            candidate_id: self.id,
            last_log_index: self.logs.len(),
            last_log_term: self.logs.last().map_or(Term(0), |e| e.term),
        };

        let mut vote_count = 1; // Include self vote
        for &peer in &self.peers {
            if let Ok(response) = self.send_request_vote(peer, request.clone()).await {
                if response.vote_granted {
                    vote_count += 1;
                } else if response.term > self.term {
                    self.become_follower(response.term);
                    return;
                }
            }
        }

        if vote_count > (self.peers.len() + 1) / 2 {
            self.become_leader();
        }
    }

    async fn send_heartbeats(&mut self) {
        let request = AppendEntriesRpc {
            term: self.term,
            leader_id: self.id,
            prev_log_index: self.logs.len(),
            prev_log_term: self.logs.last().map_or(Term(0), |e| e.term),
            entries: None, // Empty for heartbeat
            leader_commit: self.commit_index,
        };

        for &peer in &self.peers {
            if let Ok(response) = self.send_append_entries(peer, request.clone()).await {
                if !response.success && response.term > self.term {
                    self.become_follower(response.term);
                    return;
                }
            }
        }
    }

    async fn send_request_vote(
        &self,
        peer: NodeId,
        request: RequestVoteRpc,
    ) -> reqwest::Result<RequestVoteResponse> {
        println!(
            "Node {} sending vote request to {} for term {}",
            self.id.0, peer.0, request.term.0
        );
        let url = format!("http://localhost:{}/requestVote", 6969 + peer.0);
        let response: RequestVoteResponse = self
            .client
            .post(url)
            .json(&request)
            .send()
            .await?
            .json()
            .await?;
        println!(
            "Node {} received vote response from {}: granted={}",
            self.id.0, peer.0, response.vote_granted
        );
        Ok(response)
    }

    async fn send_append_entries(
        &self,
        peer: NodeId,
        request: AppendEntriesRpc,
    ) -> reqwest::Result<AppendEntriesResponse> {
        let url = format!("http://localhost:{}/appendEntries", 6969 + peer.0);
        self.client
            .post(url)
            .json(&request)
            .send()
            .await?
            .json()
            .await
    }

    fn become_follower(&mut self, term: Term) {
        self.term = term;
        self.role = Role::Follower {
            voted_for: None,
            last_heartbeat: std::time::Instant::now(),
        };
    }

    fn become_leader(&mut self) {
        println!(
            "Node {} becoming leader for term {}",
            self.id.0, self.term.0
        );
        let mut next_index = HashMap::new();
        let mut match_index = HashMap::new();

        for &peer in &self.peers {
            next_index.insert(peer, self.logs.len() + 1);
            match_index.insert(peer, 0);
        }

        self.role = Role::Leader {
            next_index,
            match_index,
        };
    }
}

fn election_timeout_duration() -> Duration {
    let mut rng = rand::thread_rng();
    Duration::from_millis(rng.gen_range(300..600)) // Increased timeout range
}

use std::net::SocketAddr;

// HTTP server setup
async fn run_server(node: Arc<Mutex<Node>>) {
    let app = axum::Router::new()
        .route("/appendEntries", axum::routing::post(handle_append_entries))
        .route("/requestVote", axum::routing::post(handle_request_vote))
        .with_state(node.clone());

    let port = 6969 + node.lock().await.id.0 as u16;
    let addr = SocketAddr::from(([0, 0, 0, 0], port));
    println!("Node {} listening on {}", node.lock().await.id.0, addr);

    axum_server::bind(addr)
        .serve(app.into_make_service())
        .await
        .unwrap();
}

async fn handle_append_entries(
    axum::extract::State(node): axum::extract::State<Arc<Mutex<Node>>>,
    axum::Json(request): axum::Json<AppendEntriesRpc>,
) -> axum::Json<AppendEntriesResponse> {
    let mut node = node.lock().await;

    if request.term < node.term {
        return axum::Json(AppendEntriesResponse {
            term: node.term,
            success: false,
        });
    }

    if request.term > node.term {
        node.become_follower(request.term);
    }

    if let Role::Follower { last_heartbeat, .. } = &mut node.role {
        *last_heartbeat = std::time::Instant::now();
    }

    axum::Json(AppendEntriesResponse {
        term: node.term,
        success: true,
    })
}

async fn handle_request_vote(
    axum::extract::State(node): axum::extract::State<Arc<Mutex<Node>>>,
    axum::Json(request): axum::Json<RequestVoteRpc>,
) -> axum::Json<RequestVoteResponse> {
    let mut node = node.lock().await;
    println!(
        "Node {} received vote request from {} for term {}",
        node.id.0, request.candidate_id.0, request.term.0
    );

    if request.term < node.term {
        println!("Node {} rejected vote: term too low", node.id.0);
        return axum::Json(RequestVoteResponse {
            term: node.term,
            vote_granted: false,
        });
    }

    if request.term > node.term {
        println!("Node {} updating term to {}", node.id.0, request.term.0);
        node.become_follower(request.term);
    }

    let node_id = node.id.clone();
    let vote_granted = if let Role::Follower { voted_for, .. } = &mut node.role {
        if voted_for.is_none() || voted_for == &Some(request.candidate_id) {
            println!(
                "Node {} granting vote to {}",
                node_id.0, request.candidate_id.0
            );
            *voted_for = Some(request.candidate_id);
            true
        } else {
            println!(
                "Node {} rejected vote: already voted for {:?}",
                node_id.0, voted_for
            );
            false
        }
    } else {
        println!("Node {} rejected vote: not a follower", node.id.0);
        false
    };

    axum::Json(RequestVoteResponse {
        term: node.term,
        vote_granted,
    })
}

#[tokio::main]
async fn main() {
    let node_id = NodeId(
        std::env::var("NODE_ID")
            .unwrap_or_else(|_| "0".to_string())
            .parse()
            .unwrap_or(0),
    );

    // Create peer list excluding self
    let peers: Vec<NodeId> = (0..3).filter(|&i| i != node_id.0).map(NodeId).collect();

    let node = Arc::new(Mutex::new(Node::new(node_id, peers)));

    // Start the HTTP server in a separate task
    let server_node = node.clone();
    let server_handle = tokio::spawn(async move {
        run_server(server_node).await;
    });

    // Give the server a moment to start
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Start the node's main loop
    let mut node = node.lock().await;
    node.start().await;

    // Wait for the server (though we'll likely never reach this)
    let _ = server_handle.await;
}
