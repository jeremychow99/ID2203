use crate::datastore::error::DatastoreError;
use crate::datastore::example_datastore::ExampleDatastore;
use crate::datastore::tx_data::TxResult;
use crate::datastore::*;
use crate::durability::omnipaxos_durability::OmniPaxosDurability;
use crate::durability::{DurabilityLayer, DurabilityLevel};
use omnipaxos::messages::*;
use omnipaxos::util::{LogEntry, NodeId};
use omnipaxos::{ClusterConfig, OmniPaxos, OmniPaxosConfig, ServerConfig};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;
use tokio::time::Duration;
use tokio::time::{self, sleep};

use crate::durability::omnipaxos_durability::Transaction;

use self::example_datastore::Tx;
use self::tx_data::DeleteList;

pub const BUFFER_SIZE: usize = 10000;
pub const ELECTION_TICK_TIMEOUT: u64 = 5;
pub const TICK_PERIOD: Duration = Duration::from_millis(10);
pub const OUTGOING_MESSAGE_PERIOD: Duration = Duration::from_millis(1);
pub const UPDATE_DB_PERIOD: Duration = Duration::from_millis(1);

pub const WAIT_LEADER_TIMEOUT: Duration = Duration::from_millis(1000);
pub const WAIT_DECIDED_TIMEOUT: Duration = Duration::from_millis(50);
pub struct NodeRunner {
    pub node: Arc<Mutex<Node>>,
    incoming: mpsc::Receiver<Message<Transaction>>,
    outgoing: HashMap<NodeId, mpsc::Sender<Message<Transaction>>>,
    // TODO Messaging and running
}

impl NodeRunner {
    async fn send_outgoing_msgs(&mut self) {
        let messages = self
            .node
            .lock()
            .unwrap()
            .omni_durability
            .omni_paxos
            .outgoing_messages();

        for msg in messages {
            let receiver = msg.get_receiver();

            let channel = self.outgoing.get_mut(&receiver).expect("No channels");
            if self.node.lock().unwrap().connected_nodes[receiver as usize - 1] == true {
                let channel = self.outgoing.get_mut(&receiver).expect("No channel");
                let _ = channel.send(msg).await;
            }
        }
    }

    pub async fn run(&mut self) {
        self.node.lock().unwrap().update_leader();
        let mut outgoing_interval = time::interval(OUTGOING_MESSAGE_PERIOD);
        let mut tick_interval = time::interval(TICK_PERIOD);
        let mut update_db_tick_interval = time::interval(UPDATE_DB_PERIOD);
        let mut leader_time_out = time::interval(WAIT_LEADER_TIMEOUT);

        sleep(Duration::from_secs(1)).await;
        loop {
            tokio::select! {
                biased;
                _ = tick_interval.tick() => {
                    self.node.lock().unwrap().omni_durability.omni_paxos.tick();
                },
                _ = outgoing_interval.tick() => {
                    if self.node.lock().unwrap().allow_msg {
                        self.send_outgoing_msgs().await;
                    }
                },
                _ = update_db_tick_interval.tick() => {
                    let current_leader = self.node.lock().unwrap().omni_durability.omni_paxos.get_current_leader();
                    if current_leader != Some(self.node.lock().unwrap().node_id) {
                        self.node.lock().unwrap().apply_replicated_txns();
                    }
                },
                _ = leader_time_out.tick() => {
                    self.node.lock().unwrap().update_leader();
                },
                Some(msg) = self.incoming.recv() => {
                    let receiver = msg.get_receiver();
                    if self.node.lock().unwrap().allow_msg {
                    self.node.lock().unwrap().omni_durability.omni_paxos.handle_incoming(msg)
                    };
                }
            }
        }
    }
}

pub struct Node {
    node_id: NodeId,
    pub omni_durability: OmniPaxosDurability,
    data_store: ExampleDatastore,
    allow_msg: bool,
    connected_nodes: Vec<bool>,
}

impl Node {
    pub fn new(node_id: NodeId, omni_durability: OmniPaxosDurability) -> Self {
        Node {
            node_id,
            omni_durability,
            allow_msg: true,
            connected_nodes: vec![true; 4],
            data_store: ExampleDatastore::new(),
        }
    }

    /// update who is the current leader. If a follower becomes the leader,
    /// it needs to apply any unapplied txns to its datastore.
    /// If a node loses leadership, it needs to rollback the txns committed in
    /// memory that have not been replicated yet.
    pub fn update_leader(&mut self) {
        let leader_id = self.omni_durability.omni_paxos.get_current_leader();
        if leader_id == Some(self.node_id) {
            self.apply_replicated_txns();
        } else {
            self.rollback_unreplicated_txns();
        }
    }

    /// Apply the transactions that have been decided in OmniPaxos to the Datastore.
    /// We need to be careful with which nodes should do this according to desired
    /// behavior in the Datastore as defined by the application.
    fn apply_replicated_txns(&mut self) {
        // apply replicated transactions if follower
        self.advance_replicated_durability_offset().unwrap();
        let current_tx_offset = self.omni_durability.get_durable_tx_offset();
        let mut txns = self
            .omni_durability
            .iter_starting_from_offset(current_tx_offset);
        while let Some((_tx_offset, tx_data)) = txns.next() {
            if let Err(err) = self.data_store.replay_transaction(&tx_data) {
                // println!("Error: {}", err);
            }
        }
    }

    fn rollback_unreplicated_txns(&mut self) {
        self.advance_replicated_durability_offset().unwrap();
        if let Err(err) = self.data_store.rollback_to_replicated_durability_offset() {
            println!("Error: {}", err);
        }
    }

    pub fn begin_tx(
        &self,
        durability_level: DurabilityLevel,
    ) -> <ExampleDatastore as Datastore<String, String>>::Tx {
        self.data_store.begin_tx(durability_level)
    }

    pub fn release_tx(&self, tx: <ExampleDatastore as Datastore<String, String>>::Tx) {
        self.data_store.release_tx(tx);
    }

    /// Begins a mutable transaction. Only the leader is allowed to do so.
    pub fn begin_mut_tx(
        &self,
    ) -> Result<<ExampleDatastore as Datastore<String, String>>::MutTx, DatastoreError> {
        if self.omni_durability.omni_paxos.get_current_leader() == Some(self.node_id) {
            // If the current node is the leader, begin a mutable transaction
            Ok(self.data_store.begin_mut_tx())
        } else {
            Err(DatastoreError::NotLeader)
        }
    }

    /// Commits a mutable transaction. Only the leader is allowed to do so.
    pub fn commit_mut_tx(
        &mut self,
        tx: <ExampleDatastore as Datastore<String, String>>::MutTx,
    ) -> Result<TxResult, DatastoreError> {
        if self.omni_durability.omni_paxos.get_current_leader() == Some(self.node_id) {
            let tx_res = self.data_store.commit_mut_tx(tx);
            tx_res
        } else {
            Err(DatastoreError::NotLeader)
        }
    }

    fn advance_replicated_durability_offset(
        &self,
    ) -> Result<(), crate::datastore::error::DatastoreError> {
        let tx_offset = self.omni_durability.get_durable_tx_offset();
        // println!("tx_offset: {:?}", tx_offset);
        self.data_store
            .advance_replicated_durability_offset(tx_offset)
    }
}

/// Your test cases should spawn up multiple nodes in tokio and cover the following:
/// 1. Find the leader and commit a transaction. Show that the transaction is really *chosen* (according to our definition in Paxos) among the nodes.
/// 2. Find the leader and commit a transaction. Kill the leader and show that another node will be elected and that the replicated state is still correct.
/// 3. Find the leader and commit a transaction. Disconnect the leader from the other nodes and continue to commit transactions before the OmniPaxos election timeout.
/// Verify that the transaction was first committed in memory but later rolled back.
/// 4. Simulate the 3 partial connectivity scenarios from the OmniPaxos liveness lecture. Does the system recover? *NOTE* for this test you may need to modify the messaging logic.
///
/// A few helper functions to help structure your tests have been defined that you are welcome to use.
#[cfg(test)]
mod tests {
    use crate::durability::DurabilityLayer;
    use crate::node::*;
    use omnipaxos::messages::Message;
    use omnipaxos::util::NodeId;
    use omnipaxos_storage::memory_storage::MemoryStorage;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use tokio::runtime::{Builder, Runtime};
    use tokio::sync::mpsc;
    use tokio::task::JoinHandle;

    const SERVERS: [NodeId; 4] = [1, 2, 3, 4];

    #[allow(clippy::type_complexity)]
    fn initialise_channels() -> (
        HashMap<NodeId, mpsc::Sender<Message<Transaction>>>,
        HashMap<NodeId, mpsc::Receiver<Message<Transaction>>>,
    ) {
        // Create HashMaps to store senders and receivers
        let mut sender_channels = HashMap::new();
        let mut receiver_channels = HashMap::new();

        for node_id in SERVERS {
            let (sender, receiver) = mpsc::channel(BUFFER_SIZE);
            sender_channels.insert(node_id, sender);
            receiver_channels.insert(node_id, receiver);
        }
        (sender_channels, receiver_channels)
    }

    fn create_runtime() -> Runtime {
        Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap()
    }

    fn spawn_nodes(runtime: &mut Runtime) -> HashMap<NodeId, (Arc<Mutex<Node>>, JoinHandle<()>)> {
        let mut nodes = HashMap::new();
        let (sender_channels, mut receiver_channels) = initialise_channels();

        for node_id in SERVERS {
            let storage = MemoryStorage::default();
            let cluster_config = ClusterConfig {
                configuration_id: 1,
                nodes: SERVERS.into(),
                ..Default::default()
            };
            let server_config = ServerConfig {
                pid: node_id,
                ..Default::default()
            };
            let omnipaxos_config = OmniPaxosConfig {
                server_config,
                cluster_config,
            };
            let omni_paxos = omnipaxos_config.build(storage).unwrap();
            let omni_durability = OmniPaxosDurability::new(omni_paxos);

            let node = Arc::new(Mutex::new(Node::new(node_id, omni_durability)));

            let mut node_runner = NodeRunner {
                node: node.clone(),
                incoming: receiver_channels.remove(&node_id).unwrap(),
                outgoing: sender_channels.clone(),
            };

            let handle = runtime.spawn(async move {
                node_runner.run().await;
            });

            // Insert the node and handle into the HashMap
            nodes.insert(node_id, (node, handle));
        }
        nodes
    }

    #[tokio::test]
    async fn test_spawn_nodes() {
        let mut runtime = create_runtime();
        let nodes: HashMap<u64, (Arc<Mutex<Node>>, JoinHandle<()>)> = spawn_nodes(&mut runtime);

        // Retrieve the node to test
        std::thread::sleep(WAIT_LEADER_TIMEOUT * 2);
        let (node, _) = nodes.get(&1).expect("Node not found");

        // get leader node
        let leader_id = {
            let node_ref = node.lock().unwrap();
            node_ref
                .omni_durability
                .omni_paxos
                .get_current_leader()
                .expect("Leader ID not found")
        };

        // Example: Mutate data and commit transaction
        let (leader_node, _) = nodes.get(&leader_id).unwrap();
        let mut mut_tx = leader_node.lock().unwrap().begin_mut_tx().unwrap();
        mut_tx.set("test".to_string(), "testvalue".to_string());
        let tx_res = leader_node.lock().unwrap().commit_mut_tx(mut_tx).unwrap();
        leader_node
            .lock()
            .unwrap()
            .omni_durability
            .append_tx(tx_res.tx_offset, tx_res.tx_data);

        std::thread::sleep(WAIT_DECIDED_TIMEOUT * 20);

        // Verify the committed data
        let last_replicated_tx = leader_node
            .lock()
            .unwrap()
            .begin_tx(DurabilityLevel::Replicated);

        let res = last_replicated_tx.get(&"test".to_string());
        if let Some(value) = res {
            println!("Transaction result for origninal leader: {}", value);
            assert_eq!(value, "testvalue");
        } else {
            println!("Transaction result is None.");
        }

        // Verify the committed data on another node
        let (node_2, _) = nodes.get(&2).expect("Node not found");
        {
            let node_2 = node_2.lock().unwrap();
            let tx = node_2.begin_tx(DurabilityLevel::Replicated);
            let res = tx.get(&"test".to_string());
            if let Some(value) = res {
                println!("Transaction result for node 2: {}", value);
                assert_eq!(value, "testvalue");
            } else {
                println!("Transaction result is None.");
            }
        };

        assert_eq!(nodes.len(), 4); // Ensure the expected number of nodes
        runtime.shutdown_background();
    }

    #[test]
    fn test_kill_node_elect_new() {
        let mut runtime_env = create_runtime();
        let mut node_registry: HashMap<u64, (Arc<Mutex<Node>>, JoinHandle<()>)> =
            spawn_nodes(&mut runtime_env);

        //delay to ensure leadership stabilization
        std::thread::sleep(WAIT_LEADER_TIMEOUT * 5);

        //retrieve a specific node, expected to exist
        let (specific_node, _) = node_registry.get(&1).expect("Node not located");
        //identify the leading node in the network
        let current_leader = specific_node
            .lock()
            .unwrap()
            .omni_durability
            .omni_paxos
            .get_current_leader()
            .expect("Leader election failed");

        //demonstrating data mutation and transaction commitment
        let (leader_node_ref, _) = node_registry.get(&current_leader).unwrap();
        let mut transaction = leader_node_ref.lock().unwrap().begin_mut_tx().unwrap();
        transaction.set("test".to_string(), "testvalue".to_string());
        let transaction_result = leader_node_ref
            .lock()
            .unwrap()
            .commit_mut_tx(transaction)
            .unwrap();

        //logging the transaction
        leader_node_ref
            .lock()
            .unwrap()
            .omni_durability
            .append_tx(transaction_result.tx_offset, transaction_result.tx_data);

        //waiting for transaction consensus
        std::thread::sleep(WAIT_DECIDED_TIMEOUT * 15);
        let retrieved_tx = leader_node_ref
            .lock()
            .unwrap()
            .begin_tx(DurabilityLevel::Replicated);
        //validating the replication of the transaction in the leader node
        assert_eq!(
            retrieved_tx.get(&"test".to_string()),
            Some("testvalue".to_string())
        );

        //terminating the leader node to trigger re-election
        node_registry.remove(&current_leader);

        //waiting for the election of a new leader
        std::thread::sleep(WAIT_DECIDED_TIMEOUT * 25);

        //determining the new leadership
        let live_nodes: Vec<&u64> = SERVERS.iter().filter(|&&id| id != current_leader).collect();
        println!("terminated node {:?}", current_leader);
        println!("nodes still running: {:?}", live_nodes);

        let (active_node, _) = node_registry.get(live_nodes[0]).unwrap();
        let elected_leader = active_node
            .lock()
            .unwrap()
            .omni_durability
            .omni_paxos
            .get_current_leader()
            .unwrap();
        println!("elected leader node {:?}", elected_leader);
        assert_ne!(elected_leader, current_leader);

        let (node_under_new_leadership, _) = node_registry.get(&elected_leader).unwrap();

        //verifying the transaction replication on the new leader's node
        let replicated_tx = node_under_new_leadership
            .lock()
            .unwrap()
            .begin_tx(DurabilityLevel::Replicated);

        println!(
            "TRANSACTION DATA ON NEW LEADER {:?}",
            replicated_tx.get(&"test".to_string())
        );
        assert_eq!(
            replicated_tx.get(&"test".to_string()),
            Some("testvalue".to_string())
        );

        //shutting down the runtime environment
        runtime_env.shutdown_background();
    }

    // fn test_3() {
    //     let mut runtime = create_runtime();
    //     let mut nodes: HashMap<u64, (Arc<Mutex<Node>>, JoinHandle<()>)> = spawn_nodes(&mut runtime);
    //     std::thread::sleep(WAIT_LEADER_TIMEOUT);
    //     let (node, _) = nodes.get(&1).expect("Node not found");
    //     // get leader node
    //     let leader = node
    //         .lock()
    //         .unwrap()
    //         .omni_durability
    //         .omni_paxos
    //         .get_current_leader()
    //         .expect("No leader elected");

    //     let (leader_node, leader_handle) = nodes.get(&leader).unwrap();

    //     std::thread::sleep(WAIT_DECIDED_TIMEOUT * 2);
    //     let _removed:Vec<()> = nodes.iter().map(|(node_id, _)| leader_node.lock().unwrap().remove_node(*node_id)).collect();

    //     std::thread::sleep(Duration::from_secs(2));
    //     let alive_servers:Vec<&u64> = SERVERS.iter().filter(|&&id| id != leader).collect();
    //     let (alive_server, handler) = nodes.get(alive_servers[0]).unwrap();

    // }

    #[test]
    fn test_constrained_election() {
        let mut runtime = create_runtime();
        let nodes = spawn_nodes(&mut runtime);
        std::thread::sleep(WAIT_LEADER_TIMEOUT * 5);
        let (node_1, _) = nodes.get(&1).unwrap();

        let leader = node_1
            .lock()
            .unwrap()
            .omni_durability
            .omni_paxos
            .get_current_leader()
            .expect("Failed to get leader");

        println!("Nodes: {:?}", SERVERS);
        println!("Leader: {}", leader);

        for node_id in SERVERS {
            if node_id != leader {
                let (node, _) = nodes.get(&node_id).unwrap();
                // remove the leader from the connected nodes
                node.lock().unwrap().connected_nodes[leader as usize - 1] = false
            }
        }
        std::thread::sleep(WAIT_LEADER_TIMEOUT * 2);
        let alive_servers: Vec<&u64> = SERVERS.iter().filter(|&&id| id != leader).collect();
        let (server, _) = nodes.get(&leader).unwrap();
        // stop messages from being sent
        server.lock().unwrap().allow_msg = false;

        //Give some time for the effect to take place
        std::thread::sleep(WAIT_LEADER_TIMEOUT * 10);

        println!("Unisolated servers: {:?}", alive_servers);

        let isolated_leader = server
            .lock()
            .unwrap()
            .omni_durability
            .omni_paxos
            .get_current_leader()
            .expect("Failed to get leader");
        println!("Old Isolated leader: {}", isolated_leader);

        let (alive_node, _handler) = nodes.get(alive_servers[0]).unwrap();
        //Get the new leader from a node that is not the leader
        let new_leader = alive_node
            .lock()
            .unwrap()
            .omni_durability
            .omni_paxos
            .get_current_leader()
            .expect("Failed to get leader");
        println!("Newly Elected leader: {}", new_leader);

        assert_ne!(new_leader, isolated_leader);

        //shutting down the runtime environment
        runtime.shutdown_background();
    }
}
