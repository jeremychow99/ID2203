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

use self::tx_data::DeleteList;

pub const BUFFER_SIZE: usize = 10000;
pub const ELECTION_TICK_TIMEOUT: u64 = 5;
pub const TICK_PERIOD: Duration = Duration::from_millis(10);
pub const OUTGOING_MESSAGE_PERIOD: Duration = Duration::from_millis(1);
pub const UPDATE_DB_PERIOD: Duration = Duration::from_millis(1);

pub const WAIT_LEADER_TIMEOUT: Duration = Duration::from_millis(500);
pub const WAIT_DECIDED_TIMEOUT: Duration = Duration::from_millis(50);
pub struct NodeRunner {
    pub node: Arc<Mutex<Node>>,
    incoming: mpsc::Receiver<Message<Transaction>>,
    outgoing: HashMap<NodeId, mpsc::Sender<Message<Transaction>>>,
    // TODO Messaging and running
}

impl NodeRunner {
    // async fn send_outgoing_msgs(&mut self) {
    //     let node_id = self.node.lock().unwrap().node_id;
    //     let sender = self
    //         .sender_channels
    //         .get(&node_id)
    //         .expect("Sender channel not found");
    //     for out_msg in self
    //         .node
    //         .lock()
    //         .unwrap()
    //         .omni_durability
    //         .omni_paxos
    //         .outgoing_messages()
    //     {
    //         // Send out_msg to receiver on network layer
    //         sender.send(out_msg).await.expect("Failed to send message");
    //     }
    // }
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
            // if !self
            //     .node
            //     .lock()
            //     .unwrap()
            //     .seperated_nodes
            //     .contains(&receiver)
            // {
            let _ = channel.send(msg).await;
            // }
        }
    }

    pub async fn run(&mut self) {
        self.node.lock().unwrap().update_leader();
        let mut outgoing_interval = time::interval(OUTGOING_MESSAGE_PERIOD);
        let mut tick_interval = time::interval(TICK_PERIOD);
        let mut update_db_tick_interval = time::interval(UPDATE_DB_PERIOD);
        let mut remove_ndisconected_nodes_interval = time::interval(UPDATE_DB_PERIOD);

        sleep(Duration::from_secs(1)).await;
        // Call the method to send outgoing messages
        // self.send_outgoing_msgs().await;
        loop {
            tokio::select! {
                biased;
                _ = tick_interval.tick() => {
                    self.node.lock().unwrap().omni_durability.omni_paxos.tick();
                    self.node.lock().unwrap().update_leader();
                },
                _ = outgoing_interval.tick() => {
                    self.send_outgoing_msgs().await;
                },
                _ = update_db_tick_interval.tick() => {
                    self.node.lock().unwrap().apply_replicated_txns();
                },
                Some(msg) = self.incoming.recv() => {
                    let receiver = msg.get_receiver();
                    self.node.lock().unwrap().omni_durability.omni_paxos.handle_incoming(msg);
                }
            }
        }
    }
}

pub struct Node {
    node_id: NodeId,
    pub omni_durability: OmniPaxosDurability,
    data_store: ExampleDatastore,
    //
}

impl Node {
    pub fn new(node_id: NodeId, omni_durability: OmniPaxosDurability) -> Self {
        Node {
            node_id,
            omni_durability,
            data_store: ExampleDatastore::new(),
        }
    }

    /// update who is the current leader. If a follower becomes the leader,
    /// it needs to apply any unapplied txns to its datastore.
    /// If a node loses leadership, it needs to rollback the txns committed in
    /// memory that have not been replicated yet.
    pub fn update_leader(&mut self) {
        //IMPORTANT
        // call tick to increment clocks
        for _ in 0..100 {
            self.omni_durability.omni_paxos.tick();
        }

        let leader = self.omni_durability.omni_paxos.get_current_leader();
        if leader == Some(self.node_id) {
            self.apply_replicated_txns();
        } else {
            // if no longer leader, rollback txns committed in memory
            let _ = self.data_store.rollback_to_replicated_durability_offset();
        }
    }

    /// Apply the transactions that have been decided in OmniPaxos to the Datastore.
    /// We need to be careful with which nodes should do this according to desired
    /// behavior in the Datastore as defined by the application.
    fn apply_replicated_txns(&mut self) {
        // apply replicated transactions if follower
        let txns = self.omni_durability.iter();
        // apply replicated transactions if follower
        let txns_vec: Vec<_> = self.omni_durability.iter().collect();
        let length = txns_vec.len();
        // println!("Length of txns: {}", length);

        for (tx_offset, tx_data) in txns_vec {
            // self.advance_replicated_durability_offset().unwrap();
            let _ = self.data_store.replay_transaction(&tx_data);
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
            // If the current node is the leader, commit the mutable transaction
            let res = self.data_store.commit_mut_tx(tx);

            match res {
                Ok(tx_result) => {
                    let tx_data = tx_result.tx_data.clone(); // Extract tx_data from TxResult
                    self.omni_durability.append_tx(tx_result.tx_offset.clone(), tx_data);
                    Ok(tx_result)
                }
                Err(err) => Err(err),
            }
        } else {
            Err(DatastoreError::NotLeader)
        }
    }

    fn advance_replicated_durability_offset(
        &self,
    ) -> Result<(), crate::datastore::error::DatastoreError> {
        // advance the replicated durability offset

        let tx_offset = self.omni_durability.get_durable_tx_offset();
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
    use crate::durability::omnipaxos_durability::Transaction;
    use crate::node::*;
    use omnipaxos::messages::Message;
    use omnipaxos::util::NodeId;
    use omnipaxos_storage::memory_storage::MemoryStorage;
    use std::collections::HashMap;
    use std::sync::{Arc, Mutex};
    use tokio::runtime::{Builder, Runtime};
    use tokio::sync::mpsc;
    use tokio::task::JoinHandle;

    const SERVERS: [NodeId; 3] = [1, 2, 3];

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
                nodes: vec![1, 2, 3],
                ..Default::default()
            };

            let server_config = ServerConfig {
                pid: node_id,
                ..Default::default()
            };

            let omnipaxos_config = OmniPaxosConfig {
                cluster_config,
                server_config,
            };

            let omni_paxos: OmniPaxos<Transaction, MemoryStorage<Transaction>> =
                omnipaxos_config.build(storage.clone()).unwrap();

            // Create an instance of OmniPaxosDurability with appropriate parameters
            let omni_durability = OmniPaxosDurability {
                omni_paxos: omni_paxos,
            };

            // Create an instance of Node
            let node = Arc::new(Mutex::new(Node::new(node_id, omni_durability)));
            let mut node_runner = NodeRunner {
                node: Arc::clone(&node),
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

    #[test]
    fn test_spawn_nodes() {
        let mut runtime = create_runtime();
        let nodes: HashMap<u64, (Arc<Mutex<Node>>, JoinHandle<()>)> = spawn_nodes(&mut runtime);

        println!("Number of nodes: {}", nodes.len());

        runtime.block_on(async {
            tokio::time::sleep(Duration::from_secs(1)).await;
        });

        // Retrieve the node to test
        let (node, _) = nodes.get(&1).expect("Node not found");

        // Access the Node instance and perform transactions
        let leader_id = {
            let node_ref = node.lock().unwrap();
            node_ref
                .omni_durability
                .omni_paxos
                .get_current_leader()
                .expect("Leader ID not found")
        };

        println!("Leader ID: {}", leader_id);
        // Example: Mutate data and commit transaction
        let (leader_node, _) = nodes.get(&leader_id).expect("Leader not found");
        let mut leader_node = leader_node.lock().unwrap();
        println!("leader node {}", leader_node.node_id);

        let mut mut_tx = leader_node.begin_mut_tx().unwrap();

        mut_tx.set("test".to_string(), "testvalue".to_string());
        let tx_res = leader_node.commit_mut_tx(mut_tx);

        runtime.block_on(async {
            tokio::time::sleep(Duration::from_secs(1)).await;
        });

        // Verify the committed data
        let tx = leader_node.begin_tx(DurabilityLevel::Memory);
        let res = tx.get(&"test".to_string());
        if let Some(value) = res {
            println!("Transaction result: {}", value);
            assert_eq!(value, "testvalue");
        } else {
            println!("Transaction result is None.");
        }

        // Verify the committed data
        let (node_2, _) = nodes.get(&2).expect("Node not found");
        {
            let node_2 = node_2.lock().unwrap();

            runtime.block_on(async {
                tokio::time::sleep(Duration::from_secs(1)).await;
            });

            let tx = node_2.begin_tx(DurabilityLevel::Replicated);
            let res = tx.get(&"test".to_string());
            if let Some(value) = res {
                println!("Transaction result: {}", value);
                assert_eq!(value, "testvalue");
            } else {
                println!("Transaction result is None.");
            }
        };

        assert_eq!(nodes.len(), 3); // Ensure the expected number of nodes
    }
}
