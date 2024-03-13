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
use tokio::time::sleep;
use tokio::time::Duration;

use crate::durability::omnipaxos_durability::Transaction;

use self::tx_data::DeleteList;

pub struct NodeRunner {
    pub node: Arc<Mutex<Node>>,
    sender_channels: HashMap<NodeId, mpsc::Sender<Message<Transaction>>>,
    receiver_channels: HashMap<NodeId, Arc<Mutex<mpsc::Receiver<Message<Transaction>>>>>,
    // TODO Messaging and running
}

impl NodeRunner {
    async fn tick(&mut self) {
        self.node.lock().unwrap().omni_durability.omni_paxos.tick();
    }

    async fn send_outgoing_msgs(&mut self) {
        let node_id = self.node.lock().unwrap().node_id;
        let sender = self
            .sender_channels
            .get(&node_id)
            .expect("Sender channel not found");
        for out_msg in self
            .node
            .lock()
            .unwrap()
            .omni_durability
            .omni_paxos
            .outgoing_messages()
        {
            // Send out_msg to receiver on network layer
            sender.send(out_msg).await.expect("Failed to send message");
        }
    }

    async fn handle_incoming_msgs(&mut self) {
        let mut node = self.node.lock().unwrap();
        let node_id = node.node_id;
        let receiver_arc = self
            .receiver_channels
            .get(&node_id)
            .expect("Receiver channel not found");
        let mut receiver = receiver_arc.lock().unwrap();

        loop {
            match receiver.recv().await {
                Some(msg) => {
                    // Handle incoming message
                    node.omni_durability.omni_paxos.handle_incoming(msg);
                }
                None => {
                    // Channel closed
                    break;
                }
            }
        }
    }

    pub async fn run(&mut self) {
        let mut ticker = tokio::time::interval(Duration::from_millis(100));
    
        loop {
            tokio::select! {
                _ = ticker.tick() => {
                    self.tick().await;
                    self.send_outgoing_msgs().await;
                    self.handle_incoming_msgs().await;
                }

            }
        }
    }
    
    
    
    
}

pub struct Node {
    node_id: NodeId,
    tx_offset: TxOffset,
    pub omni_durability: OmniPaxosDurability,
    data_store: ExampleDatastore,
    //
}

impl Node {
    pub fn new(node_id: NodeId, omni_durability: OmniPaxosDurability) -> Self {
        Node {
            node_id,
            tx_offset: TxOffset(0),
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
        let txns = self.omni_durability.iter();

        for (tx_offset, tx_data) in txns {
            if tx_offset > self.tx_offset {
                self.tx_offset = tx_offset;
                let _ = self.data_store.replay_transaction(&tx_data);
            }
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
            let tx_offset = self.tx_offset + 1;
            let res = self.data_store.commit_mut_tx(tx);

            match res {
                Ok(tx_result) => {
                    let tx_data = tx_result.tx_data.clone(); // Extract tx_data from TxResult
                    self.omni_durability.append_tx(tx_offset, tx_data);
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
        self.data_store
            .advance_replicated_durability_offset(self.tx_offset)
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
        const BUFFER_SIZE: usize = 100;

        // Create HashMaps to store senders and receivers
        let mut senders = HashMap::new();
        let mut receivers = HashMap::new();

        for &node_id in SERVERS.iter() {
            // Create a channel for each node
            let (sender, receiver) = mpsc::channel::<Message<Transaction>>(BUFFER_SIZE);

            // Insert the sender and receiver into the HashMaps
            senders.insert(node_id, sender);
            receivers.insert(node_id, receiver);
        }
        (senders, receivers)
    }

    fn create_runtime() -> Runtime {
        Builder::new_multi_thread()
            .worker_threads(4)
            .enable_all()
            .build()
            .unwrap()
    }

    fn spawn_nodes(
        runtime: &mut Runtime,
        sender_channels: HashMap<NodeId, mpsc::Sender<Message<Transaction>>>,
        receiver_channels: HashMap<NodeId, Arc<Mutex<mpsc::Receiver<Message<Transaction>>>>>,
    ) -> HashMap<NodeId, (Arc<Mutex<Node>>, JoinHandle<()>)> {
        let mut nodes = HashMap::new();
    
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
    
            let cloned_node = Arc::clone(&node);
            let sender_channels_clone = sender_channels.clone();
            let receiver_channels_clone = receiver_channels.clone();
    
            // Spawn a task for the NodeRunner
            let handle = runtime.spawn(async move {
                let mut node_runner = NodeRunner {
                    node: cloned_node,
                    sender_channels: sender_channels_clone,
                    receiver_channels: receiver_channels_clone,
                };
    
                node_runner.run().await;
            });
    
            // Insert the node and handle into the HashMap
            nodes.insert(node_id, (node, handle));
        }
    
        nodes
    }
    
    #[test]
    fn test_spawn_nodes() {
        let (sender_channels, receiver_channels) = initialise_channels();
        let receiver_channels: HashMap<NodeId, Arc<Mutex<mpsc::Receiver<Message<Transaction>>>>> =
        receiver_channels
            .into_iter()
            .map(|(k, v)| (k, Arc::new(Mutex::new(v))))
            .collect();
        let mut runtime = create_runtime();
        let nodes: HashMap<u64, (Arc<Mutex<Node>>, JoinHandle<()>)> = spawn_nodes(&mut runtime, sender_channels, receiver_channels);

        println!("Number of nodes: {}", nodes.len());

        runtime.block_on(async {
            tokio::time::sleep(Duration::from_secs(2)).await;
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
        println!("Leader ID: {:?}", leader_id);

        // Example: Mutate data and commit transaction
        let (leader_node, _) = nodes.get(&1).expect("Leader not found");
        let mut leader_node = leader_node.lock().unwrap();
        println!("leader node {}", leader_node.node_id);

        let mut mut_tx = leader_node.begin_mut_tx().unwrap();

        mut_tx.set("test".to_string(), "testvalue".to_string());
        let tx_res = leader_node.commit_mut_tx(mut_tx);

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
            let mut node_2 = node_2.lock().unwrap();
            node_2.apply_replicated_txns();
            node_2.begin_tx(DurabilityLevel::Memory);
            let tx = node_2.begin_tx(DurabilityLevel::Memory);
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
