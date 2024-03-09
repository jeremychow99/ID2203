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
    // TODO Messaging and running
}

impl NodeRunner {
    async fn send_outgoing_msgs(&mut self) {
        todo!()
        // use omnipaxos messages for msg passing
    }

    pub async fn run(&mut self) {
        self.node.lock().unwrap().update_leader();

        sleep(Duration::from_secs(1)).await;

        // Call the method to send outgoing messages
        // self.send_outgoing_msgs().await;
    }
}

pub struct Node {
    node_id: NodeId,
    tx_offset: TxOffset,
    omni_durability: OmniPaxosDurability,
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
        self.omni_durability.omni_paxos.tick();

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
            let txns = self.omni_durability.iter();
            for (tx_offset, tx_data) in txns {
                if tx_offset > self.tx_offset {
                    self.omni_durability.append_tx(tx_offset, tx_data);
                }
            }
            self.data_store.commit_mut_tx(tx)
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

    fn  spawn_nodes(
        runtime: &mut Runtime,
        sender_channels: HashMap<NodeId, mpsc::Sender<Message<Transaction>>>,
        receiver_channels: HashMap<NodeId, mpsc::Receiver<Message<Transaction>>>,
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

            // Spawn a task for the NodeRunner
            let handle = runtime.spawn(async move {
                let mut node_runner = NodeRunner { node: cloned_node };

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
        let mut runtime = create_runtime();
        let nodes = spawn_nodes(&mut runtime, sender_channels, receiver_channels);

        println!("number of nodes: {}", nodes.len());
        assert!(nodes.len() == 3);

    }
}
