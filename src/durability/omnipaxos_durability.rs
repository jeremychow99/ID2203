use std::thread;
use std::time::Duration;

use super::*;
use crate::datastore::{tx_data::TxData, TxOffset};
use omnipaxos::macros::Entry;
use omnipaxos::util::{LogEntry, SnapshottedEntry};
use omnipaxos::*;
use omnipaxos_storage::memory_storage::MemoryStorage;

#[derive(Clone, Debug, Entry)] // Clone and Debug are required traits.
pub struct Transaction {
    tx_offset: TxOffset,
    tx_data: TxData,
}

impl Transaction {
    pub fn new(tx_offset: TxOffset, tx_data: TxData) -> Self {
        Transaction { tx_offset, tx_data }
    }
}

type OmniPaxosStruct = OmniPaxos<Transaction, MemoryStorage<Transaction>>;

/// OmniPaxosDurability is an OmniPaxos node that should provide the replicated
/// implementation of the DurabilityLayer trait required by the Datastore.
pub struct OmniPaxosDurability {
   pub omni_paxos: OmniPaxosStruct,
    // more traits
}

impl OmniPaxosDurability {
    pub fn new(omni_paxos: OmniPaxos<Transaction, MemoryStorage<Transaction>>) -> Self {
        OmniPaxosDurability { omni_paxos }
    }
}

impl DurabilityLayer for OmniPaxosDurability {
    fn iter(&self) -> Box<dyn Iterator<Item = (TxOffset, TxData)>> {
        // You need to implement this method based on your requirements.
        // It should return an iterator over the transactions in the Omnipaxos log.
        if let Some(entries) = self.omni_paxos.read_entries(..) {
            let iter = entries
                .into_iter()
                .flat_map(|log_entry| {
                    match log_entry {
                        LogEntry::Decided(decided_entry) => Some(decided_entry),
                        LogEntry::Undecided(_undecided_entry) => Some(_undecided_entry),
                        LogEntry::Snapshotted(SnapshottedEntry { .. }) => {None}
                        LogEntry::Trimmed(_) | LogEntry::StopSign(_, _) => None,
}
                })
                .map(|log_entry| {
                    let tx_offset = log_entry.tx_offset;
                    let tx_data = log_entry.tx_data;
                    (tx_offset, tx_data)
                });

            // Box the iterator and return
            Box::new(iter)
        } else {
            // If read_entries returns None, return an empty iterator
            Box::new(std::iter::empty())
        }
    }

    fn iter_starting_from_offset(
        &self,
        offset: TxOffset,
    ) -> Box<dyn Iterator<Item = (TxOffset, TxData)>> {
        if let Some(entries) = self.omni_paxos.read_entries(offset.0..) {
            let iter = entries
                .into_iter()
                .flat_map(|log_entry| {
                    match log_entry {
                        LogEntry::Decided(decided_entry) => Some(decided_entry),
                        LogEntry::Undecided(undecided_entry) => Some(undecided_entry),
                        LogEntry::Snapshotted(SnapshottedEntry { .. }) => {None}
                        LogEntry::Trimmed(_) | LogEntry::StopSign(_, _) => None,
                    }
                })
                .map(|log_entry| {
                    let tx_offset = log_entry.tx_offset;
                    let tx_data = log_entry.tx_data;
                    (tx_offset, tx_data)
                });
            Box::new(iter)
        } else {
            Box::new(std::iter::empty())
        }
    }

    fn append_tx(&mut self, tx_offset: TxOffset, tx_data: TxData) {
        // You need to implement this method based on your requirements.
        // It should append the given transaction to the Omnipaxos log.
        let log_entry = Transaction::new(tx_offset, tx_data);
        match self.omni_paxos.append(log_entry) {
            Ok(()) => {
                println!("Transaction appended successfully");
                let entries = self.omni_paxos.read_entries(..);
                println!("Entries: {:?}", entries);
            }
            Err(err) => {
                println!("Failed to append transaction: {:?}", err);
            }
        }
    }

    fn get_durable_tx_offset(&self) -> TxOffset {
        // You need to implement this method based on your requirements.
        // It should return the offset of the last durable transaction.
        let index = self.omni_paxos.get_decided_idx();
        return TxOffset(index);
    }
    
}
