//! A simple rust implementation of twitter's snowflake id
//!
//! Twitters snowflake id is highly useful for distributed systems. It consists of a
//! 41 bit timestamp, a 5 bit datacenter id, 5 bit machine/node id and a 12 bit sequence.
//!
//! # Example
//! ```
//! use qanik::SnowFlake;
//!
//! fn main() {
//!     let snowflake: SnowFlake = SnowFlake::new(1, 1)
//!         .expect("Datacenter or machine id is too big.");
//!     let id: u64 = snowflake.generate_id();
//!     // Use the generated id to persist any record
//! }
//! ```

use anyhow::{anyhow, Error};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub const EPOCH_START: u128 = 1119657600000; // 2005-06-25

pub const DATACENTER_ID_BITS: u32 = 3;
pub const MACHINE_ID_BITS: u32 = 7;
pub const SEQUENCE_BITS: u32 = 12;

pub const MAX_DATACENTER_ID: u64 = (1 << DATACENTER_ID_BITS) - 1;
pub const MAX_MACHINE_ID: u64 = (1 << MACHINE_ID_BITS) - 1;
pub const MAX_SEQUENCE: u64 = (1 << SEQUENCE_BITS) - 1;

/// Struct containing the datacenter-, machine id and atomic sequence used to
/// generate the twitter snowflake id.
pub struct SnowFlake {
    datacenter_id: u64,
    machine_id: u64,
    sequence: AtomicU64,
}

impl SnowFlake {
    /// Create a new SnowFlake instance with for a datacenter and machine.
    pub fn new(datacenter_id: u64, machine_id: u64) -> Result<SnowFlake, Error> {
        if datacenter_id > MAX_DATACENTER_ID {
            return Err(anyhow!(
                "Datacenter id must be less than {}",
                MAX_DATACENTER_ID
            ));
        }

        if machine_id > MAX_MACHINE_ID {
            return Err(anyhow!("Machine id must be less than {}", MAX_MACHINE_ID));
        }

        let sequence = AtomicU64::new(1);
        Ok(Self {
            datacenter_id,
            machine_id,
            sequence,
        })
    }

    /// Generate a new snowflake id in the sequence for the current timestamp, datacenter and machine
    pub fn generate_id(&self) -> u64 {
        let sequence: &AtomicU64 = &self.sequence;
        let current = sequence.fetch_add(1, Ordering::Relaxed);
        sequence.compare_exchange(MAX_SEQUENCE, 1, Ordering::SeqCst, Ordering::Relaxed).unwrap_or_else(| e | e);
        if current == MAX_SEQUENCE {
            thread::sleep(Duration::from_millis(1))    
        }
        
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("Time went to heck!")
            .as_millis();

        let mut id: u64 = ((timestamp - EPOCH_START)
            << (DATACENTER_ID_BITS + MACHINE_ID_BITS + SEQUENCE_BITS))
            as u64;
        id = id
            | (self.datacenter_id << (MACHINE_ID_BITS + SEQUENCE_BITS))
            | (self.machine_id << SEQUENCE_BITS)
            | current;
        id
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    #[should_panic]
    fn datacenter_id_too_large() {
        let _snowflake = SnowFlake::new(128, 0).expect("Ha, got me datacenter errr...");
    }

    #[test]
    #[should_panic]
    fn machine_id_too_large() {
        let _snowflake = SnowFlake::new(0, 128).expect("Ha, got me machine errr...");
    }
    #[test]
    fn check_generated_id() {
        let snowflake = SnowFlake::new(1, 1).expect("Something went wrong...");
        let id = snowflake.generate_id();

        assert_eq!(id & 1, 1);
        assert_eq!(id >> SEQUENCE_BITS & 1, 1);
        assert_eq!(id >> (SEQUENCE_BITS + MACHINE_ID_BITS) & 1, 1);
    }
}
