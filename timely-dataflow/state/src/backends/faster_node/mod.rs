extern crate faster_rs;
extern crate tempfile;

use managed_count::FASTERManagedCount;
use managed_map::FASTERManagedMap;
use managed_value::FASTERManagedValue;

mod managed_count;
mod managed_map;
mod managed_value;

use crate::primitives::{ManagedCount, ManagedMap, ManagedValue};
use crate::StateBackend;
use faster_rs::{FasterKey, FasterKv, FasterRmw, FasterValue};
use std::cell::RefCell;
use std::hash::Hash;
use std::rc::Rc;
use std::sync::mpsc::Receiver;
use std::sync::Arc;
use std::time::Duration;
use tempfile::TempDir;

#[allow(dead_code)]
pub struct FASTERNodeBackend {
    faster: Arc<FasterKv>,
    monotonic_serial_number: Rc<RefCell<u64>>,
    faster_directory: Arc<TempDir>,
}

fn maybe_refresh_faster(faster: &Arc<FasterKv>, monotonic_serial_number: u64) {
    if monotonic_serial_number % (1 << 4) == 0 {
        faster.refresh();
        if monotonic_serial_number % (1 << 10) == 0 {
            faster.complete_pending(true);
        }
    }
    if monotonic_serial_number % (1 << 20) == 0 {
        println!("Size: {}", faster.size());
    }
}

fn faster_upsert<K: FasterKey, V: FasterValue>(
    faster: &Arc<FasterKv>,
    key: &K,
    value: &V,
    monotonic_serial_number: &Rc<RefCell<u64>>,
) {
    let old_monotonic_serial_number = *monotonic_serial_number.borrow();
    *monotonic_serial_number.borrow_mut() = old_monotonic_serial_number + 1;
    faster.upsert(key, value, old_monotonic_serial_number);
    maybe_refresh_faster(faster, old_monotonic_serial_number);
}

fn faster_read<K: FasterKey, V: FasterValue>(
    faster: &Arc<FasterKv>,
    key: &K,
    monotonic_serial_number: &Rc<RefCell<u64>>,
) -> (u8, Receiver<V>) {
    let old_monotonic_serial_number = *monotonic_serial_number.borrow();
    *monotonic_serial_number.borrow_mut() = old_monotonic_serial_number + 1;
    let (status, recv) = faster.read(key, old_monotonic_serial_number);
    maybe_refresh_faster(faster, old_monotonic_serial_number);
    (status, recv)
}

fn faster_rmw<K: FasterKey, V: FasterValue + FasterRmw>(
    faster: &Arc<FasterKv>,
    key: &K,
    modification: &V,
    monotonic_serial_number: &Rc<RefCell<u64>>,
) {
    let old_monotonic_serial_number = *monotonic_serial_number.borrow();
    *monotonic_serial_number.borrow_mut() = old_monotonic_serial_number + 1;
    faster.rmw(key, modification, old_monotonic_serial_number);
    maybe_refresh_faster(faster, old_monotonic_serial_number);
}

impl StateBackend for FASTERNodeBackend {
    fn new() -> Self {
        unimplemented!();
    }

    fn get_managed_count(&self, name: &str) -> Box<ManagedCount> {
        Box::new(FASTERManagedCount::new(
            Arc::clone(&self.faster),
            Rc::clone(&self.monotonic_serial_number),
            name,
        ))
    }

    fn get_managed_value<V: 'static + FasterValue + FasterRmw>(
        &self,
        name: &str,
    ) -> Box<ManagedValue<V>> {
        Box::new(FASTERManagedValue::new(
            Arc::clone(&self.faster),
            Rc::clone(&self.monotonic_serial_number),
            name,
        ))
    }

    fn get_managed_map<K, V>(&self, name: &str) -> Box<ManagedMap<K, V>>
    where
        K: 'static + FasterKey + Hash + Eq + std::fmt::Debug,
        V: 'static + FasterValue + FasterRmw,
    {
        Box::new(FASTERManagedMap::new(
            Arc::clone(&self.faster),
            Rc::clone(&self.monotonic_serial_number),
            name,
        ))
    }
}

impl FASTERNodeBackend {
    pub fn new_from_existing(faster_kv: &Arc<FasterKv>, faster_directory: &Arc<TempDir>) -> Self {
        FASTERNodeBackend {
            faster: Arc::clone(faster_kv),
            monotonic_serial_number: Rc::new(RefCell::new(1)),
            faster_directory: Arc::clone(faster_directory),
        }
    }
}
