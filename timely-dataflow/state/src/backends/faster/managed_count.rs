use crate::backends::faster::{faster_read, faster_rmw, faster_upsert};
use crate::primitives::ManagedCount;
use faster_rs::{status, FasterKv};
use std::cell::RefCell;
use std::rc::Rc;
use std::sync::Arc;

pub struct FASTERManagedCount {
    faster: Arc<FasterKv>,
    monotonic_serial_number: Rc<RefCell<u64>>,
    name: String,
}

impl FASTERManagedCount {
    pub fn new(
        faster: Arc<FasterKv>,
        monotonic_serial_number: Rc<RefCell<u64>>,
        name: &str,
    ) -> Self {
        FASTERManagedCount {
            faster,
            monotonic_serial_number,
            name: name.to_owned(),
        }
    }
}

impl ManagedCount for FASTERManagedCount {
    fn decrease(&mut self, amount: i64) {
        faster_rmw(
            &self.faster,
            &self.name,
            &-amount,
            &self.monotonic_serial_number,
        );
    }

    fn increase(&mut self, amount: i64) {
        faster_rmw(
            &self.faster,
            &self.name,
            &amount,
            &self.monotonic_serial_number,
        );
    }

    fn get(&self) -> i64 {
        let (status, recv) = faster_read(&self.faster, &self.name, &self.monotonic_serial_number);
        if status != status::OK {
            return 0;
        }
        return match recv.recv() {
            Ok(count) => count,
            Err(_) => 0,
        };
    }

    fn set(&mut self, value: i64) {
        faster_upsert(
            &self.faster,
            &self.name,
            &value,
            &self.monotonic_serial_number,
        );
    }
}
