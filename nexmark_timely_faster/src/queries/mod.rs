use std::rc::Rc;
use timely::dataflow::operators::capture::event::link::EventLink;
use timely::dataflow::operators::capture::Replay;
use timely::dataflow::{Scope, Stream};

use crate::event::{Auction, Bid, Date, Person};

mod window_1_faster;
mod window_1_faster_count;
mod window_1_faster_count_custom_slice;
mod window_1_faster_rank;
mod window_1_faster_rank_custom_slice;
mod window_2_faster;
mod window_1_rocksdb;
mod window_1_rocksdb_count;
mod window_1_rocksdb_rank;
mod window_2a_rocksdb;
mod window_2a_rocksdb_count;
mod window_2a_rocksdb_rank;
mod window_2b_rocksdb;
mod window_2b_rocksdb_count;
mod window_2b_rocksdb_rank;
mod keyed_window_2b_rocksdb_rank;
mod window_2_faster_count;
mod keyed_window_3_faster_count;
mod window_2_faster_rank;
mod keyed_window_2_faster_rank;
mod window_3_faster;
mod window_3a_rocksdb;
mod window_3a_rocksdb_count;
mod keyed_window_3a_rocksdb_count;
mod window_3a_rocksdb_rank;
mod window_3b_rocksdb;
mod window_3b_rocksdb_count;
mod window_3b_rocksdb_rank;
mod window_3_faster_count;
mod window_3_faster_rank;

mod q3_managed;
mod q4;
mod q4_managed;
mod q4_q6_common_managed;
mod q5_managed;
mod q5_managed_index;
mod q6_managed;
mod q7_managed;
mod q8_managed;
mod q8_managed_map;

pub use self::q3_managed::q3_managed;
pub use self::q4::q4;
pub use self::q4_managed::q4_managed;
pub use self::q4_q6_common_managed::q4_q6_common_managed;
pub use self::q5_managed::q5_managed;
pub use self::q5_managed_index::q5_managed_index;
pub use self::q6_managed::q6_managed;
pub use self::q7_managed::q7_managed;
pub use self::q8_managed::q8_managed;
pub use self::q8_managed_map::q8_managed_map;

pub use self::window_1_rocksdb::window_1_rocksdb;
pub use self::window_1_rocksdb_count::window_1_rocksdb_count;
pub use self::window_1_rocksdb_rank::window_1_rocksdb_rank;
pub use self::window_2a_rocksdb::window_2a_rocksdb;
pub use self::window_2a_rocksdb_count::window_2a_rocksdb_count;
pub use self::window_2a_rocksdb_rank::window_2a_rocksdb_rank;
pub use self::window_2b_rocksdb::window_2b_rocksdb;
pub use self::window_2b_rocksdb_count::window_2b_rocksdb_count;
pub use self::window_2b_rocksdb_rank::window_2b_rocksdb_rank;
pub use self::keyed_window_2b_rocksdb_rank::keyed_window_2b_rocksdb_rank;
pub use self::window_3a_rocksdb::window_3a_rocksdb;
pub use self::window_3a_rocksdb_count::window_3a_rocksdb_count;
pub use self::keyed_window_3a_rocksdb_count::keyed_window_3a_rocksdb_count;
pub use self::window_3a_rocksdb_rank::window_3a_rocksdb_rank;
pub use self::window_3b_rocksdb::window_3b_rocksdb;
pub use self::window_3b_rocksdb_count::window_3b_rocksdb_count;
pub use self::window_3b_rocksdb_rank::window_3b_rocksdb_rank;
pub use self::window_1_faster::window_1_faster;
pub use self::window_1_faster_count::window_1_faster_count;
pub use self::window_1_faster_count_custom_slice::window_1_faster_count_custom_slice;
pub use self::window_1_faster_rank::window_1_faster_rank;
pub use self::window_1_faster_rank_custom_slice::window_1_faster_rank_custom_slice;
pub use self::window_2_faster::window_2_faster;
pub use self::window_2_faster_count::window_2_faster_count;
pub use self::window_2_faster_rank::window_2_faster_rank;
pub use self::keyed_window_2_faster_rank::keyed_window_2_faster_rank;
pub use self::window_3_faster::window_3_faster;
pub use self::window_3_faster_count::window_3_faster_count;
pub use self::keyed_window_3_faster_count::keyed_window_3_faster_count;
pub use self::window_3_faster_rank::window_3_faster_rank;

use faster_rs::FasterKv;

#[inline(always)]
fn maybe_refresh_faster(faster: &FasterKv, monotonic_serial_number: &mut u64) {
    if *monotonic_serial_number % (1 << 4) == 0 {
        faster.refresh();
        if *monotonic_serial_number % (1 << 10) == 0 {
            faster.complete_pending(true);
        }
    }
    if *monotonic_serial_number % (1 << 20) == 0 {
        println!("Size: {}", faster.size());
    }
    *monotonic_serial_number += 1;
}


pub struct NexmarkInput<'a> {
    pub bids: &'a Rc<EventLink<usize, Bid>>,
    pub auctions: &'a Rc<EventLink<usize, Auction>>,
    pub people: &'a Rc<EventLink<usize, Person>>,
    pub closed_auctions: &'a Rc<EventLink<usize, (Auction, Bid)>>,
    pub closed_auctions_flex: &'a Rc<EventLink<usize, (Auction, Bid)>>,
}

impl<'a> NexmarkInput<'a> {
    pub fn bids<S: Scope<Timestamp = usize>>(&self, scope: &mut S) -> Stream<S, Bid> {
        Some(self.bids.clone()).replay_into(scope)
    }

    pub fn auctions<S: Scope<Timestamp = usize>>(&self, scope: &mut S) -> Stream<S, Auction> {
        Some(self.auctions.clone()).replay_into(scope)
    }

    pub fn people<S: Scope<Timestamp = usize>>(&self, scope: &mut S) -> Stream<S, Person> {
        Some(self.people.clone()).replay_into(scope)
    }

    pub fn closed_auctions<S: Scope<Timestamp = usize>>(
        &self,
        scope: &mut S,
    ) -> Stream<S, (Auction, Bid)> {
        Some(self.closed_auctions.clone()).replay_into(scope)
    }
}

#[derive(Copy, Clone)]
pub struct NexmarkTimer {
    pub time_dilation: usize,
}

impl NexmarkTimer {
    #[inline(always)]
    fn to_nexmark_time(self, x: usize) -> Date {
        debug_assert!(
            x.checked_mul(self.time_dilation).is_some(),
            "multiplication failed: {} * {}",
            x,
            self.time_dilation
        );
        Date::new(x * self.time_dilation)
    }

    #[inline(always)]
    fn from_nexmark_time(self, x: Date) -> usize {
        *x / self.time_dilation
    }
}
