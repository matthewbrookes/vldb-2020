use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::{Scope, Stream};

use std::collections::HashMap;

use crate::queries::{NexmarkInput, NexmarkTimer};
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::map::Map;

// NOTE: This is a *very* quick-n-dirty implementation of a keyed window on RocksDB
pub fn keyed_window_3a_rocksdb_count<S: Scope<Timestamp = usize>>(
    input: &NexmarkInput,
    _nt: NexmarkTimer,
    scope: &mut S,
    window_slice_count: usize,
    window_slide_ns: usize,
) -> Stream<S, (usize, usize)> {

    let mut last_slide_seen = 0;

    input
        .bids(scope)
        .map(move |b| {
            (
                b.auction,
                *b.date_time
            )
        })
        .unary_notify(
            Exchange::new(|b: &(usize, _)| b.0 as u64),
            "Accumulate records",
            None,
            move |input, output, notificator, state_handle| {
                // slice end timestamp -> distinct keys in slice
                let mut state_index = state_handle.get_managed_map("state_index1");
                // (key, pane end timestamp) -> keyed pane contents
                let mut pane_buckets = state_handle.get_managed_map("pane_buckets");
                let prefix_key_len_1: usize = state_index.as_ref().get_key_prefix_length();
                let prefix_key_len_2: usize = pane_buckets.as_ref().get_key_prefix_length();
                let mut buffer = Vec::new();
                input.for_each(|time, data| {
                    // The end timestamp of the slide the current epoch corresponds to
                    let current_slide = ((time.time() / window_slide_ns) + 1) * window_slide_ns;
                    // println!("Current slide: {:?}", current_slide);
                    // Ask notifications for all remaining slides up to the current one
                    assert!(last_slide_seen <= current_slide);
                    if last_slide_seen < current_slide {
                        let start = last_slide_seen + window_slide_ns;
                        let end = current_slide + window_slide_ns;
                        for sl in (start..end).step_by(window_slide_ns) {
                            let window_end = sl + window_slide_ns * (window_slice_count - 1);
                            // println!("Asking notification for the end of window: {:?}", window_end);
                            notificator.notify_at(time.delayed(&window_end));
                        }
                        last_slide_seen = current_slide;
                    }
                    // Update state with the new records
                    data.swap(&mut buffer);
                    for record in buffer.iter() {
                        // Add key to the index, if not exists
                        let slice = ((record.1 / 1_000_000_000) + 1) * 1_000_000_000;
                        let mut exists = false;
                        let keys: Option<std::rc::Rc<Vec<usize>>> = state_index.get(&slice.to_be());
                        if keys.is_some()  {
                            let keys = keys.unwrap();
                            exists = keys.iter().any(|k: &usize| *k==record.0);
                        }
                        if !exists { // Add key in slice
                            let mut keys = state_index.remove(&slice.to_be()).unwrap_or(Vec::new());
                            keys.push(record.0);
                            // println!("Inserting slice {} with keys {:?} to index", slice, keys);
                            state_index.insert(slice.to_be(), keys);
                        }
                        // Add record to state
                        // Pane size equals slide size as window is a multiple of slide
                        let pane = ((record.1 / window_slide_ns) + 1) * window_slide_ns;  
                        // println!("Inserting record with time {:?} in keyed pane {:?}", record.1, (record.0, pane));
                        pane_buckets.rmw((record.0.to_be(), pane.to_be()), 1 as usize);
                    }
                });

                notificator.for_each(|cap, _, _| {
                    let window_end = cap.time();
                    let window_start = window_end - (window_slide_ns * window_slice_count);
                    let first_pane = window_start + window_slide_ns; // To know which records to delete
                    // println!("Start of window: {}", window_start);
                    // println!("End of window: {}", *window_end);
                    // println!("End of first slide: {}", first_pane_end);

                    // Step 1: Get all distinct keys in all slices of the expired window
                    let mut all_keys = HashMap::new();  // key -> min slice the key appears in
                    let slice_start = window_start + 1_000_000_000;
                    // println!("Slice start: {:?}", slice_start);
                    let index_iter = state_index.iter(slice_start.to_be());
                    for (ser_key, ser_value) in index_iter {
                        let pref = &ser_key[..prefix_key_len_1];
                        let p: &str =  bincode::deserialize(unsafe {
                                                            std::slice::from_raw_parts(pref.as_ptr(), pref.len())
                                                      }).expect("Cannot deserialize prefix");
                        if p.find("index").is_none() {
                            break;
                        }
                        let k = &ser_key[prefix_key_len_1..];  // Ignore prefix
                        let mut timestamp: usize = bincode::deserialize(unsafe {
                                                            std::slice::from_raw_parts(k.as_ptr(), k.len())
                                                      }).expect("Cannot deserialize slice");
                        timestamp = usize::from_be(timestamp);  // The end timestamp of the pane
                        let keys: Vec<usize> = bincode::deserialize(unsafe {
                                                            std::slice::from_raw_parts(ser_value.as_ptr(), ser_value.len())
                                                        }).expect("Cannot deserialize keys");
                        // println!("Found distinct keys: time: {}, keys:{:?}", timestamp, keys);
                        if timestamp > *window_end {  // Outside keyed window
                            break;
                        }
                        for key in keys {
                            let e = all_keys.entry(key).or_insert(timestamp);
                            if *e > timestamp {
                                *e = timestamp;
                            } 
                        }
                    }

                    // Step 2: Output result for each keyed window
                    let last_pane = window_end + window_slide_ns;
                    let mut all_keys: Vec<(usize, usize)> = all_keys.into_iter().collect();
                    all_keys.sort_by(|a,b| a.0.cmp(&b.0));
                    {
                        for (key, min_slice) in all_keys.iter() {
                            // println!("Key {} Min slice {}", key, min_slice);
                            let mut count = 0;
                            let mut pane = (min_slice / window_slide_ns) * window_slide_ns;
                            if pane == 0 {
                                pane = first_pane;
                            }
                            // TODO (john): Do a prefix scan within the key
                            assert!((pane >= first_pane) && (pane < last_pane));
                            let composite_key = (key.to_be(), pane.to_be());
                            // println!("Composite Key {:?}", (key, pane));
                            let mut auction_id = 0;
                            let mut last_auction_id_seen = 0;
                            // Iterate over the panes belonging to the current window
                            let window_iter = pane_buckets.iter(composite_key);
                            for (ser_key, ser_value) in window_iter {
                                let pref = &ser_key[..prefix_key_len_2];
                                let p: &str =  bincode::deserialize(unsafe {
                                                        std::slice::from_raw_parts(pref.as_ptr(), pref.len())
                                                  }).expect("Cannot deserialize prefix");
                                if p.find("buckets").is_none() {
                                    break;
                                }
                                let k = &ser_key[prefix_key_len_2..];  // Ignore prefix
                                let (auction, mut timestamp): (usize, usize) = bincode::deserialize(unsafe {
                                                                    std::slice::from_raw_parts(k.as_ptr(), k.len())
                                                              }).expect("Cannot deserialize (key, timestamp)");
                                timestamp = usize::from_be(timestamp);  // The end timestamp of the pane
                                auction_id = usize::from_be(auction);
                                let record_count: usize = bincode::deserialize(unsafe {
                                                                    std::slice::from_raw_parts(ser_value.as_ptr(), ser_value.len())
                                                                }).expect("Cannot deserialize count");
                                // println!("Found keyed pane:: auction {} time: {} count:{}", auction_id, timestamp, record_count);
                                if timestamp > last_pane || (auction_id != last_auction_id_seen && last_auction_id_seen != 0){  // Outside keyed window
                                    auction_id = last_auction_id_seen;
                                    break;
                                }
                                last_auction_id_seen = auction_id;
                                count += record_count;
                            }
                            if auction_id != 0 {
                                // println!("*** End of window: {:?}, Auction: {} Count: {:?}", cap.time(), auction_id, count);
                                output.session(&cap).give((auction_id, count));
                            }
                        }
                    }
                    
                    // Step 3: Purge state of first slide/pane in window
                    for slice in (slice_start..first_pane+1).step_by(1_000_000_000) {
                        // println!("Slice to remove from index: {}", slice);
                        state_index.remove(&slice.to_be()).expect("Slice must exist in index");
                    }
                    for (key, _) in all_keys {
                        for pane in (first_pane..first_pane+1).step_by(window_slide_ns) {
                            // println!("Keyed pane to remove: {:?}", (key, pane));
                            let composite_key = (key.to_be(), pane.to_be());
                            // println!("Removing pane with end timestamp time: {}", first_pane_end);
                            pane_buckets.remove(&composite_key); //.expect("Keyed pane to remove must exist");
                        }
                    }
                });
            }
        )
}
