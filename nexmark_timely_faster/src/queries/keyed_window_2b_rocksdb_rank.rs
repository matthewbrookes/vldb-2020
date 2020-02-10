use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::{Scope, Stream};

use crate::queries::{NexmarkInput, NexmarkTimer};
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::map::Map;

use std::collections::HashSet;

pub fn assign_windows(event_time: usize,
                      window_slide: usize,
                      window_size: usize
                     ) -> Vec<usize> {
    let mut windows = Vec::new();
    let last_window_start = event_time - (event_time + window_slide) % window_slide;
    let num_windows = (window_size as f64 / window_slide as f64).ceil() as i64;
    for i in 0i64..num_windows {
        let w_id = last_window_start as i64 - (i * window_slide as i64);
        if w_id >= 0 && (event_time < w_id  as usize + window_size) {
            windows.push(w_id as usize);
        }
    }
    windows
}

// 2nd window implementation using merge
pub fn keyed_window_2b_rocksdb_rank<S: Scope<Timestamp = usize>>(
    input: &NexmarkInput,
    _nt: NexmarkTimer,
    scope: &mut S,
    window_slice_count: usize,
    window_slide_ns: usize,
) -> Stream<S, (usize, usize, usize)> {

    // let mut max_window_seen = 0;

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
                let mut state_index = state_handle.get_managed_map("index");
                let window_size = window_slice_count * window_slide_ns;
                // window_start_timestamp -> window_contents
                let mut window_buckets = state_handle.get_managed_map("window_buckets");
                let mut buffer = Vec::new();
                input.for_each(|time, data| {
                    data.swap(&mut buffer);
                    // The end timestamp of the slide the current epoch corresponds to
                    let slide = ((time.time() / window_slide_ns) + 1) * window_slide_ns;
                    // if max_window_seen < slide {
                    //     for window_start in (max_window_seen..slide).step_by(window_slide_ns) {
                    //         // Merging in RocksDB needs a first 'put' operation to work properly
                    //         // println!("First PUT operation for window start: {:?}", window_start);
                    //         window_buckets.insert(window_start, vec![]);  // Initialize window state
                    //     }
                    //     max_window_seen = slide;
                    // }
                    for record in buffer.iter() {
                        let windows = assign_windows(record.1, window_slide_ns, window_size);
                        // Add record to index
                        let slice = ((record.1 / 1_000_000_000) + 1) * 1_000_000_000;
                        let mut exists = false;
                        let keys: Option<std::rc::Rc<Vec<usize>>> = state_index.get(&slice);
                        if keys.is_some()  {
                            let keys = keys.unwrap();
                            exists = keys.iter().any(|k: &usize| *k==record.0);
                        }
                        if !exists { // Add key in slice
                            let mut keys = state_index.remove(&slice).unwrap_or(Vec::new());
                            keys.push(record.0);
                            // println!("Inserting slice {} with keys {:?} to index", slice, keys);
                            state_index.insert(slice, keys);
                        }
                        for win in windows {
                            // Notify at end of this window
                            notificator.notify_at(time.delayed(&(win + window_size)));
                            // println!("Asking notification for end of window: {:?}", win + window_size);
                            // NOTE: This is inefficient but...
                            let mut exists = false;
                            let records: Option<std::rc::Rc<Vec<(usize,usize)>>> = window_buckets.get(&(record.0,win));
                            if records.is_some()  {
                                let records = records.unwrap();
                                exists = records.iter().any(|(a,_): &(usize,_)| *a==record.0);
                            }
                            if !exists { // Add a first put for this keyed window
                                // println!("First PUT operation for window start: {:?}", (record.0,win));
                                window_buckets.insert((record.0,win), vec![]);  // Initialize window state
                            }
                            window_buckets.rmw((record.0, win), vec![*record]);
                            // println!("Appending record with timestamp {} and auction id {} to window {:?}.", record.1, record.0, (record.0, win));
                        }
                    }
                });

                notificator.for_each(|cap, _, _| {
                    // println!("Firing and cleaning window with start timestamp {}.", cap.time() - window_size);
                    let window_end = cap.time();
                    let window_start = window_end - (window_slide_ns * window_slice_count);
                    // println!("Start of window: {}", window_start);
                    // println!("End of window: {}", *window_end);

                    // Step 1: Get all distinct keys appearing in the expired window
                    let mut all_keys = HashSet::new();  
                    
                    let first_slice = window_start + 1_000_000_000;
                    let last_slice = window_end + 1_000_000_000;
                    for slice in (first_slice..last_slice).step_by(1_000_000_000) {
                        // println!("Slice to lookup: {}", slice);
                        if let Some(keys) = state_index.get(&slice) {
                            for key in keys.iter() {
                                all_keys.insert(*key);
                            }
                        }
                        else {
                            println!("Slice {} does not exist (experiment timeout).", slice);
                        }
                    }

                    for key in all_keys {
                        let composite_key = (key, window_start);
                        // println!("Composite key: {:?}", composite_key);
                        let mut records = window_buckets.remove(&composite_key).expect("Must exist");
                        // Apply the rank function to the window
                        records.sort_unstable_by(|a, b| a.0.cmp(&b.0)); // Sort auctions by id
                        let mut rank = 1;
                        let mut count = 0;
                        let mut current_record = records[0];
                        for record in records {
                            // output (timestamp, auctionID, rank)
                            let auction = record;
                            if auction.0 != current_record.0 {
                                // increase rank and update current
                                rank += count;
                                count = 0;
                                current_record = auction;
                            }
                            count += 1;
                            output.session(&cap).give((*cap.time(), auction.0, rank));
                            // println!("*** End of window: {:?}, Auction: {:?}, Rank: {:?}", cap.time(), auction.0, rank);
                        }
                    }

                    // Step 3: Clean up state index
                    let limit = window_start + window_slide_ns + 1;
                    for slice in (first_slice..limit).step_by(1_000_000_000) {
                        // println!("Slice to remove from index: {}", slice);
                        state_index.remove(&slice).expect("Slice must exist in index");
                    }
                    
                });
            },
        )
}
