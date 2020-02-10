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

pub fn keyed_window_2_faster_rank<S: Scope<Timestamp = usize>>(
    input: &NexmarkInput,
    _nt: NexmarkTimer,
    scope: &mut S,
    window_slice_count: usize,
    window_slide_ns: usize,
) -> Stream<S, (usize, usize, usize)> {
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
                let window_size = window_slice_count * window_slide_ns;
                // slice end timestamp -> distinct keys in slice
                let mut state_index = state_handle.get_managed_map("index");
                // window_start_timestamp -> window_contents
                let mut window_buckets = state_handle.get_managed_map("window_buckets");
                let mut buffer = Vec::new();
                input.for_each(|time, data| {
                    data.swap(&mut buffer);
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
                            window_buckets.rmw((record.0, win), vec![*record]);
                            // println!("Appending record with timestamp {} to window {:?}.", record.1, (record.0, win));
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
                        let records = window_buckets.remove(&composite_key).expect("Must exist");
                        let mut auctions = Vec::new();
                        for record in records.iter() {
                            auctions.push(record.0);
                        }
                        // println!("*** Window: {:?}, contents {:?}.", composite_key, records);
                        auctions.sort_unstable();
                        let mut rank = 1;
                        let mut count = 0;
                        let mut current_record = auctions[0];
                        for auction in &auctions {
                            // output (timestamp, auctionID, rank)
                            if *auction != current_record {
                                // increase rank and update current
                                rank += count;
                                count = 0;
                                current_record = *auction;
                            }
                            count+=1;
                            output.session(&cap).give((*cap.time(), *auction, rank));
                            // println!("*** Start of window: {:?}, Auction: {:?}, Rank: {:?}", window_start, auction, rank);
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
