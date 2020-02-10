use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::{Scope, Stream};

use crate::queries::{NexmarkInput, NexmarkTimer};
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::map::Map;

use std::collections::HashSet;

pub fn keyed_window_3_faster_count<S: Scope<Timestamp = usize>>(
    input: &NexmarkInput,
    _nt: NexmarkTimer,
    scope: &mut S,
    window_slice_count: usize,
    window_slide_ns: usize,
) -> Stream<S, (usize, usize)> {

    let mut last_slide_seen = 0;
    let mut to_fire = HashSet::new();

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
                // pane end timestamp -> pane contents
                let mut pane_buckets = state_handle.get_managed_map("pane_buckets");
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
                            if window_end < current_slide { 
                                // Computation is way behind. Keep track of the pending windows to fire
                                to_fire.insert(window_end);
                            }
                            else{ 
                                // println!("Asking notification for the end of window: {:?}", window_end);
                                notificator.notify_at(time.delayed(&window_end));    
                            }
                        }
                        last_slide_seen = current_slide;
                    }
                    // Update state with new records
                    data.swap(&mut buffer);
                    for record in buffer.iter() {
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
                        // Add record to pane
                        let pane = ((record.1 / window_slide_ns) + 1) * window_slide_ns;  // Pane size equals slide size as window is a multiple of slide
                        // println!("Inserting record {:?} in pane {:?}", (record.0, record.1), pane);
                        pane_buckets.rmw((record.0, pane), 1);
                    }
                });

                notificator.for_each(|cap, _, _| {
                    // println!("Received notification for end of window {:?}", cap.time());
                    let mut windows_to_fire = Vec::new();
                    windows_to_fire.push(*cap.time());
                    if to_fire.len() > 0 {
                        windows_to_fire = to_fire.clone().into_iter().collect();
                        windows_to_fire.sort();
                        to_fire.clear();
                    }
                    for window_end in windows_to_fire {
                        let window_start = window_end - (window_slide_ns * window_slice_count);
                        // println!("Start of window: {}", window_start);
                        // println!("End of window: {}", window_end);

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
                        
                        // Step 2: Output result for each keyed window and clean up
                        for key in all_keys {
                            let mut count = 0;
                            //lookup all panes in the window
                            for i in 0..window_slice_count {
                                let pane = cap.time() - window_slide_ns * i;
                                let composite_key = (key, pane);
                                // println!("Lookup keyed pane {:?}", composite_key);
                                if let Some(record) = pane_buckets.get(&composite_key) {
                                        count+=*record.as_ref();
                                }
                                // Remove the first slide of the fired window
                                if i == window_slice_count - 1 {
                                    // println!("Removing keyed pane {:?}", composite_key);
                                    pane_buckets.remove(&composite_key); //.expect("Pane to remove must exist");
                                }
                            }
                            // println!("*** End of window: {:?}, Key {} Count: {:?}", cap.time(), key, count);
                            output.session(&cap).give((key, count));
                        }

                        // Step 3: Clean up state
                        let limit = window_start + window_slide_ns;
                        for slice in (first_slice..limit+1).step_by(1_000_000_000) {
                            // println!("Slice to remove from index: {}", slice);
                            state_index.remove(&slice).expect("Slice must exist in index");
                        }
                    }
                });
            }
        )
}
