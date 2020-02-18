use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::{Scope, Stream};

use crate::queries::{NexmarkInput, NexmarkTimer};
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::map::Map;

pub fn window_3b_rocksdb<S: Scope<Timestamp = usize>>(
    input: &NexmarkInput,
    _nt: NexmarkTimer,
    scope: &mut S,
    window_slice_count: usize,
    window_slide_ns: usize,
) -> Stream<S, (usize, usize)> {

    let mut last_slide_seen = 0;
    let mut max_window_seen = 0;

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
                // pane end timestamp -> pane contents
                let mut pane_buckets = state_handle.get_managed_map("pane_buckets");
                let prefix_key_len: usize = pane_buckets.as_ref().get_key_prefix_length();
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
                    // Add window start so that we can iterate over its contents upon notification
                    if max_window_seen <= current_slide {
                        let end = current_slide + window_slide_ns;
                        for window_start in (max_window_seen..end).step_by(window_slide_ns) {
                            // println!("First PUT operation for window start: {:?}", window_start);
                            pane_buckets.insert(window_start.to_be(), vec![]);  // Initialize window state
                        }
                        max_window_seen = end;
                    }
                    data.swap(&mut buffer);
                    for record in buffer.iter() {
                        let pane = ((record.1 / window_slide_ns) + 1) * window_slide_ns;  // Pane size equals slide size as window is a multiple of slide
                        //println!("Inserting record with time {:?} in pane {:?}", record.1, pane);
                        pane_buckets.rmw(pane.to_be(), vec![*record]);
                    }
                });

                notificator.for_each(|cap, _, _| {
                    let window_end = cap.time(); 
                    let window_start = window_end - (window_slide_ns * window_slice_count);  
                    let first_pane_end = window_start + window_slide_ns; // To know which records to delete
                    // println!("Start of window: {}", window_start);
                    // println!("End of window: {}", *window_end);
                    // println!("End of first slide: {}", first_pane_end);
                    {// Iterate over the panes belonging to the current window
                        let mut window_iter = pane_buckets.iter(window_start.to_be());
                        let _ = window_iter.next();  // Skip first pane
                        for (ser_key, ser_value) in window_iter {
                            let k = &ser_key[prefix_key_len..];  // Ignore prefix
                            let mut timestamp: usize = bincode::deserialize(unsafe {
                                                        std::slice::from_raw_parts(k.as_ptr(), k.len())
                                                    }).expect("Cannot deserialize timestamp");
                            timestamp = usize::from_be(timestamp);  // The end timestamp of the pane
                            let records: Vec<(usize, usize)> = bincode::deserialize(unsafe {
                                                                std::slice::from_raw_parts(ser_value.as_ptr(), ser_value.len())
                                                            }).expect("Cannot deserialize auction id");
                            // println!("***Found pane:: time: {}, records:{:?}", timestamp, records);
                            if timestamp > *window_end {  // Outside window
                                break;
                            }
                            // println!("Output records for pane with end timestamp {}: {:?}", timestamp, records);
                            for record in records {
                                output.session(&cap).give(record.clone());
                            }
                        }
                    }
                    // Purge state of first slide/pane in window
                    // println!("Removing pane with end timestamp time: {}", ts);
                    pane_buckets.remove(&first_pane_end.to_be()).expect("Pane to remove must exist");
                });
            }
        )
}
