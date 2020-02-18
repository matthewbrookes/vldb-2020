use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::{Scope, Stream};

use crate::queries::{NexmarkInput, NexmarkTimer};
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::map::Map;

pub fn window_1_faster_count_custom_slice<S: Scope<Timestamp = usize>>(
    input: &NexmarkInput,
    _nt: NexmarkTimer,
    scope: &mut S,
    window_slice_count: usize,
    window_slide_ns: usize,
) -> Stream<S, (usize, usize)> {

    let mut last_slide_seen = 0;
    let num_slices = window_slide_ns / 1_000_000_000;

    input
        .bids(scope)
        .map(move |b| {
            (
                b.auction,
                *b.date_time,
            )
        })
        .unary_notify(
            Exchange::new(|b: &(usize, _)| b.0 as u64),
            "Accumulate records",
            None,
            move |input, output, notificator, state_handle| {
                // Slide end timestamp -> event timestamps
                let mut slide_index = state_handle.get_managed_map("slide_index");
                // Event timestamp -> auction id
                let mut window_contents = state_handle.get_managed_map("window_contents");
                let mut buffer = Vec::new();
                input.for_each(|time, data| {
                    // The end timestamp of the slide the current epoch corresponds to
                    let current_slide = ((time.time() / window_slide_ns) + 1) * window_slide_ns;
                    // println!("Current slide: {:?}", current_slide);
                    assert!(last_slide_seen <= current_slide);
                    // Ask notifications for all remaining slides up to the current one
                    if last_slide_seen < current_slide {
                        let start = last_slide_seen + window_slide_ns;
                        let end = current_slide + window_slide_ns;
                        for sl in (start..end).step_by(window_slide_ns) {
                            // println!("Asking notification for the end of window: {:?}", sl + window_slide_ns * (window_slice_count - 1));
                            notificator.notify_at(time.delayed(&(sl + window_slide_ns * (window_slice_count - 1))));
                        }
                        last_slide_seen = current_slide;
                    }
                    data.swap(&mut buffer);
                    for record in buffer.iter() {
                        window_contents.insert(record.1, record.0);
                        let record_slice = ((record.1 / 1_000_000_000) + 1) * 1_000_000_000;  // Use slices of 1s each to index state
                        // println!("Inserting timestamp in the index: slide: {:?}, timestamp: {:?}", record_slice, record.1);
                        slide_index.rmw(record_slice, vec![record.1]);
                    }
                });

                notificator.for_each(|cap, _, _| {
                    // println!("End of window: {:?}", cap.time());
                    let mut count = 0;
                    let limit = window_slice_count * num_slices;
                    for i in 0..limit {
                        // println!("Lookup slice {:?}", &(cap.time() - 1_000_000_000 * i));
                        if let Some(keys) = slide_index.get(&(cap.time() - 1_000_000_000 * i)) {
                            for timestamp in keys.as_ref() {
                                // println!("Lookup timestamp {:?}", timestamp);
                                let _ = window_contents.get(timestamp).expect("Timestamp must exist");
                                count += 1;
                            }
                        }
                        else {
                            println!("Processing slide {} of last window.", cap.time() - 1_000_000_000 * i);
                        }
                    }
                    // println!("*** End of window: {:?}, Count: {:?}", cap.time(), count);
                    output.session(&cap).give((*cap.time(), count));
                    let slide = cap.time() - (window_slice_count - 1) * window_slide_ns;
                    // println!("Removing slide {:?}", slide);
                    // Remove all slices of the first slide
                    for i in 0..num_slices {
                        // println!("Removing slice {:?}", slide - i * 1_000_000_000);
                        if let Some(keys_to_remove) = slide_index.remove(&(slide - i * 1_000_000_000)) {
                            for timestamp in keys_to_remove {
                                let _ = window_contents.remove(&timestamp).expect("Timestamp to remove must exist");
                            }
                        }
                        else {
                            println!("Tried to remove slide {} of last window, which doesn't exist.", slide - i * 1_000_000_000);
                        }
                    }
                });
            },
        )
}
