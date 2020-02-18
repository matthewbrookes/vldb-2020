use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::{Scope, Stream};

use crate::queries::{NexmarkInput, NexmarkTimer};
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::map::Map;

pub fn window_3_faster_count<S: Scope<Timestamp = usize>>(
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
                            // println!("Asking notification for the end of window: {:?}", window_end);
                            notificator.notify_at(time.delayed(&window_end));
                        }
                        last_slide_seen = current_slide;
                    }
                    data.swap(&mut buffer);
                    for record in buffer.iter() {
                        let pane = ((record.1 / window_slide_ns) + 1) * window_slide_ns;  // Pane size equals slide size as window is a multiple of slide
                        // println!("Inserting record with time {:?} in pane {:?}", record.1, pane);
                        pane_buckets.rmw(pane, 1);
                    }
                });

                notificator.for_each(|cap, _, _| {
                    // println!("Received notification for end of window {:?}", &(cap.time()));
                    let mut count = 0;
                    //lookup all panes in the window
                    for i in 0..window_slice_count {
                        let pane = cap.time() - window_slide_ns * i;
                        // println!("Lookup pane {:?}", &pane);
                        if let Some(record) = pane_buckets.get(&pane) {
                                count+=*record.as_ref();
                        } else {
                            println!("Processing pane {} of last window.", cap.time() - window_slide_ns * i);
                        }
                        // remove the first slide of the fired window
                        if i == window_slice_count - 1 {
                            // println!("Removing pane {:?}", pane);
                            let _ = pane_buckets.remove(&pane).expect("Pane to remove must exist");
                        }
                    }
                    // println!("*** End of window: {:?}, Count: {:?}", cap.time(), count);
                    output.session(&cap).give((*cap.time(), count));
                });
            }
        )
}
