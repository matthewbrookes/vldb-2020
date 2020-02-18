use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::{Scope, Stream};
use bincode;

use crate::queries::{NexmarkInput, NexmarkTimer};
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::operators::map::Map;


pub fn window_1_rocksdb_rank<S: Scope<Timestamp = usize>>(
    input: &NexmarkInput,
    _nt: NexmarkTimer,
    scope: &mut S,
    window_slice_count: usize,
    window_slide_ns: usize,
) -> Stream<S, (usize, usize, usize)> {
    
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
            Exchange::new(|b: &(usize,_)| b.0 as u64),
            "Accumulate records",
            None,
            move |input, output, notificator, state_handle| {
                let mut window_contents = state_handle.get_managed_map("window_contents");
                let prefix_key_len: usize = window_contents.as_ref().get_key_prefix_length();
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
                            // Add window margins so that we can iterate over its contents upon notification
                            // println!("Inserting dummy record:: time: {:?}, value:{:?}", sl - window_slide_ns, 0);
                            window_contents.insert((sl - window_slide_ns).to_be(), 0);  // Start timestamp of window
                            // TODO (john): Omit adding the end here and change loop condition below
                            // println!("Inserting dummy record:: time: {:?}, value:{:?}", window_end, 0);
                            window_contents.insert(window_end.to_be(), 0);  // End timestamp of window
                        }
                        last_slide_seen = current_slide;
                    }
                    // Add window contents
                    data.swap(&mut buffer);
                    for record in buffer.iter() {
                        let key = record.1; // Event time
                        let auction_id = record.0;
                        // Add record 
                        // println!("Inserting window record:: time: {}, value:{}", key, auction_id);
                        window_contents.insert(key.to_be(), auction_id);
                    }
                });

                notificator.for_each(|cap, _, _| {
                    let window_end = cap.time(); 
                    let window_start = window_end - (window_slide_ns * window_slice_count);  
                    let first_slide_end = window_start + window_slide_ns; // To know which records to delete
                    // println!("Start of window: {}", window_start);
                    // println!("End of window: {}", *window_end);
                    // println!("End of first slide: {}", first_slide_end);
                    let mut records = Vec::new();
                    let mut to_delete = Vec::new();  // Keep keys to delete here
                    to_delete.push(window_start);
                    {
                        let mut window_iter = window_contents.iter(window_start.to_be());
                        let _ = window_iter.next();  // Skip dummy record
                        for (ser_key, ser_value) in window_iter {
                            let k = &ser_key[prefix_key_len..];  // Ignore prefix
                            let mut timestamp: usize = bincode::deserialize(unsafe {
                                                        std::slice::from_raw_parts(k.as_ptr(), k.len())
                                                    }).expect("Cannot deserialize timestamp");
                            timestamp = usize::from_be(timestamp);
                            let auction_id: usize = bincode::deserialize(unsafe {
                                                        std::slice::from_raw_parts(ser_value.as_ptr(), ser_value.len())
                                                    }).expect("Cannot deserialize auction id");
                            // println!("Found record:: time: {}, value:{}", timestamp, auction_id);
                            if (timestamp % window_slide_ns) != 0 {  // Omit dummy records
                                records.push(auction_id);  // Add auction id to window contents
                            }
                            if timestamp == *window_end {  // Reached the end of the window
                                break;
                            }
                            assert!(timestamp < *window_end);
                            if timestamp < first_slide_end {
                                to_delete.push(timestamp);
                            }
                        }
                    }
                    // Apply the rank function to the window
                    records.sort_unstable(); // Sort auctions by id
                    let mut rank = 1;
                    let mut count = 0;
                    let mut current_record = records[0];
                    for record in records {
                        // output (timestamp, auctionID, rank)
                        let auction = record;
                        if auction != current_record {
                            // increase rank and update current
                            rank += count;
                            count = 0;
                            current_record = auction;
                        }
                        count += 1;
                        output.session(&cap).give((*cap.time(), auction, rank));
                        // println!("*** End of window: {:?}, Auction: {:?}, Rank: {:?}", cap.time(), auction, rank);
                    }
                    // Purge state of first slide in window
                    for ts in to_delete {
                        // println!("Removing record:: time: {}", ts);
                        window_contents.remove(&ts.to_be()).expect("Record to remove must exist");
                    }
                });
            },
        )
}
