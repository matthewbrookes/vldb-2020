extern crate timely;

use timely::Configuration;
use timely::dataflow::channels::pact::Pipeline;
use timely::dataflow::operators::{Feedback, ConnectLoop};
use timely::dataflow::operators::generic::operator::Operator;
use timely::state::backends::InMemoryBackend;

#[test] fn barrier_sync_1w() { barrier_sync_helper(Configuration::Thread); }
#[test] fn barrier_sync_2w() { barrier_sync_helper(Configuration::Process(2)); }
#[test] fn barrier_sync_3w() { barrier_sync_helper(Configuration::Process(3)); }

// This method asserts that each round of execution is notified of at most one time.
fn barrier_sync_helper(config: ::timely::Configuration) {
    timely::execute(config, move |worker| {
        worker.dataflow::<_,_,_,InMemoryBackend>(move |scope| {
            let (handle, stream) = scope.feedback::<u64>(1);
            stream.unary_notify(
                Pipeline,
                "Barrier",
                vec![0, 1],
                move |_, _, notificator, _| {
                    let mut count = 0;
                    while let Some((cap, _count)) = notificator.next() {
                        count += 1;
                        let time = *cap.time() + 1;
                        if time < 100 {
                            notificator.notify_at(cap.delayed(&time));
                        }
                    }
                    assert!(count <= 1);
                }
            )
            .connect_loop(handle);
        });
    }).unwrap(); // asserts error-free execution;
}
