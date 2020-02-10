
//! Methods to construct generic streaming and blocking unary operators.

use crate::dataflow::channels::pushers::Tee;
use crate::dataflow::channels::pact::ParallelizationContract;

use crate::dataflow::operators::generic::handles::{InputHandle, FrontieredInputHandle, OutputHandle};
use crate::dataflow::operators::capability::Capability;

use crate::Data;

use crate::dataflow::{Stream, Scope};

use super::builder_rc::OperatorBuilder;
use crate::dataflow::operators::generic::OperatorInfo;
use crate::dataflow::operators::generic::notificator::{Notificator, FrontierNotificator};
use crate::state::{StateBackend, StateHandle};
use std::rc::Rc;

/// Methods to construct generic streaming and blocking operators.
pub trait Operator<G: Scope, D1: Data> {
    /// Creates a new dataflow operator that partitions its input stream by a parallelization
    /// strategy `pact`, and repeatedly invokes `logic`, the function returned by the function passed as `constructor`.
    /// `logic` can read from the input stream, write to the output stream, and inspect the frontier at the input.
    ///
    /// # Examples
    /// ```
    /// use std::collections::HashMap;
    /// use timely::dataflow::operators::{ToStream, FrontierNotificator};
    /// use timely::dataflow::operators::generic::Operator;
    /// use timely::dataflow::channels::pact::Pipeline;
    ///
    /// fn main() {
    ///     timely::example(|scope| {
    ///         (0u64..10).to_stream(scope)
    ///             .unary_frontier(Pipeline, "example", |default_cap, _info, _state_handle| {
    ///                 let mut cap = Some(default_cap.delayed(&12));
    ///                 let mut notificator = FrontierNotificator::new();
    ///                 let mut stash = HashMap::new();
    ///                 let mut vector = Vec::new();
    ///                 move |input, output| {
    ///                     if let Some(ref c) = cap.take() {
    ///                         output.session(&c).give(12);
    ///                     }
    ///                     while let Some((time, data)) = input.next() {
    ///                         data.swap(&mut vector);
    ///                         stash.entry(time.time().clone())
    ///                              .or_insert(Vec::new())
    ///                              .extend(vector.drain(..));
    ///                     }
    ///                     notificator.for_each(&[input.frontier()], |time, _not| {
    ///                         if let Some(mut vec) = stash.remove(time.time()) {
    ///                             output.session(&time).give_iterator(vec.drain(..));
    ///                         }
    ///                     });
    ///                 }
    ///             });
    ///     });
    /// }
    /// ```
    fn unary_frontier<D2, B, L, P>(&self, pact: P, name: &str, constructor: B) -> Stream<G, D2>
    where
        D2: Data,
        B: FnOnce(Capability<G::Timestamp>, OperatorInfo, StateHandle<G::StateBackend>) -> L,
        L: FnMut(&mut FrontieredInputHandle<G::Timestamp, D1, P::Puller>,
            &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>)+'static,
        P: ParallelizationContract<G::Timestamp, D1>;

    /// Creates a new dataflow operator that partitions its input stream by a parallelization
    /// strategy `pact`, and repeatedly invokes `logic`, the function returned by the function passed as `constructor`.
    /// `logic` can read from the input stream, write to the output stream, and inspect the frontier at the input.
    ///
    /// This variant allows specifying the state backend that will be used by `state_handle`.
    /// # Examples
    /// ```
    /// use std::collections::HashMap;
    /// use timely::dataflow::operators::{ToStream, FrontierNotificator};
    /// use timely::dataflow::operators::generic::Operator;
    /// use timely::dataflow::channels::pact::Pipeline;
    /// use timely::state::StateHandle;
    /// use timely::state::backends::InMemoryBackend;
    ///
    /// fn main() {
    ///     timely::example(|scope| {
    ///         (0u64..10).to_stream(scope)
    ///             .unary_frontier_core(Pipeline, "example", |default_cap, _info, _state_handle: StateHandle<InMemoryBackend>| {
    ///                 let mut cap = Some(default_cap.delayed(&12));
    ///                 let mut notificator = FrontierNotificator::new();
    ///                 let mut stash = HashMap::new();
    ///                 let mut vector = Vec::new();
    ///                 move |input, output| {
    ///                     if let Some(ref c) = cap.take() {
    ///                         output.session(&c).give(12);
    ///                     }
    ///                     while let Some((time, data)) = input.next() {
    ///                         data.swap(&mut vector);
    ///                         stash.entry(time.time().clone())
    ///                              .or_insert(Vec::new())
    ///                              .extend(vector.drain(..));
    ///                     }
    ///                     notificator.for_each(&[input.frontier()], |time, _not| {
    ///                         if let Some(mut vec) = stash.remove(time.time()) {
    ///                             output.session(&time).give_iterator(vec.drain(..));
    ///                         }
    ///                     });
    ///                 }
    ///             });
    ///     });
    /// }
    /// ```
    fn unary_frontier_core<D2, B, L, P, S>(&self, pact: P, name: &str, constructor: B) -> Stream<G, D2>
    where
        D2: Data,
        B: FnOnce(Capability<G::Timestamp>, OperatorInfo, StateHandle<S>) -> L,
        L: FnMut(&mut FrontieredInputHandle<G::Timestamp, D1, P::Puller>,
                 &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>)+'static,
        P: ParallelizationContract<G::Timestamp, D1>,
        S: StateBackend;

    /// Creates a new dataflow operator that partitions its input stream by a parallelization
    /// strategy `pact`, and repeatedly invokes `logic`, the function returned by the function passed as `constructor`.
    /// `logic` can read from the input stream, write to the output stream, and inspect the frontier at the input.
    ///
    /// # Examples
    /// ```
    /// use std::collections::HashMap;
    /// use timely::dataflow::operators::{ToStream, FrontierNotificator};
    /// use timely::dataflow::operators::generic::Operator;
    /// use timely::dataflow::channels::pact::Pipeline;
    ///
    /// fn main() {
    ///     timely::example(|scope| {
    ///         let mut vector = Vec::new();
    ///         (0u64..10)
    ///             .to_stream(scope)
    ///             .unary_notify(Pipeline, "example", None, move |input, output, notificator, _state_handle| {
    ///                 input.for_each(|time, data| {
    ///                     data.swap(&mut vector);
    ///                     output.session(&time).give_vec(&mut vector);
    ///                     notificator.notify_at(time.retain());
    ///                 });
    ///                 notificator.for_each(|time, _cnt, _not| {
    ///                     println!("notified at {:?}", time);
    ///                 });
    ///             });
    ///     });
    /// }
    /// ```
    fn unary_notify<D2: Data,
            L: FnMut(&mut InputHandle<G::Timestamp, D1, P::Puller>,
                     &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>,
                     &mut Notificator<G::Timestamp>,
                     &StateHandle<G::StateBackend>)+'static,
            P: ParallelizationContract<G::Timestamp, D1>>
             (&self, pact: P, name: &str, init: impl IntoIterator<Item=G::Timestamp>, logic: L) -> Stream<G, D2>;

    /// Creates a new dataflow operator that partitions its input stream by a parallelization
    /// strategy `pact`, and repeatedly invokes `logic`, the function returned by the function passed as `constructor`.
    /// `logic` can read from the input stream, write to the output stream, and inspect the frontier at the input.
    ///
    /// This variant allows specifying the state backend that will be used by `state_handle`.
    /// # Examples
    /// ```
    /// use std::collections::HashMap;
    /// use timely::dataflow::operators::{ToStream, FrontierNotificator, Notificator};
    /// use timely::dataflow::operators::generic::{Operator, InputHandle, OutputHandle};
    /// use timely::dataflow::channels::pact::Pipeline;
    /// use timely::state::StateHandle;
    /// use timely::state::backends::InMemoryBackend;
    ///
    /// fn main() {
    ///     timely::example(|scope| {
    ///         let mut vector = Vec::new();
    ///         (0u64..10)
    ///             .to_stream(scope)
    ///             .unary_notify_core(Pipeline, "example", None, move |input, output, notificator, _state_handle: &StateHandle<InMemoryBackend>| {
    ///                 input.for_each(|time, data| {
    ///                     data.swap(&mut vector);
    ///                     output.session(&time).give_vec(&mut vector);
    ///                     notificator.notify_at(time.retain());
    ///                 });
    ///                 notificator.for_each(|time, _cnt, _not| {
    ///                     println!("notified at {:?}", time);
    ///                 });
    ///             });
    ///     });
    /// }
    /// ```
    fn unary_notify_core<D2: Data,
        L: FnMut(&mut InputHandle<G::Timestamp, D1, P::Puller>,
            &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>,
            &mut Notificator<G::Timestamp>,
            &StateHandle<S>)+'static,
        P: ParallelizationContract<G::Timestamp, D1>,
        S: StateBackend>
    (&self, pact: P, name: &str, init: impl IntoIterator<Item=G::Timestamp>, logic: L) -> Stream<G, D2>;

    /// Creates a new dataflow operator that partitions its input stream by a parallelization
    /// strategy `pact`, and repeatedly invokes `logic`, the function returned by the function passed as `constructor`.
    /// `logic` can read from the input stream, and write to the output stream.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, FrontierNotificator};
    /// use timely::dataflow::operators::generic::operator::Operator;
    /// use timely::dataflow::channels::pact::Pipeline;
    /// use timely::dataflow::Scope;
    ///
    /// timely::example(|scope| {
    ///     (0u64..10).to_stream(scope)
    ///         .unary(Pipeline, "example", |default_cap, _info, _state_handle| {
    ///             let mut cap = Some(default_cap.delayed(&12));
    ///             let mut vector = Vec::new();
    ///             move |input, output| {
    ///                 if let Some(ref c) = cap.take() {
    ///                     output.session(&c).give(100);
    ///                 }
    ///                 while let Some((time, data)) = input.next() {
    ///                     data.swap(&mut vector);
    ///                     output.session(&time).give_vec(&mut vector);
    ///                 }
    ///             }
    ///         });
    /// });
    /// ```
    fn unary<D2, B, L, P>(&self, pact: P, name: &str, constructor: B) -> Stream<G, D2>
    where
        D2: Data,
        B: FnOnce(Capability<G::Timestamp>, OperatorInfo, StateHandle<G::StateBackend>) -> L,
        L: FnMut(&mut InputHandle<G::Timestamp, D1, P::Puller>,
                 &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>)+'static,
        P: ParallelizationContract<G::Timestamp, D1>;

    /// Creates a new dataflow operator that partitions its input stream by a parallelization
    /// strategy `pact`, and repeatedly invokes `logic`, the function returned by the function passed as `constructor`.
    /// `logic` can read from the input stream, and write to the output stream.
    ///
    /// This variant allows specifying the state backend that will be used by `state_handle`.
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, FrontierNotificator};
    /// use timely::dataflow::operators::generic::operator::Operator;
    /// use timely::dataflow::channels::pact::Pipeline;
    /// use timely::dataflow::Scope;
    /// use timely::state::StateHandle;
    /// use timely::state::backends::InMemoryBackend;
    ///
    /// timely::example(|scope| {
    ///     (0u64..10).to_stream(scope)
    ///         .unary(Pipeline, "example", |default_cap, _info, _state_handle: StateHandle<InMemoryBackend>| {
    ///             let mut cap = Some(default_cap.delayed(&12));
    ///             let mut vector = Vec::new();
    ///             move |input, output| {
    ///                 if let Some(ref c) = cap.take() {
    ///                     output.session(&c).give(100);
    ///                 }
    ///                 while let Some((time, data)) = input.next() {
    ///                     data.swap(&mut vector);
    ///                     output.session(&time).give_vec(&mut vector);
    ///                 }
    ///             }
    ///         });
    /// });
    /// ```
    fn unary_core<D2, B, L, P, S>(&self, pact: P, name: &str, constructor: B) -> Stream<G, D2>
    where
        D2: Data,
        B: FnOnce(Capability<G::Timestamp>, OperatorInfo, StateHandle<S>) -> L,
        L: FnMut(&mut InputHandle<G::Timestamp, D1, P::Puller>,
            &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>)+'static,
        P: ParallelizationContract<G::Timestamp, D1>,
        S: StateBackend;

    /// Creates a new dataflow operator that partitions its input streams by a parallelization
    /// strategy `pact`, and repeatedly invokes `logic`, the function returned by the function passed as `constructor`.
    /// `logic` can read from the input streams, write to the output stream, and inspect the frontier at the inputs.
    ///
    /// # Examples
    /// ```
    /// use std::collections::HashMap;
    /// use timely::dataflow::operators::{Input, Inspect, FrontierNotificator};
    /// use timely::dataflow::operators::generic::operator::Operator;
    /// use timely::dataflow::channels::pact::Pipeline;
    /// use timely::state::backends::InMemoryBackend;
    ///
    /// timely::execute(timely::Configuration::Thread, |worker| {
    ///    let (mut in1, mut in2) = worker.dataflow::<usize,_,_,InMemoryBackend>(|scope| {
    ///        let (in1_handle, in1) = scope.new_input();
    ///        let (in2_handle, in2) = scope.new_input();
    ///        in1.binary_frontier(&in2, Pipeline, Pipeline, "example", |mut _default_cap, _info, _state_handle| {
    ///            let mut notificator = FrontierNotificator::new();
    ///            let mut stash = HashMap::new();
    ///            let mut vector1 = Vec::new();
    ///            let mut vector2 = Vec::new();
    ///            move |input1, input2, output| {
    ///                while let Some((time, data)) = input1.next() {
    ///                    data.swap(&mut vector1);
    ///                    stash.entry(time.time().clone()).or_insert(Vec::new()).extend(vector1.drain(..));
    ///                    notificator.notify_at(time.retain());
    ///                }
    ///                while let Some((time, data)) = input2.next() {
    ///                    data.swap(&mut vector2);
    ///                    stash.entry(time.time().clone()).or_insert(Vec::new()).extend(vector2.drain(..));
    ///                    notificator.notify_at(time.retain());
    ///                }
    ///                notificator.for_each(&[input1.frontier(), input2.frontier()], |time, _not| {
    ///                    if let Some(mut vec) = stash.remove(time.time()) {
    ///                        output.session(&time).give_iterator(vec.drain(..));
    ///                    }
    ///                });
    ///            }
    ///        }).inspect_batch(|t, x| println!("{:?} -> {:?}", t, x));
    ///
    ///        (in1_handle, in2_handle)
    ///    });
    ///
    ///    for i in 1..10 {
    ///        in1.send(i - 1);
    ///        in1.advance_to(i);
    ///        in2.send(i - 1);
    ///        in2.advance_to(i);
    ///    }
    /// }).unwrap();
    /// ```
    fn binary_frontier<D2, D3, B, L, P1, P2>(&self, other: &Stream<G, D2>, pact1: P1, pact2: P2, name: &str, constructor: B) -> Stream<G, D3>
    where
        D2: Data,
        D3: Data,
        B: FnOnce(Capability<G::Timestamp>, OperatorInfo, StateHandle<G::StateBackend>) -> L,
        L: FnMut(&mut FrontieredInputHandle<G::Timestamp, D1, P1::Puller>,
                 &mut FrontieredInputHandle<G::Timestamp, D2, P2::Puller>,
                 &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>)+'static,
        P1: ParallelizationContract<G::Timestamp, D1>,
        P2: ParallelizationContract<G::Timestamp, D2>;

    /// Creates a new dataflow operator that partitions its input streams by a parallelization
    /// strategy `pact`, and repeatedly invokes `logic`, the function returned by the function passed as `constructor`.
    /// `logic` can read from the input streams, write to the output stream, and inspect the frontier at the inputs.
    ///
    /// This variant allows specifying the state backend that will be used by `state_handle`.
    /// # Examples
    /// ```
    /// use std::collections::HashMap;
    /// use timely::dataflow::operators::{Input, Inspect, FrontierNotificator};
    /// use timely::dataflow::operators::generic::operator::Operator;
    /// use timely::dataflow::channels::pact::Pipeline;
    /// use timely::state::StateHandle;
    /// use timely::state::backends::InMemoryBackend;
    ///
    /// timely::execute(timely::Configuration::Thread, |worker| {
    ///    let (mut in1, mut in2) = worker.dataflow::<usize,_,_,InMemoryBackend>(|scope| {
    ///        let (in1_handle, in1) = scope.new_input();
    ///        let (in2_handle, in2) = scope.new_input();
    ///        in1.binary_frontier_core(&in2, Pipeline, Pipeline, "example", |mut _default_cap, _info, _state_handle: StateHandle<InMemoryBackend>| {
    ///            let mut notificator = FrontierNotificator::new();
    ///            let mut stash = HashMap::new();
    ///            let mut vector1 = Vec::new();
    ///            let mut vector2 = Vec::new();
    ///            move |input1, input2, output| {
    ///                while let Some((time, data)) = input1.next() {
    ///                    data.swap(&mut vector1);
    ///                    stash.entry(time.time().clone()).or_insert(Vec::new()).extend(vector1.drain(..));
    ///                    notificator.notify_at(time.retain());
    ///                }
    ///                while let Some((time, data)) = input2.next() {
    ///                    data.swap(&mut vector2);
    ///                    stash.entry(time.time().clone()).or_insert(Vec::new()).extend(vector2.drain(..));
    ///                    notificator.notify_at(time.retain());
    ///                }
    ///                notificator.for_each(&[input1.frontier(), input2.frontier()], |time, _not| {
    ///                    if let Some(mut vec) = stash.remove(time.time()) {
    ///                        output.session(&time).give_iterator(vec.drain(..));
    ///                    }
    ///                });
    ///            }
    ///        }).inspect_batch(|t, x| println!("{:?} -> {:?}", t, x));
    ///
    ///        (in1_handle, in2_handle)
    ///    });
    ///
    ///    for i in 1..10 {
    ///        in1.send(i - 1);
    ///        in1.advance_to(i);
    ///        in2.send(i - 1);
    ///        in2.advance_to(i);
    ///    }
    /// }).unwrap();
    /// ```
    fn binary_frontier_core<D2, D3, B, L, P1, P2, S>(&self, other: &Stream<G, D2>, pact1: P1, pact2: P2, name: &str, constructor: B) -> Stream<G, D3>
    where
        D2: Data,
        D3: Data,
        B: FnOnce(Capability<G::Timestamp>, OperatorInfo, StateHandle<S>) -> L,
        L: FnMut(&mut FrontieredInputHandle<G::Timestamp, D1, P1::Puller>,
            &mut FrontieredInputHandle<G::Timestamp, D2, P2::Puller>,
            &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>)+'static,
        P1: ParallelizationContract<G::Timestamp, D1>,
        P2: ParallelizationContract<G::Timestamp, D2>,
        S: StateBackend;

    /// Creates a new dataflow operator that partitions its input streams by a parallelization
    /// strategy `pact`, and repeatedly invokes `logic`, the function returned by the function passed as `constructor`.
    /// `logic` can read from the input streams, write to the output stream, and inspect the frontier at the inputs.
    ///
    /// # Examples
    /// ```
    /// use std::collections::HashMap;
    /// use timely::dataflow::operators::{Input, Inspect, FrontierNotificator};
    /// use timely::dataflow::operators::generic::operator::Operator;
    /// use timely::dataflow::channels::pact::Pipeline;
    /// use timely::state::backends::InMemoryBackend;
    ///
    /// timely::execute(timely::Configuration::Thread, |worker| {
    ///    let (mut in1, mut in2) = worker.dataflow::<usize,_,_,InMemoryBackend>(|scope| {
    ///        let (in1_handle, in1) = scope.new_input();
    ///        let (in2_handle, in2) = scope.new_input();
    ///
    ///        let mut vector1 = Vec::new();
    ///        let mut vector2 = Vec::new();
    ///        in1.binary_notify(&in2, Pipeline, Pipeline, "example", None, move |input1, input2, output, notificator, _state_handle| {
    ///            input1.for_each(|time, data| {
    ///                data.swap(&mut vector1);
    ///                output.session(&time).give_vec(&mut vector1);
    ///                notificator.notify_at(time.retain());
    ///            });
    ///            input2.for_each(|time, data| {
    ///                data.swap(&mut vector2);
    ///                output.session(&time).give_vec(&mut vector2);
    ///                notificator.notify_at(time.retain());
    ///            });
    ///            notificator.for_each(|time, _cnt, _not| {
    ///                println!("notified at {:?}", time);
    ///            });
    ///        });
    ///
    ///        (in1_handle, in2_handle)
    ///    });
    ///
    ///    for i in 1..10 {
    ///        in1.send(i - 1);
    ///        in1.advance_to(i);
    ///        in2.send(i - 1);
    ///        in2.advance_to(i);
    ///    }
    /// }).unwrap();
    /// ```
    fn binary_notify<D2: Data,
              D3: Data,
              L: FnMut(&mut InputHandle<G::Timestamp, D1, P1::Puller>,
                       &mut InputHandle<G::Timestamp, D2, P2::Puller>,
                       &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>,
                       &mut Notificator<G::Timestamp>,
                       &StateHandle<G::StateBackend>)+'static,
              P1: ParallelizationContract<G::Timestamp, D1>,
              P2: ParallelizationContract<G::Timestamp, D2>>
            (&self, other: &Stream<G, D2>, pact1: P1, pact2: P2, name: &str, init: impl IntoIterator<Item=G::Timestamp>, logic: L) -> Stream<G, D3>;

    /// Creates a new dataflow operator that partitions its input streams by a parallelization
    /// strategy `pact`, and repeatedly invokes `logic`, the function returned by the function passed as `constructor`.
    /// `logic` can read from the input streams, write to the output stream, and inspect the frontier at the inputs.
    ///
    /// This variant allows specifying the state backend that will be used by `state_handle`.
    /// # Examples
    /// ```
    /// use std::collections::HashMap;
    /// use timely::dataflow::operators::{Input, Inspect, FrontierNotificator};
    /// use timely::dataflow::operators::generic::operator::Operator;
    /// use timely::dataflow::channels::pact::Pipeline;
    /// use timely::state::StateHandle;
    /// use timely::state::backends::InMemoryBackend;
    /// use timely::dataflow::operators::generic::{InputHandle, OutputHandle};
    ///
    /// timely::execute(timely::Configuration::Thread, |worker| {
    ///    let (mut in1, mut in2) = worker.dataflow::<usize,_,_,InMemoryBackend>(|scope| {
    ///        let (in1_handle, in1) = scope.new_input();
    ///        let (in2_handle, in2) = scope.new_input();
    ///
    ///        let mut vector1 = Vec::new();
    ///        let mut vector2 = Vec::new();
    ///        in1.binary_notify_core(&in2, Pipeline, Pipeline, "example", None, move |input1, input2, output, notificator, _state_handle: &StateHandle<InMemoryBackend>| {
    ///            input1.for_each(|time, data| {
    ///                data.swap(&mut vector1);
    ///                output.session(&time).give_vec(&mut vector1);
    ///                notificator.notify_at(time.retain());
    ///            });
    ///            input2.for_each(|time, data| {
    ///                data.swap(&mut vector2);
    ///                output.session(&time).give_vec(&mut vector2);
    ///                notificator.notify_at(time.retain());
    ///            });
    ///            notificator.for_each(|time, _cnt, _not| {
    ///                println!("notified at {:?}", time);
    ///            });
    ///        });
    ///
    ///        (in1_handle, in2_handle)
    ///    });
    ///
    ///    for i in 1..10 {
    ///        in1.send(i - 1);
    ///        in1.advance_to(i);
    ///        in2.send(i - 1);
    ///        in2.advance_to(i);
    ///    }
    /// }).unwrap();
    /// ```
    fn binary_notify_core<D2: Data,
        D3: Data,
        L: FnMut(&mut InputHandle<G::Timestamp, D1, P1::Puller>,
            &mut InputHandle<G::Timestamp, D2, P2::Puller>,
            &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>,
            &mut Notificator<G::Timestamp>,
            &StateHandle<S>)+'static,
        P1: ParallelizationContract<G::Timestamp, D1>,
        P2: ParallelizationContract<G::Timestamp, D2>,
        S: StateBackend>
    (&self, other: &Stream<G, D2>, pact1: P1, pact2: P2, name: &str, init: impl IntoIterator<Item=G::Timestamp>, logic: L) -> Stream<G, D3>;

    /// Creates a new dataflow operator that partitions its input streams by a parallelization
    /// strategy `pact`, and repeatedly invokes `logic`, the function returned by the function passed as `constructor`.
    /// `logic` can read from the input streams, write to the output stream, and inspect the frontier at the inputs.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Inspect, FrontierNotificator};
    /// use timely::dataflow::operators::generic::operator::Operator;
    /// use timely::dataflow::channels::pact::Pipeline;
    /// use timely::dataflow::Scope;
    ///
    /// timely::example(|scope| {
    ///     let stream2 = (0u64..10).to_stream(scope);
    ///     (0u64..10).to_stream(scope)
    ///         .binary(&stream2, Pipeline, Pipeline, "example", |default_cap, _info, _state_handle| {
    ///             let mut cap = Some(default_cap.delayed(&12));
    ///             let mut vector1 = Vec::new();
    ///             let mut vector2 = Vec::new();
    ///             move |input1, input2, output| {
    ///                 if let Some(ref c) = cap.take() {
    ///                     output.session(&c).give(100);
    ///                 }
    ///                 while let Some((time, data)) = input1.next() {
    ///                     data.swap(&mut vector1);
    ///                     output.session(&time).give_vec(&mut vector1);
    ///                 }
    ///                 while let Some((time, data)) = input2.next() {
    ///                     data.swap(&mut vector2);
    ///                     output.session(&time).give_vec(&mut vector2);
    ///                 }
    ///             }
    ///         }).inspect(|x| println!("{:?}", x));
    /// });
    /// ```
    fn binary<D2, D3, B, L, P1, P2>(&self, other: &Stream<G, D2>, pact1: P1, pact2: P2, name: &str, constructor: B) -> Stream<G, D3>
    where
        D2: Data,
        D3: Data,
        B: FnOnce(Capability<G::Timestamp>, OperatorInfo, StateHandle<G::StateBackend>) -> L,
        L: FnMut(&mut InputHandle<G::Timestamp, D1, P1::Puller>,
                 &mut InputHandle<G::Timestamp, D2, P2::Puller>,
                 &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>)+'static,
        P1: ParallelizationContract<G::Timestamp, D1>,
        P2: ParallelizationContract<G::Timestamp, D2>;

    /// Creates a new dataflow operator that partitions its input streams by a parallelization
    /// strategy `pact`, and repeatedly invokes `logic`, the function returned by the function passed as `constructor`.
    /// `logic` can read from the input streams, write to the output stream, and inspect the frontier at the inputs.
    ///
    /// This variant allows specifying the state backend that will be used by `state_handle`.
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, Inspect, FrontierNotificator};
    /// use timely::dataflow::operators::generic::operator::Operator;
    /// use timely::dataflow::channels::pact::Pipeline;
    /// use timely::dataflow::Scope;
    /// use timely::state::StateHandle;
    /// use timely::state::backends::InMemoryBackend;
    ///
    /// timely::example(|scope| {
    ///     let stream2 = (0u64..10).to_stream(scope);
    ///     (0u64..10).to_stream(scope)
    ///         .binary_core(&stream2, Pipeline, Pipeline, "example", |default_cap, _info, _state_handle: StateHandle<InMemoryBackend>| {
    ///             let mut cap = Some(default_cap.delayed(&12));
    ///             let mut vector1 = Vec::new();
    ///             let mut vector2 = Vec::new();
    ///             move |input1, input2, output| {
    ///                 if let Some(ref c) = cap.take() {
    ///                     output.session(&c).give(100);
    ///                 }
    ///                 while let Some((time, data)) = input1.next() {
    ///                     data.swap(&mut vector1);
    ///                     output.session(&time).give_vec(&mut vector1);
    ///                 }
    ///                 while let Some((time, data)) = input2.next() {
    ///                     data.swap(&mut vector2);
    ///                     output.session(&time).give_vec(&mut vector2);
    ///                 }
    ///             }
    ///         }).inspect(|x| println!("{:?}", x));
    /// });
    /// ```
    fn binary_core<D2, D3, B, L, P1, P2, S>(&self, other: &Stream<G, D2>, pact1: P1, pact2: P2, name: &str, constructor: B) -> Stream<G, D3>
    where
        D2: Data,
        D3: Data,
        B: FnOnce(Capability<G::Timestamp>, OperatorInfo, StateHandle<S>) -> L,
        L: FnMut(&mut InputHandle<G::Timestamp, D1, P1::Puller>,
            &mut InputHandle<G::Timestamp, D2, P2::Puller>,
            &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>)+'static,
        P1: ParallelizationContract<G::Timestamp, D1>,
        P2: ParallelizationContract<G::Timestamp, D2>,
        S: StateBackend;

    /// Creates a new dataflow operator that partitions its input stream by a parallelization
    /// strategy `pact`, and repeatedly invokes the function `logic` which can read from the input stream
    /// and inspect the frontier at the input.
    ///
    /// # Examples
    /// ```
    /// use timely::dataflow::operators::{ToStream, FrontierNotificator};
    /// use timely::dataflow::operators::generic::operator::Operator;
    /// use timely::dataflow::channels::pact::Pipeline;
    /// use timely::dataflow::Scope;
    ///
    /// timely::example(|scope| {
    ///     (0u64..10)
    ///         .to_stream(scope)
    ///         .sink(Pipeline, "example", |input| {
    ///             while let Some((time, data)) = input.next() {
    ///                 for datum in data.iter() {
    ///                     println!("{:?}:\t{:?}", time, datum);
    ///                 }
    ///             }
    ///         });
    /// });
    /// ```
    fn sink<L, P>(&self, pact: P, name: &str, logic: L)
    where
        L: FnMut(&mut FrontieredInputHandle<G::Timestamp, D1, P::Puller>)+'static,
        P: ParallelizationContract<G::Timestamp, D1>;
}

impl<G: Scope, D1: Data> Operator<G, D1> for Stream<G, D1> {

    fn unary_frontier<D2, B, L, P>(&self, pact: P, name: &str, constructor: B) -> Stream<G, D2>
    where
        D2: Data,
        B: FnOnce(Capability<G::Timestamp>, OperatorInfo, StateHandle<G::StateBackend>) -> L,
        L: FnMut(&mut FrontieredInputHandle<G::Timestamp, D1, P::Puller>,
            &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>)+'static,
        P: ParallelizationContract<G::Timestamp, D1>
    {
        let mut builder = OperatorBuilder::new(name.to_owned(), self.scope());
        let operator_info = builder.operator_info();
        let state_handle = self.scope().get_state_handle().create_sub_handle(&operator_info.global_id.to_string());

        let mut input = builder.new_input(self, pact);
        let (mut output, stream) = builder.new_output();

        builder.build(move |mut capabilities| {
            // `capabilities` should be a single-element vector.
            let capability = capabilities.pop().unwrap();
            let mut logic = constructor(capability, operator_info, state_handle);
            move |frontiers| {
                let mut input_handle = FrontieredInputHandle::new(&mut input, &frontiers[0]);
                let mut output_handle = output.activate();
                logic(&mut input_handle, &mut output_handle);
            }
        });

        stream
    }

    fn unary_frontier_core<D2, B, L, P, S>(&self, pact: P, name: &str, constructor: B) -> Stream<G, D2>
    where
        D2: Data,
        B: FnOnce(Capability<G::Timestamp>, OperatorInfo, StateHandle<S>) -> L,
        L: FnMut(&mut FrontieredInputHandle<G::Timestamp, D1, P::Puller>,
                 &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>)+'static,
        P: ParallelizationContract<G::Timestamp, D1>,
        S: StateBackend
    {

        let mut builder = OperatorBuilder::new(name.to_owned(), self.scope());
        let operator_info = builder.operator_info();
        let state_handle = StateHandle::new(Rc::new(S::new()), &operator_info.global_id.to_string());

        let mut input = builder.new_input(self, pact);
        let (mut output, stream) = builder.new_output();

        builder.build(move |mut capabilities| {
            // `capabilities` should be a single-element vector.
            let capability = capabilities.pop().unwrap();
            let mut logic = constructor(capability, operator_info, state_handle);
            move |frontiers| {
                let mut input_handle = FrontieredInputHandle::new(&mut input, &frontiers[0]);
                let mut output_handle = output.activate();
                logic(&mut input_handle, &mut output_handle);
            }
        });

        stream
    }

    fn unary_notify<D2: Data,
        L: FnMut(&mut InputHandle<G::Timestamp, D1, P::Puller>,
            &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>,
            &mut Notificator<G::Timestamp>,
            &StateHandle<G::StateBackend>)+'static,
        P: ParallelizationContract<G::Timestamp, D1>>
    (&self, pact: P, name: &str, init: impl IntoIterator<Item=G::Timestamp>, mut logic: L) -> Stream<G, D2> {
        self.unary_frontier(pact, name, move |capability, _info, state_handle| {
            let mut notificator = FrontierNotificator::new();
            for time in init {
                notificator.notify_at(capability.delayed(&time));
            }

            let logging = self.scope().logging();
            move |input, output| {
                let frontier = &[input.frontier()];
                let notificator = &mut Notificator::new(frontier, &mut notificator, &logging);
                logic(&mut input.handle, output, notificator, &state_handle);
            }
        })
    }

    fn unary_notify_core<D2: Data,
            L: FnMut(&mut InputHandle<G::Timestamp, D1, P::Puller>,
                     &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>,
                     &mut Notificator<G::Timestamp>,
                     &StateHandle<S>)+'static,
            P: ParallelizationContract<G::Timestamp, D1>,
            S: StateBackend>
             (&self, pact: P, name: &str, init: impl IntoIterator<Item=G::Timestamp>, mut logic: L) -> Stream<G, D2> {

        self.unary_frontier_core(pact, name, move |capability, _info, state_handle| {
            let mut notificator = FrontierNotificator::new();
            for time in init {
                notificator.notify_at(capability.delayed(&time));
            }

            let logging = self.scope().logging();
            move |input, output| {
                let frontier = &[input.frontier()];
                let notificator = &mut Notificator::new(frontier, &mut notificator, &logging);
                logic(&mut input.handle, output, notificator, &state_handle);
            }
        })
    }

    fn unary<D2, B, L, P>(&self, pact: P, name: &str, constructor: B) -> Stream<G, D2>
    where
        D2: Data,
        B: FnOnce(Capability<G::Timestamp>, OperatorInfo, StateHandle<G::StateBackend>) -> L,
        L: FnMut(&mut InputHandle<G::Timestamp, D1, P::Puller>,
            &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>)+'static,
        P: ParallelizationContract<G::Timestamp, D1>
    {
        let mut builder = OperatorBuilder::new(name.to_owned(), self.scope());
        let operator_info = builder.operator_info();
        let state_handle = self.scope().get_state_handle().create_sub_handle(&operator_info.global_id.to_string());

        let mut input = builder.new_input(self, pact);
        let (mut output, stream) = builder.new_output();
        builder.set_notify(false);

        builder.build(move |mut capabilities| {
            // `capabilities` should be a single-element vector.
            let capability = capabilities.pop().unwrap();
            let mut logic = constructor(capability, operator_info, state_handle);
            move |_frontiers| {
                let mut output_handle = output.activate();
                logic(&mut input, &mut output_handle);
            }
        });

        stream
    }

    fn unary_core<D2, B, L, P, S>(&self, pact: P, name: &str, constructor: B) -> Stream<G, D2>
    where
        D2: Data,
        B: FnOnce(Capability<G::Timestamp>, OperatorInfo, StateHandle<S>) -> L,
        L: FnMut(&mut InputHandle<G::Timestamp, D1, P::Puller>,
                 &mut OutputHandle<G::Timestamp, D2, Tee<G::Timestamp, D2>>)+'static,
        P: ParallelizationContract<G::Timestamp, D1>,
        S: StateBackend
    {

        let mut builder = OperatorBuilder::new(name.to_owned(), self.scope());
        let operator_info = builder.operator_info();
        let state_handle = StateHandle::new(Rc::new(S::new()), &operator_info.global_id.to_string());

        let mut input = builder.new_input(self, pact);
        let (mut output, stream) = builder.new_output();
        builder.set_notify(false);

        builder.build(move |mut capabilities| {
            // `capabilities` should be a single-element vector.
            let capability = capabilities.pop().unwrap();
            let mut logic = constructor(capability, operator_info, state_handle);
            move |_frontiers| {
                let mut output_handle = output.activate();
                logic(&mut input, &mut output_handle);
            }
        });

        stream
    }

    fn binary_frontier<D2, D3, B, L, P1, P2>(&self, other: &Stream<G, D2>, pact1: P1, pact2: P2, name: &str, constructor: B) -> Stream<G, D3>
    where
        D2: Data,
        D3: Data,
        B: FnOnce(Capability<G::Timestamp>, OperatorInfo, StateHandle<G::StateBackend>) -> L,
        L: FnMut(&mut FrontieredInputHandle<G::Timestamp, D1, P1::Puller>,
            &mut FrontieredInputHandle<G::Timestamp, D2, P2::Puller>,
            &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>)+'static,
        P1: ParallelizationContract<G::Timestamp, D1>,
        P2: ParallelizationContract<G::Timestamp, D2>
    {
        let mut builder = OperatorBuilder::new(name.to_owned(), self.scope());
        let operator_info = builder.operator_info();
        let state_handle = self.scope().get_state_handle().create_sub_handle(&operator_info.global_id.to_string());

        let mut input1 = builder.new_input(self, pact1);
        let mut input2 = builder.new_input(other, pact2);
        let (mut output, stream) = builder.new_output();

        builder.build(move |mut capabilities| {
            // `capabilities` should be a single-element vector.
            let capability = capabilities.pop().unwrap();
            let mut logic = constructor(capability, operator_info, state_handle);
            move |frontiers| {
                let mut input1_handle = FrontieredInputHandle::new(&mut input1, &frontiers[0]);
                let mut input2_handle = FrontieredInputHandle::new(&mut input2, &frontiers[1]);
                let mut output_handle = output.activate();
                logic(&mut input1_handle, &mut input2_handle, &mut output_handle);
            }
        });

        stream
    }

    fn binary_frontier_core<D2, D3, B, L, P1, P2, S>(&self, other: &Stream<G, D2>, pact1: P1, pact2: P2, name: &str, constructor: B) -> Stream<G, D3>
    where
        D2: Data,
        D3: Data,
        B: FnOnce(Capability<G::Timestamp>, OperatorInfo, StateHandle<S>) -> L,
        L: FnMut(&mut FrontieredInputHandle<G::Timestamp, D1, P1::Puller>,
                 &mut FrontieredInputHandle<G::Timestamp, D2, P2::Puller>,
                 &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>)+'static,
        P1: ParallelizationContract<G::Timestamp, D1>,
        P2: ParallelizationContract<G::Timestamp, D2>,
        S: StateBackend
    {

        let mut builder = OperatorBuilder::new(name.to_owned(), self.scope());
        let operator_info = builder.operator_info();
        let state_handle = StateHandle::new(Rc::new(S::new()), &operator_info.global_id.to_string());

        let mut input1 = builder.new_input(self, pact1);
        let mut input2 = builder.new_input(other, pact2);
        let (mut output, stream) = builder.new_output();

        builder.build(move |mut capabilities| {
            // `capabilities` should be a single-element vector.
            let capability = capabilities.pop().unwrap();
            let mut logic = constructor(capability, operator_info, state_handle);
            move |frontiers| {
                let mut input1_handle = FrontieredInputHandle::new(&mut input1, &frontiers[0]);
                let mut input2_handle = FrontieredInputHandle::new(&mut input2, &frontiers[1]);
                let mut output_handle = output.activate();
                logic(&mut input1_handle, &mut input2_handle, &mut output_handle);
            }
        });

        stream
    }

    fn binary_notify<D2: Data,
        D3: Data,
        L: FnMut(&mut InputHandle<G::Timestamp, D1, P1::Puller>,
            &mut InputHandle<G::Timestamp, D2, P2::Puller>,
            &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>,
            &mut Notificator<G::Timestamp>,
            &StateHandle<G::StateBackend>)+'static,
        P1: ParallelizationContract<G::Timestamp, D1>,
        P2: ParallelizationContract<G::Timestamp, D2>>
    (&self, other: &Stream<G, D2>, pact1: P1, pact2: P2, name: &str, init: impl IntoIterator<Item=G::Timestamp>, mut logic: L) -> Stream<G, D3> {
        self.binary_frontier(other, pact1, pact2, name, |capability, _info, state_handle| {
            let mut notificator = FrontierNotificator::new();
            for time in init {
                notificator.notify_at(capability.delayed(&time));
            }

            let logging = self.scope().logging();
            move |input1, input2, output| {
                let frontiers = &[input1.frontier(), input2.frontier()];
                let notificator = &mut Notificator::new(frontiers, &mut notificator, &logging);
                logic(&mut input1.handle, &mut input2.handle, output, notificator, &state_handle);
            }
        })
    }

    fn binary_notify_core<D2: Data,
              D3: Data,
              L: FnMut(&mut InputHandle<G::Timestamp, D1, P1::Puller>,
                       &mut InputHandle<G::Timestamp, D2, P2::Puller>,
                       &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>,
                       &mut Notificator<G::Timestamp>,
                       &StateHandle<S>)+'static,
              P1: ParallelizationContract<G::Timestamp, D1>,
              P2: ParallelizationContract<G::Timestamp, D2>,
              S: StateBackend>
            (&self, other: &Stream<G, D2>, pact1: P1, pact2: P2, name: &str, init: impl IntoIterator<Item=G::Timestamp>, mut logic: L) -> Stream<G, D3> {

        self.binary_frontier_core(other, pact1, pact2, name, |capability, _info, state_handle| {
            let mut notificator = FrontierNotificator::new();
            for time in init {
                notificator.notify_at(capability.delayed(&time));
            }

            let logging = self.scope().logging();
            move |input1, input2, output| {
                let frontiers = &[input1.frontier(), input2.frontier()];
                let notificator = &mut Notificator::new(frontiers, &mut notificator, &logging);
                logic(&mut input1.handle, &mut input2.handle, output, notificator, &state_handle);
            }
        })

    }

    fn binary<D2, D3, B, L, P1, P2>(&self, other: &Stream<G, D2>, pact1: P1, pact2: P2, name: &str, constructor: B) -> Stream<G, D3>
    where
        D2: Data,
        D3: Data,
        B: FnOnce(Capability<G::Timestamp>, OperatorInfo, StateHandle<G::StateBackend>) -> L,
        L: FnMut(&mut InputHandle<G::Timestamp, D1, P1::Puller>,
            &mut InputHandle<G::Timestamp, D2, P2::Puller>,
            &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>)+'static,
        P1: ParallelizationContract<G::Timestamp, D1>,
        P2: ParallelizationContract<G::Timestamp, D2>
    {
        let mut builder = OperatorBuilder::new(name.to_owned(), self.scope());
        let operator_info = builder.operator_info();
        let state_handle = self.scope().get_state_handle().create_sub_handle(&operator_info.global_id.to_string());

        let mut input1 = builder.new_input(self, pact1);
        let mut input2 = builder.new_input(other, pact2);
        let (mut output, stream) = builder.new_output();
        builder.set_notify(false);

        builder.build(move |mut capabilities| {
            // `capabilities` should be a single-element vector.
            let capability = capabilities.pop().unwrap();
            let mut logic = constructor(capability, operator_info, state_handle);
            move |_frontiers| {
                let mut output_handle = output.activate();
                logic(&mut input1, &mut input2, &mut output_handle);
            }
        });

        stream
    }

    fn binary_core<D2, D3, B, L, P1, P2, S>(&self, other: &Stream<G, D2>, pact1: P1, pact2: P2, name: &str, constructor: B) -> Stream<G, D3>
    where
        D2: Data,
        D3: Data,
        B: FnOnce(Capability<G::Timestamp>, OperatorInfo, StateHandle<S>) -> L,
        L: FnMut(&mut InputHandle<G::Timestamp, D1, P1::Puller>,
                 &mut InputHandle<G::Timestamp, D2, P2::Puller>,
                 &mut OutputHandle<G::Timestamp, D3, Tee<G::Timestamp, D3>>)+'static,
        P1: ParallelizationContract<G::Timestamp, D1>,
        P2: ParallelizationContract<G::Timestamp, D2>,
        S: StateBackend
    {

        let mut builder = OperatorBuilder::new(name.to_owned(), self.scope());
        let operator_info = builder.operator_info();
        let state_handle = StateHandle::new(Rc::new(S::new()), &operator_info.global_id.to_string());

        let mut input1 = builder.new_input(self, pact1);
        let mut input2 = builder.new_input(other, pact2);
        let (mut output, stream) = builder.new_output();
        builder.set_notify(false);

        builder.build(move |mut capabilities| {
            // `capabilities` should be a single-element vector.
            let capability = capabilities.pop().unwrap();
            let mut logic = constructor(capability, operator_info, state_handle);
            move |_frontiers| {
                let mut output_handle = output.activate();
                logic(&mut input1, &mut input2, &mut output_handle);
            }
        });

        stream
    }

    fn sink<L, P>(&self, pact: P, name: &str, mut logic: L)
    where
        L: FnMut(&mut FrontieredInputHandle<G::Timestamp, D1, P::Puller>)+'static,
        P: ParallelizationContract<G::Timestamp, D1> {

        let mut builder = OperatorBuilder::new(name.to_owned(), self.scope());
        let mut input = builder.new_input(self, pact);

        builder.build(|_capabilities| {
            move |frontiers| {
                let mut input_handle = FrontieredInputHandle::new(&mut input, &frontiers[0]);
                logic(&mut input_handle);
            }
        });
    }
}

/// Creates a new data stream source for a scope.
///
/// The source is defined by a name, and a constructor which takes a default capability to
/// a method that can be repeatedly called on a output handle. The method is then repeatedly
/// invoked, and is expected to eventually send data and downgrade and release capabilities.
///
/// # Examples
/// ```
/// use timely::scheduling::Scheduler;
/// use timely::dataflow::operators::Inspect;
/// use timely::dataflow::operators::generic::operator::source;
/// use timely::dataflow::Scope;
///
/// timely::example(|scope| {
///
///     source(scope, "Source", |capability, info| {
///
///         let activator = scope.activator_for(&info.address[..]);
///
///         let mut cap = Some(capability);
///         move |output| {
///
///             let mut done = false;
///             if let Some(cap) = cap.as_mut() {
///                 // get some data and send it.
///                 let time = cap.time().clone();
///                 output.session(&cap)
///                       .give(*cap.time());
///
///                 // downgrade capability.
///                 cap.downgrade(&(time + 1));
///                 done = time > 20;
///             }
///
///             if done { cap = None; }
///             else    { activator.activate(); }
///         }
///     })
///     .inspect(|x| println!("number: {:?}", x));
/// });
/// ```
pub fn source<G: Scope, D, B, L>(scope: &G, name: &str, constructor: B) -> Stream<G, D>
where
    D: Data,
    B: FnOnce(Capability<G::Timestamp>, OperatorInfo) -> L,
    L: FnMut(&mut OutputHandle<G::Timestamp, D, Tee<G::Timestamp, D>>)+'static {

    let mut builder = OperatorBuilder::new(name.to_owned(), scope.clone());
    let operator_info = builder.operator_info();

    let (mut output, stream) = builder.new_output();
    builder.set_notify(false);

    builder.build(move |mut capabilities| {
        // `capabilities` should be a single-element vector.
        let capability = capabilities.pop().unwrap();
        let mut logic = constructor(capability, operator_info);
        move |_frontier| {
            logic(&mut output.activate());
        }
    });

    stream
}

/// Constructs an empty stream.
///
/// This method is useful in patterns where an input is required, but there is no
/// meaningful data to provide. The replaces patterns like `stream.filter(|_| false)`
/// which are just silly.
///
/// # Examples
/// ```
/// use timely::dataflow::operators::Inspect;
/// use timely::dataflow::operators::generic::operator::empty;
/// use timely::dataflow::Scope;
///
/// timely::example(|scope| {
///
///
///     empty(scope)     //-- type required in this example
///         .inspect(|_: &()| panic!("never called"));
///
/// });
/// ```
pub fn empty<G: Scope, D: Data>(scope: &G) -> Stream<G, D> {
    source(scope, "Empty", |_capability, _info| |_output| {
        // drop capability, do nothing
    })
}