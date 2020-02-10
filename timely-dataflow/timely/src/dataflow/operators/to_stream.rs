//! Conversion to the `Stream` type from iterators.

use crate::progress::Timestamp;

use crate::Data;
use crate::dataflow::channels::Message;
use crate::dataflow::operators::generic::operator::source;
use crate::dataflow::{Stream, Scope};

/// Converts to a timely `Stream`.
pub trait ToStream<T: Timestamp, D: Data> {
    /// Converts to a timely `Stream`.
    ///
    /// # Examples
    ///
    /// ```
    /// use timely::dataflow::operators::{ToStream, Capture};
    /// use timely::dataflow::operators::capture::Extract;
    ///
    /// let (data1, data2) = timely::example(|scope| {
    ///     let data1 = (0..3).to_stream(scope).capture();
    ///     let data2 = vec![0,1,2].to_stream(scope).capture();
    ///     (data1, data2)
    /// });
    ///
    /// assert_eq!(data1.extract(), data2.extract());
    /// ```
    fn to_stream<S: Scope<Timestamp=T>>(self, scope: &mut S) -> Stream<S, D>;
}

impl<T: Timestamp, I: IntoIterator+'static> ToStream<T, I::Item> for I where I::Item: Data {
    fn to_stream<S: Scope<Timestamp=T>>(self, scope: &mut S) -> Stream<S, I::Item> {

        source(scope, "ToStream", |capability, info| {

            // Acquire an activator, so that the operator can rescheduled itself.
            let activator = scope.activator_for(&info.address[..]);

            let mut iterator = self.into_iter().fuse();
            let mut capability = Some(capability);

            move |output| {

                if let Some(element) = iterator.next() {
                    let mut session = output.session(capability.as_ref().unwrap());
                    session.give(element);
                    for element in iterator.by_ref().take((256 * Message::<T, I::Item>::default_length()) - 1) {
                        session.give(element);
                    }
                    activator.activate();
                }
                else {
                    capability = None;
                }
            }
        })
    }
}
