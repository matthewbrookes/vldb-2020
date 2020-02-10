//! Starts a timely dataflow execution from configuration information and per-worker logic.
use crate::communication::{initialize_from, Configuration, Allocator, allocator::AllocateBuilder, WorkerGuards};
use crate::dataflow::scopes::Child;
use crate::worker::Worker;
use crate::state::backends::{InMemoryBackend, FASTERBackend};
use crate::state::StateHandle;

use std::rc::Rc;
use faster_rs::{FasterKv, FasterKvBuilder};
use tempfile::TempDir;
use std::sync::Arc;
use timely_state::backends::FASTERNodeBackend;
use std::time::Duration;

/// Executes a single-threaded timely dataflow computation.
///
/// The `example` method takes a closure on a `Scope` which it executes to initialize and run a
/// timely dataflow computation on a single thread. This method is intended for use in examples,
/// rather than programs that may need to run across multiple workers.
///
/// The `example` method returns whatever the single worker returns from its closure.
/// This is often nothing, but the worker can return something about the data it saw in order to
/// test computations.
///
/// The method aggressively unwraps returned `Result<_>` types.
///
/// # Examples
///
/// The simplest example creates a stream of data and inspects it.
///
/// ```rust
/// use timely::dataflow::operators::{ToStream, Inspect};
///
/// timely::example(|scope| {
///     (0..10).to_stream(scope)
///            .inspect(|x| println!("seen: {:?}", x));
/// });
/// ```
///
/// This next example captures the data and displays them once the computation is complete.
///
/// More precisely, the example captures a stream of events (receiving batches of data,
/// updates to input capabilities) and displays these events.
///
/// ```rust
/// use timely::dataflow::operators::{ToStream, Inspect, Capture};
/// use timely::dataflow::operators::capture::Extract;
///
/// let data = timely::example(|scope| {
///     (0..10).to_stream(scope)
///            .inspect(|x| println!("seen: {:?}", x))
///            .capture()
/// });
///
/// // the extracted data should have data (0..10) at timestamp 0.
/// assert_eq!(data.extract()[0].1, (0..10).collect::<Vec<_>>());
/// ```
pub fn example<T, F>(func: F) -> T
where
    T: Send+'static,
    F: FnOnce(&mut Child<Worker<crate::communication::allocator::thread::Thread>,u64,InMemoryBackend>)->T+Send+Sync+'static
{
    crate::execute::execute_directly(|worker| worker.dataflow(|scope, _| func(scope)))
}


/// Executes a single-threaded timely dataflow computation.
///
/// The `execute_directly` constructs a `Worker` and directly executes the supplied
/// closure to construct and run a timely dataflow computation. It does not create any
/// worker threads, and simply uses the current thread of control.
///
/// The closure may return a result, which will be returned from the computation.
///
/// # Examples
/// ```rust
/// use timely::dataflow::operators::{ToStream, Inspect};
/// use timely::state::backends::InMemoryBackend;
///
/// // execute a timely dataflow using three worker threads.
/// timely::execute_directly(|worker| {
///     worker.dataflow::<(),_,_,InMemoryBackend>(|scope| {
///         (0..10).to_stream(scope)
///                .inspect(|x| println!("seen: {:?}", x));
///     })
/// });
/// ```
pub fn execute_directly<T, F>(func: F) -> T
where
    T: Send+'static,
    F: FnOnce(&mut Worker<crate::communication::allocator::thread::Thread>)->T+Send+Sync+'static
{
    let alloc = crate::communication::allocator::thread::Thread::new();
    let mut worker = crate::worker::Worker::new(alloc);
    let result = func(&mut worker);
    while worker.step_or_park(None) { }
    result
}

/// Executes a timely dataflow from a configuration and per-communicator logic.
///
/// The `execute` method takes a `Configuration` and spins up some number of
/// workers threads, each of which execute the supplied closure to construct
/// and run a timely dataflow computation.
///
/// The closure may return a `T: Send+'static`, and `execute` returns a result
/// containing a `WorkerGuards<T>` (or error information), which can be joined
/// to recover the result `T` values from the local workers.
///
/// # Examples
/// ```rust
/// use timely::dataflow::operators::{ToStream, Inspect};
/// use timely::state::backends::InMemoryBackend;
///
/// // execute a timely dataflow using three worker threads.
/// timely::execute(timely::Configuration::Process(3), |worker| {
///     worker.dataflow::<(),_,_,InMemoryBackend>(|scope| {
///         (0..10).to_stream(scope)
///                .inspect(|x| println!("seen: {:?}", x));
///     })
/// }).unwrap();
/// ```
///
/// The following example demonstrates how one can extract data from a multi-worker execution.
/// In a multi-process setting, each process will only receive those records present at workers
/// in the process.
///
/// ```rust
/// use std::sync::{Arc, Mutex};
/// use timely::dataflow::operators::{ToStream, Inspect, Capture};
/// use timely::dataflow::operators::capture::Extract;
/// use timely::state::backends::InMemoryBackend;
///
/// // get send and recv endpoints, wrap send to share
/// let (send, recv) = ::std::sync::mpsc::channel();
/// let send = Arc::new(Mutex::new(send));
///
/// // execute a timely dataflow using three worker threads.
/// timely::execute(timely::Configuration::Process(3), move |worker| {
///     let send = send.lock().unwrap().clone();
///     worker.dataflow::<(),_,_,InMemoryBackend>(move |scope| {
///         (0..10).to_stream(scope)
///                .inspect(|x| println!("seen: {:?}", x))
///                .capture_into(send);
///     });
/// }).unwrap();
///
/// // the extracted data should have data (0..10) thrice at timestamp 0.
/// assert_eq!(recv.extract()[0].1, (0..30).map(|x| x / 3).collect::<Vec<_>>());
/// ```
pub fn execute<T, F>(mut config: Configuration, func: F) -> Result<WorkerGuards<T>,String>
where
    T:Send+'static,
    F: Fn(&mut Worker<Allocator>, StateHandle<FASTERNodeBackend>)->T+Send+Sync+'static {

    if let Configuration::Cluster { ref mut log_fn, .. } = config {

        *log_fn = Box::new(|events_setup| {

            let mut result = None;
            if let Ok(addr) = ::std::env::var("TIMELY_COMM_LOG_ADDR") {

                use ::std::net::TcpStream;
                use crate::logging::BatchLogger;
                use crate::dataflow::operators::capture::EventWriter;

                eprintln!("enabled COMM logging to {}", addr);

                if let Ok(stream) = TcpStream::connect(&addr) {
                    let writer = EventWriter::new(stream);
                    let mut logger = BatchLogger::new(writer);
                    result = Some(crate::logging_core::Logger::new(
                        ::std::time::Instant::now(),
                        events_setup,
                        move |time, data| logger.publish_batch(time, data)
                    ));
                }
                else {
                    panic!("Could not connect to communication log address: {:?}", addr);
                }
            }
            result
        });
    }

    let (allocators, other) = config.try_build()?;

    let faster_directory = Arc::new(TempDir::new_in(".").expect("Unable to create directory for FASTER"));
    let faster_directory_string = faster_directory.path().to_str().unwrap();

    let mut builder = FasterKvBuilder::new(1 << 24, 12 * 1024 * 1024 * 1024);
    builder.with_disk(faster_directory_string)
            .set_pre_allocate_log(false);
    let faster_kv = Arc::new(builder.build().unwrap());

    initialize_from(allocators, other, move |allocator| {

        let mut worker = Worker::new(allocator);

        // If an environment variable is set, use it as the default timely logging.
        if let Ok(addr) = ::std::env::var("TIMELY_WORKER_LOG_ADDR") {

            use ::std::net::TcpStream;
            use crate::logging::{BatchLogger, TimelyEvent};
            use crate::dataflow::operators::capture::EventWriter;

            if let Ok(stream) = TcpStream::connect(&addr) {
                let writer = EventWriter::new(stream);
                let mut logger = BatchLogger::new(writer);
                worker.log_register()
                    .insert::<TimelyEvent,_>("timely", move |time, data|
                        logger.publish_batch(time, data)
                    );
            }
            else {
                panic!("Could not connect logging stream to: {:?}", addr);
            }
        }

        faster_kv.start_session();
        let faster_backend = Rc::new(FASTERNodeBackend::new_from_existing(&faster_kv, &faster_directory));
        let state_handle = StateHandle::new(faster_backend, "");

        let result = func(&mut worker, state_handle);
        while worker.step_or_park(None) { }

        result
    })
}

/// Executes a timely dataflow from supplied arguments and per-communicator logic.
///
/// The `execute` method takes arguments (typically `std::env::args()`) and spins up some number of
/// workers threads, each of which execute the supplied closure to construct and run a timely
/// dataflow computation.
///
/// The closure may return a `T: Send+'static`, and `execute` returns a result
/// containing a `WorkerGuards<T>` (or error information), which can be joined
/// to recover the result `T` values from the local workers.
///
/// The arguments `execute` currently understands are:
///
/// `-w, --workers`: number of per-process worker threads.
///
/// `-n, --processes`: number of processes involved in the computation.
///
/// `-p, --process`: identity of this process; from 0 to n-1.
///
/// `-h, --hostfile`: a text file whose lines are "hostname:port" in order of process identity.
/// If not specified, `localhost` will be used, with port numbers increasing from 2101 (chosen
/// arbitrarily).
///
/// # Examples
///
/// ```rust
/// use timely::dataflow::operators::{ToStream, Inspect};
/// use timely::state::backends::InMemoryBackend;
///
/// // execute a timely dataflow using command line parameters
/// timely::execute_from_args(std::env::args(), |worker| {
///     worker.dataflow::<(),_,_,InMemoryBackend>(|scope| {
///         (0..10).to_stream(scope)
///                .inspect(|x| println!("seen: {:?}", x));
///     })
/// }).unwrap();
/// ```
/// ```ignore
/// host0% cargo run -- -w 2 -n 4 -h hosts.txt -p 0
/// host1% cargo run -- -w 2 -n 4 -h hosts.txt -p 1
/// host2% cargo run -- -w 2 -n 4 -h hosts.txt -p 2
/// host3% cargo run -- -w 2 -n 4 -h hosts.txt -p 3
/// ```
/// ```ignore
/// % cat hosts.txt
/// host0:port
/// host1:port
/// host2:port
/// host3:port
/// ```
pub fn execute_from_args<I, T, F>(iter: I, func: F) -> Result<WorkerGuards<T>,String>
    where I: Iterator<Item=String>,
          T:Send+'static,
          F: Fn(&mut Worker<Allocator>, StateHandle<FASTERNodeBackend>)->T+Send+Sync+'static, {
    let configuration = Configuration::from_args(iter)?;
    execute(configuration, func)
}

/// Executes a timely dataflow from supplied allocators and logging.
///
/// Refer to [`execute`](fn.execute.html) for more details.
///
/// ```rust
/// use timely::dataflow::operators::{ToStream, Inspect};
/// use timely::state::backends::InMemoryBackend;
///
/// // execute a timely dataflow using command line parameters
/// let (builders, other) = timely::Configuration::Process(3).try_build().unwrap();
/// timely::execute::execute_from(builders, other, |worker| {
///     worker.dataflow::<(),_,_,InMemoryBackend>(|scope| {
///         (0..10).to_stream(scope)
///                .inspect(|x| println!("seen: {:?}", x));
///     })
/// }).unwrap();
/// ```
pub fn execute_from<A, T, F>(builders: Vec<A>, others: Box<::std::any::Any>, func: F) -> Result<WorkerGuards<T>,String>
where
    A: AllocateBuilder+'static,
    T: Send+'static,
    F: Fn(&mut Worker<<A as AllocateBuilder>::Allocator>, StateHandle<FASTERNodeBackend>)->T+Send+Sync+'static {
    let faster_directory = Arc::new(TempDir::new_in(".").expect("Unable to create directory for FASTER"));
    let faster_directory_string = faster_directory.path().to_str().unwrap();

    let mut builder = FasterKvBuilder::new(1 << 24, 12 * 1024 * 1024 * 1024);
    builder.with_disk(faster_directory_string)
        .set_pre_allocate_log(false);
    let faster_kv = Arc::new(builder.build().unwrap());

    initialize_from(builders, others, move |allocator| {
        let mut worker = Worker::new(allocator);
        faster_kv.start_session();
        let faster_backend = Rc::new(FASTERNodeBackend::new_from_existing(&faster_kv, &faster_directory));
        let state_handle = StateHandle::new(faster_backend, &worker.index().to_string());

        let result = func(&mut worker, state_handle);
        while worker.step_or_park(None) { }

        faster_kv.complete_pending(true);
        faster_kv.stop_session();
        result
    })
}
