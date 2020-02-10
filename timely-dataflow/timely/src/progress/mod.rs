//! Progress tracking mechanisms to support notification in timely dataflow

pub use self::operate::Operate;
pub use self::subgraph::{Subgraph, SubgraphBuilder};
pub use self::timestamp::{Timestamp, PathSummary};
pub use self::change_batch::ChangeBatch;
pub use self::frontier::Antichain;

pub mod change_batch;
pub mod frontier;
pub mod timestamp;
pub mod operate;
pub mod broadcast;
pub mod reachability;
pub mod subgraph;

/// A timely dataflow location.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Abomonation, Serialize, Deserialize)]
pub struct Location {
    /// A scope-local operator identifier.
    node: usize,
    /// An operator port identifier.`
    port: Port,
}

impl Location {
    /// Creates a new target location (operator input or scope output).
    pub fn new_target(node: usize, port: usize) -> Location {
        Location { node, port: Port::Target(port) }
    }
    /// Creates a new source location (operator output or scope input).
    pub fn new_source(node: usize, port: usize) -> Location {
        Location { node, port: Port::Source(port) }
    }
    /// If the location is a target.
    pub fn is_target(&self) -> bool { if let Port::Target(_) = self.port { true } else { false } }
    /// If the location is a source.
    pub fn is_source(&self) -> bool { if let Port::Source(_) = self.port { true } else { false } }
}

impl From<Target> for Location {
    fn from(target: Target) -> Self {
        Location {
            node: target.index,
            port: Port::Target(target.port),
        }
    }
}

impl From<Source> for Location {
    fn from(source: Source) -> Self {
        Location {
            node: source.index,
            port: Port::Source(source.port),
        }
    }
}

/// An operator port.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Abomonation, Serialize, Deserialize)]
pub enum Port {
    /// An operator input.
    Target(usize),
    /// An operator output.
    Source(usize),
}

/// Names a source of a data stream.
///
/// A source of data is either a child output, or an input from a parent.
/// Conventionally, `index` zero is used for parent input.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Debug)]
pub struct Source {
    /// Index of the source operator.
    pub index: usize,
    /// Number of the output port from the operator.
    pub port: usize,
}

/// Names a target of a data stream.
///
/// A target of data is either a child input, or an output to a parent.
/// Conventionally, `index` zero is used for parent output.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Debug)]
pub struct Target {
    /// Index of the target operator.
    pub index: usize,
    /// Number of the input port to the operator.
    pub port: usize,
}