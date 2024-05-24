mod chunk;
mod merge;
mod sorter;

pub use sorter::{sort, SortConfig};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error<E> {
    #[error("An error occurred")]
    IO(#[from] std::io::Error),
    #[error("Canceled")]
    Canceled,
    #[error("Source error")]
    Source(E),
}

pub type Result<T, E> = std::result::Result<T, Error<E>>;
