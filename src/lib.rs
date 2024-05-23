mod chunk;
mod sorter;

pub use sorter::{sort, SortConfig};
use thiserror::Error;

#[derive(Error, Debug)]
pub enum Error {
    #[error("An error occurred")]
    IO(#[from] std::io::Error),
    #[error("Canceled")]
    Canceled,
}

pub type Result<T> = std::result::Result<T, Error>;
