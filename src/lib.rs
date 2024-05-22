mod chunk;
mod sorter;

use thiserror::Error;

pub use sorter::{sort, SortConfig};

#[derive(Error, Debug)]
pub enum Error {
    #[error("An error occurred")]
    IO(#[from] std::io::Error),
    #[error("Canceled")]
    Canceled,
}

pub type Result<T> = std::result::Result<T, Error>;
