use std::{convert::Infallible, time::Instant};

use kv_extsort::{Result, SortConfig};
use log::{debug, error};
use rand::{rngs::SmallRng, Rng, SeedableRng};

mod sort {
    use std::{fs, io, marker::PhantomData};

    use ext_sort::chunk::ExternalChunk;

    pub struct BincodeExternalChunk<T> {
        reader: io::Take<io::BufReader<fs::File>>,
        item_type: PhantomData<T>,
    }

    impl<T> ExternalChunk<T> for BincodeExternalChunk<T>
    where
        T: serde::ser::Serialize + serde::de::DeserializeOwned,
    {
        type SerializationError = bincode::error::EncodeError;
        type DeserializationError = bincode::error::DecodeError;

        fn new(reader: io::Take<io::BufReader<fs::File>>) -> Self {
            Self {
                reader,
                item_type: PhantomData,
            }
        }

        fn dump(
            mut chunk_writer: &mut io::BufWriter<fs::File>,
            items: impl IntoIterator<Item = T>,
        ) -> Result<(), Self::SerializationError> {
            let bincode_config = bincode::config::standard();
            for item in items.into_iter() {
                bincode::serde::encode_into_std_write(&item, &mut chunk_writer, bincode_config)?;
            }
            Ok(())
        }
    }

    impl<T> Iterator for BincodeExternalChunk<T>
    where
        T: serde::ser::Serialize + serde::de::DeserializeOwned,
    {
        type Item = Result<T, <Self as ExternalChunk<T>>::DeserializationError>;

        fn next(&mut self) -> Option<Self::Item> {
            let bincode_config = bincode::config::standard();
            if self.reader.limit() == 0 {
                None
            } else {
                match bincode::serde::decode_from_std_read(&mut self.reader, bincode_config) {
                    Ok(result) => Some(Ok(result)),
                    Err(err) => Some(Err(err)),
                }
            }
        }
    }
}

use sort::BincodeExternalChunk;

fn main() -> Result<()> {
    env_logger::init();

    let merge_k = 12;
    let size = 14_000_000;
    let body_size = 128;
    let concurrency = num_cpus::get();
    let max_mem = (concurrency + 1) * 256 * 1024 * 1024;

    let make_iter = || {
        let mut rng = SmallRng::from_entropy();
        type T = (i32, Vec<u8>);
        let source = (0..size).map(move |_| {
            let u: i32 = rng.gen();
            let s = vec![0; body_size];
            (u, s)
        });

        let head_size = std::mem::size_of::<T>();
        let total_size = size * (head_size + body_size);
        debug!("Total size: {} MiB", total_size / 1024 / 1024);

        source
    };

    // use ext_sort::{
    //     buffer::mem::MemoryLimitedBufferBuilder, ExternalSorter, ExternalSorterBuilder,
    // };
    // use std::path;
    // let sorter: ExternalSorter<
    //     (i32, Vec<u8>),
    //     Infallible,
    //     MemoryLimitedBufferBuilder,
    //     BincodeExternalChunk<_>,
    // > = ExternalSorterBuilder::new()
    //     .with_tmp_dir(path::Path::new("./"))
    //     .with_buffer(MemoryLimitedBufferBuilder::new(256 * 1024 * 1024))
    //     .with_threads_number(num_cpus::get())
    //     .build()
    //     .unwrap();

    // println!("ext-sort");
    // let t = Instant::now();
    // sorter
    //     .sort_by(make_iter().map(Ok), |a, b| a.0.cmp(&b.0))
    //     .unwrap();
    // println!("ext-sort: {:?}", t.elapsed());

    {
        let t = Instant::now();
        let config = SortConfig::new()
            .max_memory(max_mem)
            .concurrency(concurrency)
            .merge_k(merge_k);

        let mut prev_key = None;
        let mut count = 0;
        for res in kv_extsort::sort(make_iter(), config) {
            match res {
                Ok((_key, _value)) => {
                    // validate if sorted
                    count += 1;
                    if let Some(p) = prev_key {
                        assert!(p <= _key);
                        prev_key = Some(_key);
                    }
                }
                Err(e) => {
                    error!("{:?}", e);
                    break;
                }
            }
        }
        assert!(count == size, "count: {} != size: {}", count, size);
        debug!("Time elapsed: {:?}", t.elapsed());
    }

    Ok(())
}
