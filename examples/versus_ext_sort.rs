// for using ext-sort crate with bincode2
mod ext_sort_chunk {
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

    #[derive(serde::Serialize, serde::Deserialize, deepsize::DeepSizeOf)]
    pub struct Item<K> {
        pub key: K,
        #[serde(with = "serde_bytes")]
        pub value: Vec<u8>,
    }
}

use ext_sort_chunk::{BincodeExternalChunk, Item};
use kv_extsort::{Result, SortConfig};
use log::{debug, error};
use rand::{rngs::SmallRng, Rng, SeedableRng};
use std::{convert::Infallible, time::Instant};

fn main() -> Result<(), Infallible> {
    env_logger::init();

    let merge_k = 16;
    let size = 2_000_000;
    let body_size = 2046;
    let concurrency = num_cpus::get();
    let max_chunk_size = 256 * 1024 * 1024;

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

    println!("ext-sort");
    {
        use ext_sort::{
            buffer::mem::MemoryLimitedBufferBuilder, ExternalSorter, ExternalSorterBuilder,
        };
        use std::path;
        let sorter: ExternalSorter<
            Item<i32>,
            Infallible,
            MemoryLimitedBufferBuilder,
            BincodeExternalChunk<_>,
        > = ExternalSorterBuilder::new()
            .with_rw_buf_size(1 << 20)
            .with_tmp_dir(path::Path::new("./"))
            .with_buffer(MemoryLimitedBufferBuilder::new(max_chunk_size as u64))
            .with_threads_number(num_cpus::get() + 1)
            .build()
            .unwrap();

        let t = Instant::now();
        let mut prev_key = None;
        let mut count = 0;
        for res in sorter
            .sort_by(
                make_iter().map(|(key, value)| Ok(Item { key, value })),
                |a, b| a.key.cmp(&b.key),
            )
            .unwrap()
        {
            match res {
                Ok(item) => {
                    // validate if sorted
                    count += 1;
                    if let Some(p) = prev_key {
                        assert!(p <= item.key);
                        prev_key = Some(item.key);
                    }
                }
                Err(e) => {
                    error!("{:?}", e);
                    break;
                }
            }
        }
        debug!("count: {count}");
        assert!(count == size, "count: {} != size: {}", count, size);
        debug!("Time elapsed: {:?}", t.elapsed());
    }

    println!("kv-extsort");
    {
        let t = Instant::now();
        let config = SortConfig::new()
            .max_chunk_bytes(max_chunk_size)
            .concurrency(concurrency)
            .merge_k(merge_k);

        let canceled = config.get_cancel_flag();

        ctrlc::set_handler(move || {
            canceled.store(true, std::sync::atomic::Ordering::Relaxed);
        })
        .expect("Error setting Ctrl-C handler");

        let mut prev_key = None;
        let mut count = 0;
        for res in kv_extsort::sort(make_iter().map(Result::<_, Infallible>::Ok), config) {
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
        debug!("count: {count}");
        assert!(count == size, "count: {} != size: {}", count, size);
        debug!("Time elapsed: {:?}", t.elapsed());
    }

    Ok(())
}
