use std::time::Instant;

use kv_extsort::{Result, SortConfig};
use log::{debug, error};
use rand::{rngs::SmallRng, Rng, SeedableRng};

fn main() -> Result<()> {
    env_logger::init();

    let mut rng = SmallRng::from_entropy();
    let size = 14_000_000;
    let body_size = 128;
    let merge_k = 9;
    let concurrency = num_cpus::get();
    let max_mem = (concurrency + 1) * 256_000_000;

    let source_iter = {
        type T = (i32, Vec<u8>);
        let source = (0..size).map(|_| {
            let u: i32 = rng.gen();
            let s = vec![0; body_size];
            (u, s)
        });

        let head_size = std::mem::size_of::<T>();
        let total_size = size * (head_size + body_size);
        debug!("Total size: {} MiB", total_size / 1024 / 1024);

        source
    };

    {
        let t = Instant::now();
        let config = SortConfig::new()
            .max_memory(max_mem)
            .concurrency(concurrency)
            .merge_k(merge_k);

        let mut prev_key = None;
        let mut count = 0;
        for res in kv_extsort::sort(source_iter, config) {
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
