use log::{error, info};
use rand::{rngs::SmallRng, Rng, SeedableRng};
use std::time::Instant;

use kv_extsort::{Result, SortConfig};

fn main() -> Result<()> {
    env_logger::init();

    info!("{:?}", std::env::temp_dir());

    info!("Preparing data...");
    let source = {
        type T = (i32, Vec<u8>);
        let mut rng = SmallRng::from_entropy();
        let source: Vec<T> = (0..12_000_000)
            .map(|_| {
                let u: i32 = rng.gen();
                let s = vec![0; 256];
                (u, s)
            })
            .collect();

        let head_size = std::mem::size_of::<T>();
        let total_size = source
            .iter()
            .map(|(_, v)| head_size + v.len())
            .sum::<usize>();
        info!("Total size: {} MiB", total_size / 1024 / 1024);

        source
    };

    info!("Sorting...");
    {
        let t = Instant::now();
        const GB: usize = 1 << 30;
        let config = SortConfig::new().max_memory(GB).merge_k(16);
        for res in kv_extsort::sort(source.into_iter(), config) {
            match res {
                Ok((_key, _value)) => {
                    // ...
                }
                Err(e) => {
                    error!("{:?}", e);
                    break;
                }
            }
        }
        info!("Elapsed: {:?}", t.elapsed());
    }

    Ok(())
}
