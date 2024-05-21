use rand::seq::SliceRandom;
use std::time::Instant;

use external_sorter::{ExternalSorter, Result};

fn main() -> Result<()> {
    eprintln!("Preparing data...");
    let sorter = ExternalSorter::new();
    let mut source: Vec<(i32, Vec<u8>)> = (0..200000)
        .map(|v| {
            let s = vec![0; 5000];
            (v, s)
        })
        .collect();

    let mut rng = rand::thread_rng();
    source.shuffle(&mut rng);

    eprintln!("Sorting...");
    let t = Instant::now();
    for (i, res) in sorter.sort(source.into_iter()).enumerate() {
        match res {
            Ok((key, _value)) => {
                assert!(key == i as i32);
            }
            Err(e) => {
                println!("{:?}", e);
                break;
            }
        }
    }
    println!("Elapsed: {:?}", t.elapsed());

    Ok(())
}
