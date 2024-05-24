use std::{
    fs::File,
    io::{BufReader, BufWriter, Read, Write},
    marker::PhantomData,
    path::{Path, PathBuf},
    sync::atomic::{self, AtomicUsize},
};

use bytemuck::Pod;
use tempfile::{tempdir, TempDir};

use crate::Result;

pub struct MemChunk<K>(Vec<(K, Vec<u8>)>);

impl<K> MemChunk<K> {
    pub fn from_unsorted(mut data: Vec<(K, Vec<u8>)>) -> MemChunk<K>
    where
        K: Ord + Copy,
    {
        data.sort_unstable_by_key(|(key, _value)| *key);
        MemChunk(data)
    }

    pub fn len(&self) -> usize {
        self.0.len()
    }

    pub fn write_to_file<E>(&self, writer: &mut FileChunkWriter<K>) -> Result<(), E>
    where
        K: Ord + Pod,
    {
        for (key, value) in &self.0 {
            writer.push(key, value)?;
        }
        Ok(())
    }
}

impl<K> IntoIterator for MemChunk<K> {
    type Item = (K, Vec<u8>);
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl<'a, K> IntoIterator for &'a MemChunk<K> {
    type Item = &'a (K, Vec<u8>);
    type IntoIter = std::slice::Iter<'a, (K, Vec<u8>)>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

pub struct FileChunkDir<K>
where
    K: Pod,
{
    temp_dir: TempDir,
    count: AtomicUsize,
    key_type: PhantomData<K>,
    buf_writer_capacity: usize,
}

impl<K> FileChunkDir<K>
where
    K: Pod,
{
    pub fn new<E>() -> Result<Self, E> {
        Ok(Self {
            temp_dir: tempdir()?,
            count: AtomicUsize::new(0),
            key_type: PhantomData,
            buf_writer_capacity: 1 << 20,
        })
    }

    pub fn add_chunk<E>(&self) -> Result<FileChunkWriter<K>, E> {
        let path = self.temp_dir.path().join(format!(
            "{}",
            self.count.fetch_add(1, atomic::Ordering::Relaxed)
        ));

        FileChunkWriter::new(path, self.buf_writer_capacity)
    }
}

pub struct FileChunkWriter<K>
where
    K: Pod,
{
    path: PathBuf,
    writer: BufWriter<File>,
    count: usize,
    key_type: PhantomData<K>,
}

impl<K> FileChunkWriter<K>
where
    K: Pod,
{
    pub fn new<E>(path: PathBuf, capacity: usize) -> Result<Self, E> {
        let writer = BufWriter::with_capacity(capacity, File::create(&path)?);
        Ok(Self {
            path,
            writer,
            count: 0,
            key_type: PhantomData,
        })
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn push<E>(&mut self, key: &K, value: &[u8]) -> Result<(), E> {
        let key_bin = bytemuck::bytes_of(key);
        self.writer.write_all(key_bin)?;
        self.writer.write_all(&(value.len() as u32).to_ne_bytes())?;
        self.writer.write_all(value)?;
        self.count += 1;
        Ok(())
    }

    pub fn finalize(self) -> FileChunk<K> {
        FileChunk::new(self.path, self.count)
    }
}

pub struct FileChunk<K>
where
    K: Pod,
{
    path: PathBuf,
    key_type: PhantomData<K>,
    count: usize,
}

impl<K> FileChunk<K>
where
    K: Pod,
{
    pub fn new(path: PathBuf, count: usize) -> Self {
        Self {
            path,
            key_type: PhantomData,
            count,
        }
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn iter<E>(&self) -> Result<FileChunkIter<K, E>, E> {
        let file = File::open(&self.path)?;
        let reader = BufReader::with_capacity(1 << 20, file);
        Ok(FileChunkIter {
            reader,
            key_type: PhantomData,
            error_type: PhantomData,
        })
    }

    pub fn len(&self) -> usize {
        self.count
    }
}

pub struct FileChunkIter<K, E>
where
    K: Pod,
{
    reader: BufReader<File>,
    key_type: PhantomData<K>,
    error_type: PhantomData<E>,
}

impl<K, E> Iterator for FileChunkIter<K, E>
where
    K: Pod,
{
    type Item = Result<(K, Vec<u8>), E>;

    fn next(&mut self) -> Option<Self::Item> {
        let mut key = K::zeroed();
        match self.reader.read_exact(bytemuck::bytes_of_mut(&mut key)) {
            Ok(_) => {}
            Err(e) => {
                if e.kind() == std::io::ErrorKind::UnexpectedEof {
                    return None;
                } else {
                    return Some(Err(e.into()));
                }
            }
        };

        let mut read = || {
            let val_size = {
                let mut buf = [0u8; 4];
                self.reader.read_exact(&mut buf)?;
                u32::from_ne_bytes(buf) as usize
            };
            let mut value: Vec<u8> = vec![0; val_size];
            self.reader.read_exact(&mut value)?;
            Ok((key, value))
        };
        Some(read())
    }
}
