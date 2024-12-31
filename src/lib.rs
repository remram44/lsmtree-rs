mod directory_storage;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Cursor, Error as IoError, ErrorKind as IoErrorKind, Read, Seek};
use tracing::info;

pub use directory_storage::DirectoryStorage;
// TODO: SingleFileStorage

pub enum Error {
    IoError(IoError),
    InvalidDatabase(String),
}

impl From<IoError> for Error {
    fn from(error: IoError) -> Error {
        Error::IoError(error)
    }
}

/// File-like trait to append to a file in storage, used for WAL.
pub trait Append {
    fn append(&mut self, buffer: &[u8]) -> Result<(), IoError>;
}

impl<A: Append> Append for &mut A {
    fn append(&mut self, buffer: &[u8]) -> Result<(), IoError> {
        (*self).append(buffer)
    }
}

pub trait Storage {
    type Reader: Read + Seek;
    type Appender: Append;

    fn read(&self, key: &str) -> Result<Self::Reader, IoError>;
    fn write(&self, key: &str, value: &[u8]) -> Result<(), IoError>;
    fn append(&self, key: &str) -> Result<Self::Appender, IoError>;
    fn delete(&self, key: &str) -> Result<(), IoError>;
    fn list(&self) -> Result<Vec<String>, IoError>;
}

#[derive(Default)]
struct MemTable {
    entries: Vec<(Vec<u8>, Vec<u8>)>,
}

impl MemTable {
    fn put(&mut self, key: &[u8], value: Vec<u8>) {
        match self.entries.binary_search_by_key(&key, |(key, _value)| key) {
            Ok(index) => {
                // There is an element with that key, update its value
                self.entries[index].1 = value;
            }
            Err(index) => {
                // There is no element with that key, insert
                self.entries.insert(index, (key.into(), value));
            }
        }
    }

    fn delete(&mut self, key: &[u8]) -> bool {
        match self.entries.binary_search_by_key(&key, |(key, _value)| key) {
            Ok(index) => {
                // There is an element with that key, update its value
                self.entries.remove(index);
                true
            }
            Err(_) => false,
        }
    }

    fn get(&self, key: &[u8]) -> Option<&[u8]> {
        match self.entries.binary_search_by_key(&key, |(key, _value)| key) {
            Ok(index) => Some(&self.entries[index].1),
            Err(_) => None,
        }
    }

    fn iter_range<'a>(&'a self, key_start: &'a [u8], key_end: &'a [u8]) -> MemTableRangeIterator<'a> {
        let index = self.entries.partition_point(|(key, _value)| key as &[u8] < key_start);
        MemTableRangeIterator {
            mem_table: self,
            next_index: index,
            key_end,
        }
    }
}

struct MemTableRangeIterator<'a> {
    mem_table: &'a MemTable,
    next_index: usize,
    key_end: &'a [u8],
}

impl<'a> Iterator for MemTableRangeIterator<'a> {
    type Item = &'a (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.next_index >= self.mem_table.entries.len() {
            None
        } else {
            let entry = &self.mem_table.entries[self.next_index];
            if &entry.0 as &[u8] < self.key_end {
                self.next_index += 1;
                Some(entry)
            } else {
                self.next_index = self.mem_table.entries.len();
                None
            }
        }
    }
}

pub struct Database<S: Storage> {
    storage: S,
    mem_table: MemTable,
    wal: S::Appender,
}

fn read_vec<R: Read>(mut file: R) -> Result<Vec<u8>, IoError> {
    let len = file.read_u32::<BigEndian>()?;
    let mut vec = vec![0u8; len as usize];
    file.read_exact(&mut vec)?;
    Ok(vec)
}

fn write_vec<A: Append>(mut file: A, buf: &[u8]) -> Result<(), IoError> {
    let mut len = [0u8; 4];
    Cursor::new(&mut len as &mut [u8]).write_u32::<BigEndian>(buf.len() as u32)?;
    file.append(&len)?;
    file.append(buf)?;
    Ok(())
}

impl<S: Storage> Database<S> {
    pub fn open(storage: S) -> Result<Database<S>, Error> {
        let mut wal_found = false;
        let mut tables_found = false;
        for entry in storage.list()? {
            if &entry == "wal" {
                wal_found = true;
            } else if entry.ends_with(".sst") {
                tables_found = true;
            } else {
                return Err(Error::InvalidDatabase("Unexpected file in storage".into()));
            }
        }

        let mut mem_table: MemTable = Default::default();

        if !wal_found && tables_found {
            return Err(Error::InvalidDatabase("Missing wal".into()));
        } else if !wal_found {
            // Initialize new empty database
            info!("Opening empty database");
        } else {
            // Open existing database
            info!("Opening existing database, replaying WAL");
            let mut entries = 0;
            let mut wal = storage.read("wal")?;
            loop {
                let op = match wal.read_u8() {
                    Ok(0) => Operation::Put,
                    Ok(1) => Operation::Delete,
                    Ok(_) => return Err(Error::InvalidDatabase("Invalid WAL entry type".into())),
                    Err(e) if e.kind() == IoErrorKind::UnexpectedEof => break,
                    Err(e) => return Err(e.into()),
                };
                let key = read_vec(&mut wal)?;
                match op {
                    Operation::Put => {
                        let value = read_vec(&mut wal)?;
                        mem_table.put(&key, value);
                    }
                    Operation::Delete => {
                        mem_table.delete(&key);
                    }
                }
                entries += 1;
            }
            info!("Replayed {} WAL entries", entries);
        }
        let wal = storage.append("wal")?;
        Ok(Database {
            storage,
            mem_table,
            wal,
        })
    }

    pub fn put(&mut self, key: &[u8], value: &[u8]) -> Result<(), IoError> {
        // Write to WAL
        self.wal.append(&[0u8])?;
        write_vec(&mut self.wal, key)?;
        write_vec(&mut self.wal, value)?;

        // Update memtable
        self.mem_table.put(key, value.into());

        Ok(())
    }

    pub fn get(&mut self, key: &[u8]) -> Result<Option<Vec<u8>>, IoError> {
        // Read from mem table
        if let Some(value) = self.mem_table.get(key) {
            return Ok(Some(value.into()));
        }

        // TODO: Read from sstables

        Ok(None)
    }

    pub fn delete(&mut self, key: &[u8]) -> Result<(), IoError> {
        // Write to WAL
        self.wal.append(&[1u8])?;
        write_vec(&mut self.wal, key)?;

        // Update memtable
        self.mem_table.delete(key);

        Ok(())
    }

    pub fn iter_range(&mut self, key_start: &[u8], key_end: &[u8]) -> RangeIterator<S> {
        RangeIterator {
            database: self,
        }
    }
}

pub struct RangeIterator<'a, S: Storage> {
    database: &'a mut Database<S>,
}

impl<'a, S: Storage> Iterator for RangeIterator<'a, S> {
    type Item = (Vec<u8>, Vec<u8>);

    fn next(&mut self) -> Option<(Vec<u8>, Vec<u8>)> {
        todo!()
    }
}

enum Operation {
    Put,
    Delete,
}
