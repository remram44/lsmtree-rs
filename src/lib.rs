mod directory_storage;
mod mem_table;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::collections::HashSet;
use std::io::{Cursor, Error as IoError, ErrorKind as IoErrorKind, Write};
use tracing::info;

pub use directory_storage::DirectoryStorage;
use mem_table::MemTable;
// TODO: SingleFileStorage

#[derive(Debug)]
pub enum Error {
    IoError(IoError),
    InvalidDatabase(String),
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Error::IoError(err) => write!(f, "I/O error: {}", err),
            Error::InvalidDatabase(msg) => write!(f, "{}", msg),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::IoError(err) => Some(err),
            Error::InvalidDatabase(_) => None,
        }
    }
}

impl From<IoError> for Error {
    fn from(error: IoError) -> Error {
        Error::IoError(error)
    }
}

/// File-like trait to append to a file in storage, used for WAL.
pub trait Append {
    fn append(&mut self, buffer: &[u8]) -> Result<(), IoError>;
    fn truncate(&mut self) -> Result<(), IoError>;
}

impl<A: Append> Append for &mut A {
    fn append(&mut self, buffer: &[u8]) -> Result<(), IoError> {
        (*self).append(buffer)
    }

    fn truncate(&mut self) -> Result<(), IoError> {
        (*self).truncate()
    }
}

pub trait ReadAt {
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), IoError>;
}

impl<R: ReadAt> ReadAt for &R {
    fn read_exact_at(&self, buf: &mut [u8], offset: u64) -> Result<(), IoError> {
        (*self).read_exact_at(buf, offset)
    }
}

pub trait Storage {
    type Reader: ReadAt;
    type Appender: Append;

    fn read(&self, key: &str) -> Result<Self::Reader, IoError>;
    fn write(&self, key: &str, value: &[u8]) -> Result<(), IoError>;
    fn append(&self, key: &str) -> Result<Self::Appender, IoError>;
    fn delete(&self, key: &str) -> Result<(), IoError>;
    fn list(&self) -> Result<Vec<String>, IoError>;
}

struct SSTableReader<R: ReadAt> {
    file: R,
    size: usize,
}

impl<R: ReadAt> SSTableReader<R> {
    fn open(file: R) -> Result<SSTableReader<R>, IoError> {
        let mut size_buf = [0u8; 4];
        file.read_exact_at(&mut size_buf, 0)?;
        let size = Cursor::new(&size_buf).read_u32::<BigEndian>()? as usize;
        Ok(SSTableReader {
            file,
            size,
        })
    }

    fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, IoError> {
        // Binary search
        let mut size = self.size;
        if size == 0 {
            return Ok(None);
        }
        let mut base = 0;

        let section_index = 4;
        let section_entries = 4 + self.size as u64 * 8;

        while size > 1 {
            let half = size / 2;
            let mid_index = base + half;

            let mut mid_offset_buf = [0u8; 8];
            self.file.read_exact_at(
                &mut mid_offset_buf,
                section_index + mid_index as u64 * 8,
            )?;
            let mid_offset = Cursor::new(&mid_offset_buf).read_u64::<BigEndian>().unwrap();

            let mut mid_key_len_buf = [0u8; 4];
            self.file.read_exact_at(
                &mut mid_key_len_buf,
                section_entries + mid_offset,
            )?;
            let mid_key_len = Cursor::new(&mid_key_len_buf).read_u32::<BigEndian>().unwrap();

            let mut mid = vec![0u8; mid_key_len as usize];
            self.file.read_exact_at(
                &mut mid,
                section_entries + mid_offset + 4,
            )?;

            if &mid as &[u8] == key {
                let mut value_len_buf = [0u8; 4];
                self.file.read_exact_at(
                    &mut value_len_buf,
                    section_entries + mid_offset + 4 + mid_key_len as u64,
                )?;
                let value_len = Cursor::new(&value_len_buf).read_u32::<BigEndian>().unwrap();

                let mut value = vec![0u8; value_len as usize];
                self.file.read_exact_at(
                    &mut value,
                    section_entries + mid_offset + 4 + mid_key_len as u64 + 4,
                )?;
                return Ok(Some(value));
            } else if &mid as &[u8] < key {
                base = mid_index;
            }

            size -= half;
        }

        Ok(None)
    }
}

fn write_sstable(entries: &[(Vec<u8>, Vec<u8>)]) -> Vec<u8> {
    let mut result = Cursor::new(Vec::new());
    result.write_u32::<BigEndian>(entries.len() as u32).unwrap();
    let mut offset = 0;
    for entry in entries {
        result.write_u64::<BigEndian>(offset).unwrap();
        offset += 4 + entry.0.len() as u64 + 4 + entry.1.len() as u64;
    }
    for entry in entries {
        result.write_u32::<BigEndian>(entry.0.len() as u32).unwrap();
        result.write_all(&entry.0).unwrap();
        result.write_u32::<BigEndian>(entry.1.len() as u32).unwrap();
        result.write_all(&entry.1).unwrap();
    }
    result.into_inner()
}

pub struct Database<S: Storage> {
    storage: S,
    sstables: Vec<((u32, u32), SSTableReader<S::Reader>)>,
    mem_table: MemTable,
    wal: S::Appender,
}

fn read_vec<R: ReadAt>(file: R, offset: &mut u64) -> Result<Vec<u8>, IoError> {
    let mut len_buf = [0u8; 4];
    file.read_exact_at(&mut len_buf, *offset)?;
    *offset += 4;
    let len = Cursor::new(&len_buf).read_u32::<BigEndian>().unwrap();
    let mut vec = vec![0u8; len as usize];
    file.read_exact_at(&mut vec, *offset)?;
    *offset += len as u64;
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
        let mut sstable_names = Vec::new();
        for entry in storage.list()? {
            if &entry == "wal" {
                wal_found = true;
            } else if entry.ends_with(".sst") {
                sstable_names.push(entry);
            } else {
                return Err(Error::InvalidDatabase("Unexpected file in storage".into()));
            }
        }

        let mut mem_table: MemTable = Default::default();
        let mut sstables = Vec::new();

        if !wal_found && sstable_names.len() > 0 {
            return Err(Error::InvalidDatabase("Missing wal".into()));
        } else if !wal_found {
            // Initialize new empty database
            info!("Opening empty database");
        } else {
            // Open existing database
            info!("Opening existing database, replaying WAL");
            let mut entries = 0;
            let mut incomplete_sstables = HashSet::new();
            let wal = storage.read("wal")?;
            let mut offset = 0;
            loop {
                let mut op_buf = [0u8];
                let op = match wal.read_exact_at(&mut op_buf, offset) {
                    Err(e) if e.kind() == IoErrorKind::UnexpectedEof => break,
                    Err(e) => return Err(e.into()),
                    Ok(()) => match op_buf[0] {
                        0 => Operation::Put,
                        1 => Operation::Delete,
                        2 => Operation::WriteSstableStart,
                        3 => Operation::WriteSstableEnd,
                        _ => return Err(Error::InvalidDatabase("Invalid WAL entry type".into())),
                    }
                };
                offset += 1;
                let key = read_vec(&wal, &mut offset)?;
                match op {
                    Operation::Put => {
                        let value = read_vec(&wal, &mut offset)?;
                        mem_table.put(&key, value);
                    }
                    Operation::Delete => {
                        mem_table.delete(&key);
                    }
                    Operation::WriteSstableStart => {
                        let table_name = read_vec(&wal, &mut offset)?;
                        let table_name = String::from_utf8(table_name)
                            .map_err(|_| ())
                            .and_then(|n| if n.is_ascii() { Ok(n) } else { Err(()) })
                            .map_err(|_| Error::InvalidDatabase("Invalid table name in WAL".into()))?;
                        incomplete_sstables.insert(table_name);
                    }
                    Operation::WriteSstableEnd => {
                        let table_name = read_vec(&wal, &mut offset)?;
                        let table_name = String::from_utf8(table_name)
                            .map_err(|_| ())
                            .and_then(|n| if n.is_ascii() { Ok(n) } else { Err(()) })
                            .map_err(|_| Error::InvalidDatabase("Invalid table name in WAL".into()))?;
                        incomplete_sstables.remove(&table_name);
                    }
                }
                entries += 1;
            }

            // Remove incomplete sstables
            info!("{} incomplete sstables to delete", incomplete_sstables.len());
            for sstable in &incomplete_sstables {
                storage.delete(&sstable)?;
            }

            // Open remaining sstables
            for name in sstable_names {
                if !incomplete_sstables.contains(&name) {
                    let reader = storage.read(&name)?;
                    let table = SSTableReader::open(reader)?;
                    let id = parse_sstable_name(&name).map_err(|_| Error::InvalidDatabase("Invalid sstable name".into()))?;
                    sstables.push((id, table));
                }
            }

            info!("Replayed {} WAL entries", entries);
        }
        let wal = storage.append("wal")?;
        Ok(Database {
            storage,
            sstables,
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

        // Read from sstables
        for (_, sstable) in self.sstables.iter().rev() {
            if let Some(value) = sstable.get(key)? {
                return Ok(Some(value));
            }
        }

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

    pub fn maintain(&mut self) -> Result<(), IoError> {
        // TODO: Merge tables

        // Write memtable to disk
        let mut new_id = 0;
        for &((level, id), _) in &self.sstables {
            if level == 0 {
                if id >= new_id {
                    new_id = id + 1;
                }
            }
        }
        let new_name = format!("1-{}.sst", new_id);
        info!("Writing memtable to new sstable '{}'", new_name);

        self.wal.append(&[2])?;
        write_vec(&mut self.wal, new_name.as_bytes())?;

        let buf = write_sstable(&self.mem_table.entries);
        self.storage.write(&new_name, &buf)?;

        self.wal.append(&[3])?;
        write_vec(&mut self.wal, new_name.as_bytes())?;
        info!("New sstable write complete");

        // Open new memtable
        let reader = self.storage.read(&new_name)?;
        let table = SSTableReader::open(reader)?;
        let index = self.sstables.partition_point(|&(k, _)| k > (1, new_id));
        self.sstables.insert(index, ((1, new_id), table));

        // Truncate WAL
        info!("Truncating WAL");
        self.wal.truncate()?;

        Ok(())
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
    WriteSstableStart,
    WriteSstableEnd,
}

fn parse_sstable_name(name: &str) -> Result<(u32, u32), ()> {
    let Some(dash) = name.find('-') else {
        return Err(());
    };
    let level = name[0..dash].parse().map_err(|_| ())?;
    let dot = match name[dash+1..].find('.') {
        Some(i) => dash + 1 + i,
        None => return Err(()),
    };
    let id = name[dash+1..dot].parse().map_err(|_| ())?;
    if &name[dot..] != ".sst" {
        return Err(());
    }
    Ok((level, id))
}

#[test]
fn test_parse_sstable_name() {
    assert_eq!(parse_sstable_name("1-0.sst"), Ok((1, 0)));
    assert_eq!(parse_sstable_name("123-456.sst"), Ok((123, 456)));
    assert_eq!(parse_sstable_name(""), Err(()));
    assert_eq!(parse_sstable_name("-0.sst"), Err(()));
    assert_eq!(parse_sstable_name("1-.sst"), Err(()));
    assert_eq!(parse_sstable_name("1-0."), Err(()));
    assert_eq!(parse_sstable_name("1-0"), Err(()));
}

#[cfg(test)]
mod tests {
    use tempdir::TempDir;

    use crate::{Database, DirectoryStorage};

    fn v(s: &[u8]) -> Vec<u8> {
        s.into()
    }

    #[test]
    fn test_database() {
        let dir = TempDir::new("lsmtree-test").unwrap();
        let storage = DirectoryStorage::new(dir.path()).unwrap();
        let mut db = Database::open(storage).unwrap();

        fn check(db: &mut Database<DirectoryStorage>) {
            db.put(b"ghi", b"111").unwrap();
            db.put(b"abc", b"222").unwrap();
            db.put(b"mno", b"333").unwrap();
            db.put(b"ghi", b"444").unwrap();
            db.put(b"def", b"555").unwrap();
            db.put(b"jkl", b"666").unwrap();
            db.put(b"def", b"777").unwrap();
            db.delete(b"ghi").unwrap();
        }
        check(&mut db);

        db.maintain().unwrap();
        check(&mut db);

        assert_eq!(db.get(b"abc").unwrap(), Some(v(b"222")));
        assert_eq!(db.get(b"def").unwrap(), Some(v(b"777")));
        assert_eq!(db.get(b"ghi").unwrap(), None);
        assert_eq!(db.get(b"jkl").unwrap(), Some(v(b"666")));
        assert_eq!(db.get(b"mno").unwrap(), Some(v(b"333")));
        assert_eq!(db.get(b"zzz").unwrap(), None);

        assert_eq!(
            db.iter_range(b"def", b"jkl").collect::<Vec<_>>(),
            vec![
                (v(b"def"), v(b"777")),
            ],
        );

        assert_eq!(
            db.iter_range(b"a", b"jz").collect::<Vec<_>>(),
            vec![
                (v(b"abc"), v(b"222")),
                (v(b"def"), v(b"777")),
                (v(b"jkl"), v(b"666")),
            ],
        );

        assert_eq!(
            db.iter_range(b"def", b"z").collect::<Vec<_>>(),
            vec![
                (v(b"def"), v(b"777")),
                (v(b"jkl"), v(b"666")),
                (v(b"mno"), v(b"333")),
            ],
        );
    }
}
