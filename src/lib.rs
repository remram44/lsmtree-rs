mod directory_storage;
mod mem_table;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use std::io::{Cursor, Error as IoError, ErrorKind as IoErrorKind, Read};
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
}

impl<A: Append> Append for &mut A {
    fn append(&mut self, buffer: &[u8]) -> Result<(), IoError> {
        (*self).append(buffer)
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

pub struct Database<S: Storage> {
    storage: S,
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

    pub fn maintain(&mut self) -> Result<(), IoError> {
        // TODO: Merge tables
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
