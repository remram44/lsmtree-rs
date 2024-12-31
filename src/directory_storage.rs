use std::fs::File;
use std::io::{Error as IoError, ErrorKind as IoErrorKind, Write};
use std::path::PathBuf;
use crate::{Append, Storage};

pub struct DirectoryStorage {
    path: PathBuf,
}

pub struct DirectoryFileAppender(File);

impl Append for DirectoryFileAppender {
    fn append(&mut self, buffer: &[u8]) -> Result<(), IoError> {
        self.0.write_all(buffer)
    }
}

impl DirectoryStorage {
    pub fn new<P: Into<PathBuf>>(path: P) -> Result<DirectoryStorage, IoError> {
        let path: PathBuf = path.into();
        if !path.is_dir() {
            return Err(IoError::new(
                IoErrorKind::NotADirectory,
                "Not a directory",
            ));
        }
        Ok(DirectoryStorage { path })
    }
}

impl Storage for DirectoryStorage {
    type Reader = File;
    type Appender = DirectoryFileAppender;

    fn read(&self, key: &str) -> Result<File, IoError> {
        File::open(self.path.join(key))
    }

    fn write(&self, key: &str, value: &[u8]) -> Result<(), IoError> {
        std::fs::write(self.path.join(key), value)
    }

    fn append(&self, key: &str) -> Result<Self::Appender, IoError> {
        let file = File::options().create(true).write(true).open(self.path.join(key))?;
        Ok(DirectoryFileAppender(file))
    }

    fn delete(&self, key: &str) -> Result<(), IoError> {
        std::fs::remove_file(self.path.join(key))
    }

    fn list(&self) -> Result<Vec<String>, IoError> {
        let mut result = Vec::new();
        for entry in std::fs::read_dir(&self.path)? {
            let entry = entry?;
            let name = entry.file_name();
            if name == ".." || name == "." {
                continue;
            }
            if let Ok(name) = name.into_string() {
                result.push(name);
            } else {
                return Err(IoError::new(
                    IoErrorKind::InvalidData,
                    "Unexpected file in directory"
                ));
            }
        }
        Ok(result)
    }
}
