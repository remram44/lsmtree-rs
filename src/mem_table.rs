#[derive(Default)]
pub(crate) struct MemTable {
    pub(crate) entries: Vec<(Vec<u8>, Vec<u8>)>,
}

impl MemTable {
    pub(crate) fn put(&mut self, key: &[u8], value: Vec<u8>) {
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

    pub(crate) fn delete(&mut self, key: &[u8]) -> bool {
        match self.entries.binary_search_by_key(&key, |(key, _value)| key) {
            Ok(index) => {
                // There is an element with that key, update its value
                self.entries.remove(index);
                true
            }
            Err(_) => false,
        }
    }

    pub(crate) fn get(&self, key: &[u8]) -> Option<&[u8]> {
        match self.entries.binary_search_by_key(&key, |(key, _value)| key) {
            Ok(index) => Some(&self.entries[index].1),
            Err(_) => None,
        }
    }

    pub(crate) fn iter_range<'a>(&'a self, key_start: &'a [u8], key_end: &'a [u8]) -> MemTableRangeIterator<'a> {
        let index = self.entries.partition_point(|(key, _value)| key as &[u8] < key_start);
        MemTableRangeIterator {
            mem_table: self,
            next_index: index,
            key_end,
        }
    }
}

pub(crate) struct MemTableRangeIterator<'a> {
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

#[cfg(test)]
mod tests {
    use super::MemTable;

    fn v(s: &[u8]) -> Vec<u8> {
        s.into()
    }

    #[test]
    fn test_memtable() {
        let mut mem_table: MemTable = Default::default();
        assert_eq!(mem_table.entries, vec![]);
        mem_table.put(b"ghi", v(b"111"));
        mem_table.put(b"abc", v(b"222"));
        mem_table.put(b"mno", v(b"333"));
        mem_table.put(b"ghi", v(b"444"));
        mem_table.put(b"def", v(b"555"));
        mem_table.put(b"jkl", v(b"666"));
        mem_table.put(b"def", v(b"777"));
        mem_table.delete(b"ghi");
        assert_eq!(mem_table.entries, vec![
            (v(b"abc"), v(b"222")),
            (v(b"def"), v(b"777")),
            (v(b"jkl"), v(b"666")),
            (v(b"mno"), v(b"333")),
        ]);

        assert_eq!(
            mem_table.iter_range(b"def", b"jkl").collect::<Vec<_>>(),
            vec![
                &(v(b"def"), v(b"777")),
            ],
        );

        assert_eq!(
            mem_table.iter_range(b"a", b"jz").collect::<Vec<_>>(),
            vec![
                &(v(b"abc"), v(b"222")),
                &(v(b"def"), v(b"777")),
                &(v(b"jkl"), v(b"666")),
            ],
        );

        assert_eq!(
            mem_table.iter_range(b"def", b"z").collect::<Vec<_>>(),
            vec![
                &(v(b"def"), v(b"777")),
                &(v(b"jkl"), v(b"666")),
                &(v(b"mno"), v(b"333")),
            ],
        );
    }
}
