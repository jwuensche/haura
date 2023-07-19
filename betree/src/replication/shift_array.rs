#[derive(Debug)]
pub struct ShiftArray<T, const A: usize> {
    count: usize,
    arr: [Option<T>; A],
}

impl<T: PartialOrd, const A: usize> ShiftArray<T, A> {
    /// In an ordered array find the index of the next largest element.
    pub fn find(&self, v: &T) -> Option<usize> {
        for idx in 0..self.count {
            if self.arr[idx].as_ref().unwrap() >= v {
                return Some(idx);
            }
        }
        Some(self.count)
    }
}

impl<T, const A: usize> ShiftArray<T, A> {
    pub fn new() -> Self {
        Self {
            arr: [0u8; A].map(|_| None),
            count: 0,
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = &T> {
        self.arr.iter().filter_map(|e| e.as_ref())
    }

    pub fn split_after(&mut self, idx: usize) -> ShiftArray<T, A> {
        assert!(idx < self.count);
        let mut other = Self::new();
        for cur in (idx + 1)..self.count {
            other.push_back(self.arr[cur].take().unwrap());
        }
        self.count = idx + 1;
        other
    }

    pub fn push_back(&mut self, val: T) {
        // Full
        assert!(self.count < A);
        self.arr[self.count] = Some(val);
        self.count += 1;
    }

    pub fn push_front(&mut self, val: T) {
        self.insert(0, val)
    }

    pub fn get(&self, idx: usize) -> Option<&T> {
        self.arr[idx].as_ref()
    }

    pub fn get_mut(&mut self, idx: usize) -> Option<&mut T> {
        self.arr[idx].as_mut()
    }

    pub fn pop_back(&mut self) -> Option<T> {
        self.remove(self.count - 1)
    }

    pub fn pop_front(&mut self) -> Option<T> {
        self.remove(0)
    }

    pub fn last(&self) -> Option<&T> {
        self.arr.get(self.count.saturating_sub(1)).unwrap().as_ref()
    }

    pub fn first(&self) -> Option<&T> {
        self.arr[0].as_ref()
    }

    pub fn last_mut(&mut self) -> Option<&mut T> {
        self.arr
            .get_mut(self.count.saturating_sub(1))
            .unwrap()
            .as_mut()
    }

    pub fn insert(&mut self, idx: usize, val: T) {
        assert!(self.count < A);
        let mut tmp = Some(val);
        for cur in idx..A {
            std::mem::swap(&mut tmp, &mut self.arr[cur])
        }
        self.count += 1;
    }

    pub fn remove(&mut self, idx: usize) -> Option<T> {
        let val = self.arr[idx].take();
        // Skip last entry
        for cur in idx..A - 1 {
            self.arr[cur] = self.arr[cur + 1].take()
        }
        self.count -= 1;
        val
    }

    pub fn size(&self) -> usize {
        self.count
    }
}
