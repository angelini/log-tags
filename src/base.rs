use std::fmt;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct DistinctId(pub usize);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FileId(pub usize);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct FilterId(pub usize);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TagId(pub usize);

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum Id {
    Distinct(DistinctId),
    File(FileId),
    Filter(FilterId),
    Tag(TagId),
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Comparator {
    Equal,
    NotEqual,
    GreaterThan,
    GreaterThanEqual,
    LessThan,
    LessThanEqual,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Aggregator {

}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
/// A Closed-Open Interval
pub struct Interval(pub usize, pub usize);

impl Interval {
    pub fn is_empty(&self) -> bool {
        self.0 == self.1
    }

    pub fn len(&self) -> usize {
        self.1 - self.0
    }

    pub fn contains(&self, other: Interval) -> bool {
        other.is_empty()
            || (self.missing_before(other).is_empty() && self.missing_after(other).is_empty())
    }

    pub fn missing_before(&self, other: Interval) -> Interval {
        if other.0 < self.0 {
            Interval(other.0, self.0)
        } else {
            Interval(self.0, self.0)
        }
    }

    pub fn missing_after(&self, other: Interval) -> Interval {
        if other.1 > self.1 {
            Interval(self.1, other.1)
        } else {
            Interval(self.1, self.1)
        }
    }

    pub fn iter(&self) -> IntervalIter {
        IntervalIter {
            index: self.0,
            interval: *self,
        }
    }
}

pub struct IntervalIter {
    index: usize,
    interval: Interval,
}

impl Iterator for IntervalIter {
    type Item = usize;

    fn next(&mut self) -> Option<Self::Item> {
        if self.index == self.interval.1 {
            return None
        }

        let result = self.index;
        self.index += 1;
        Some(result)
    }
}

impl fmt::Display for Interval {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}, {})", self.0, self.1)
    }
}
