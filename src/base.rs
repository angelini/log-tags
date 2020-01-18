#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct DistinctId(pub usize);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct FileId(pub usize);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct FilterId(pub usize);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TagId(pub usize);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
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
}

pub trait Bounded {
    fn bounds(&self) -> Interval;
}
