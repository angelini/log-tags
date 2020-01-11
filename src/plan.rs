// use std::collections::HashMap;
// use std::fs;

// use regex::Regex;

// use crate::base::{Comparator, FileId, FilterId, Id, Interval, TagId};
// use crate::error::Result;


// struct Scope {
//     files: HashMap<FileId, fs::File>,
//     tags: HashMap<TagId, Tag>,
//     filters: HashMap<FilterId, Filter>,
// }

// struct Plan {
//     list: Vec<Id>,
// }

/*

Load("apache.log") # 1
Take(5)
[File(1, "apache.log"), Take(1, 5)]
files: 1-(0,5)

Load("apache.log") # 1
Tag("foo")         # 2
Regex("[a|b]")
Take(5)
[File(1, "apache.log"), Tag(2, "[a|b]"), Take(2, 5)]
files: 1-(0,5)
tags:  2-(0,5)

Load("apache.log") # 1
Tag("foo")         # 2
Regex("[a|b]")
Filter(==, "a")    # 3
Take(5)
[File(1, "apache.log"), Tag(2, "[a|b]"), Filter(3, ==, "a"), Take(3, 5)]
files:   1-(0,?)
tags:    2-(0,?)
filters: 3-(0,5)

Load("apache.log") # 1
Tag("foo")         # 2
Regex("[a|b]")
Distinct()         # 3
Take(5)
[File(1, "apache.log"), Tag(2, "[a|b]"), Distinct(3), Take(3, 5)]
files:     1-(0,?)
tags:      2-(0,?)
distincts: 3-(0,5)

*/
