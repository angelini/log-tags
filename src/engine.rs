use std::collections::HashMap;
use std::fmt;
use std::fs;
use std::io;
use std::io::prelude::*;
use std::path;

use bit_set;
use ethbloom;
use regex;

use crate::base::{
    Aggregator, Comparator, DistinctId, FileId, FilterId, Id, Interval, TagId,
};
use crate::error::{Error, Result};

#[derive(Debug)]
pub enum Command {
    Load(path::PathBuf),
    Script(String),

    Tag(FileId, String),
    Regex(TagId, String),
    Transform(TagId, String),

    DirectFilter(Id, Comparator, String),
    ScriptedFilter(Id, String),

    Distinct(Id),

    Group(Id, Aggregator),

    Take(Id, usize),
}

struct File {
    index: usize,
    reader: io::BufReader<fs::File>,
}

impl File {
    fn new(path: path::PathBuf) -> Result<File> {
        let file = fs::File::open(&path)?;
        Ok(File {
            index: 0,
            reader: io::BufReader::new(file),
        })
    }

    fn read(&mut self, interval: Interval) -> Result<Vec<String>> {
        let offset = interval.0 as i64 - self.index as i64;

        if offset != 0 {
            // FIXME: use seek_relative when it's in stable
            self.reader.seek(io::SeekFrom::Current(offset))?;
        }

        let mut result = Vec::with_capacity(interval.len());
        for idx in interval.iter() {
            self.index = idx;

            let mut buffer = String::new();
            let bytes_read = self.reader.read_line(&mut buffer)?;
            if bytes_read == 0 {
                break;
            }

            result.push(buffer);
        }

        Ok(result)
    }
}

struct Tag {
    name: String,
    regex: Option<regex::Regex>,
    transform: Option<String>,
}

impl Tag {
    fn new<S: Into<String>>(name: S) -> Tag {
        Tag {
            name: name.into(),
            regex: None,
            transform: None,
        }
    }

    fn with_regex(&mut self, regex: &str) -> Result<()> {
        self.regex = Some(regex::Regex::new(regex)?);
        Ok(())
    }

    fn with_transform(&mut self, transform: String) {
        self.transform = Some(transform);
    }
}

enum Filter {
    Direct(Comparator, String),
    Scripted(String),
}

trait Cache {
    fn bounds(&self) -> Interval;
    fn size(&self) -> usize;
}

#[derive(Default)]
struct FileCache {
    start: usize,
    loaded: Vec<String>,
}

impl Cache for FileCache {
    fn bounds(&self) -> Interval {
        Interval(self.start, self.loaded.len())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(&self.loaded)
            + self
                .loaded
                .iter()
                .map(|s| std::mem::size_of_val(s) + s.capacity())
                .sum::<usize>()
    }
}

type TagValue = Option<String>;

#[derive(Default)]
struct TagCache {
    start: usize,
    loaded: Vec<TagValue>,
}

impl Cache for TagCache {
    fn bounds(&self) -> Interval {
        Interval(self.start, self.loaded.len())
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(&self.loaded)
            + self
                .loaded
                .iter()
                .map(|s_opt| {
                    std::mem::size_of_val(s_opt) + s_opt.as_ref().map(|s| s.capacity()).unwrap_or(0)
                })
                .sum::<usize>()
    }
}

#[derive(Default)]
struct FilterCache {
    start: usize,
    end: usize,
    loaded: bit_set::BitSet,
}

impl FilterCache {
    fn count(&self) -> usize {
        self.loaded.iter().count()
    }
}

impl Cache for FilterCache {
    fn bounds(&self) -> Interval {
        Interval(self.start, self.end)
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(&self.loaded)
    }
}

#[derive(Default)]
struct DistinctCache {
    start: usize,
    end: usize,
    loaded: bit_set::BitSet,
    bloom: ethbloom::Bloom,
}

impl DistinctCache {
    fn count(&self) -> usize {
        self.loaded.iter().count()
    }
}

impl Cache for DistinctCache {
    fn bounds(&self) -> Interval {
        Interval(self.start, self.end)
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(&self.loaded) + std::mem::size_of_val(&self.bloom)
    }
}

#[derive(Debug, Default)]
pub struct IntervalStats {
    distincts: HashMap<DistinctId, Vec<Interval>>,
    files: HashMap<FileId, Vec<Interval>>,
    filters: HashMap<FilterId, Vec<Interval>>,
    tags: HashMap<TagId, Vec<Interval>>,
}

impl IntervalStats {
    fn add(&mut self, id: Id, interval: Interval) {
        match id {
            Id::Distinct(did) => self
                .distincts
                .entry(did)
                .or_insert_with(Vec::new)
                .push(interval),
            Id::File(fid) => self
                .files
                .entry(fid)
                .or_insert_with(Vec::new)
                .push(interval),
            Id::Filter(fid) => self
                .filters
                .entry(fid)
                .or_insert_with(Vec::new)
                .push(interval),
            Id::Tag(tid) => self.tags.entry(tid).or_insert_with(Vec::new).push(interval),
        }
    }
}

impl fmt::Display for IntervalStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fn write_intervals(f: &mut fmt::Formatter<'_>, intervals: &[Interval]) -> fmt::Result {
            write!(f, "[")?;
            for interval in intervals {
                write!(f, "{}, ", interval)?;
            }
            writeln!(f, "]")
        }

        fn write_interval_kind<T: fmt::Debug + std::cmp::Ord>(
            f: &mut fmt::Formatter<'_>,
            name: &str,
            kinds: &HashMap<T, Vec<Interval>>,
        ) -> fmt::Result {
            if kinds.is_empty() {
                return Ok(())
            }
            writeln!(f, "{}: {{", name)?;

            let mut kinds_vec: Vec<(&T, &Vec<Interval>)> = kinds.iter().collect();
            kinds_vec.sort_by_key(|&(id, _)| id);

            for (id, intervals) in kinds_vec {
                write!(f, "  {:?}: ", id)?;
                write_intervals(f, intervals)?;
            }

            writeln!(f, "}}")
        }

        write_interval_kind(f, "files", &self.files)?;
        write_interval_kind(f, "tags", &self.tags)?;
        write_interval_kind(f, "filters", &self.filters)?;
        write_interval_kind(f, "distincts", &self.distincts)
    }
}

#[derive(Debug, Default)]
pub struct SizeStats {
    distincts: HashMap<DistinctId, usize>,
    files: HashMap<FileId, usize>,
    filters: HashMap<FilterId, usize>,
    tags: HashMap<TagId, usize>,
}

impl SizeStats {
    fn add(&mut self, id: Id, size: usize) {
        match id {
            Id::Distinct(did) => *self.distincts.entry(did).or_insert(0) = size,
            Id::File(fid) => *self.files.entry(fid).or_insert(0) = size,
            Id::Filter(fid) => *self.filters.entry(fid).or_insert(0) = size,
            Id::Tag(tid) => *self.tags.entry(tid).or_insert(0) = size,
        }
    }
}

impl fmt::Display for SizeStats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fn write_size_kind<T: fmt::Debug + std::cmp::Ord>(
            f: &mut fmt::Formatter<'_>,
            name: &str,
            kinds: &HashMap<T, usize>,
        ) -> fmt::Result {
            if kinds.is_empty() {
                return Ok(());
            }
            writeln!(f, "{}: {{", name)?;

            let mut kinds_vec: Vec<(&T, &usize)> = kinds.iter().collect();
            kinds_vec.sort_by_key(|&(id, _)| id);

            for (id, size) in kinds_vec {
                writeln!(f, "  {:?}: {:.3} MB", id, *size as f64 / 1_000_000.0)?;
            }

            writeln!(f, "}}")
        }

        write_size_kind(f, "files", &self.files)?;
        write_size_kind(f, "tags", &self.tags)?;
        write_size_kind(f, "filters", &self.filters)?;
        write_size_kind(f, "distincts", &self.distincts)
    }
}

#[derive(Debug)]
pub struct Stats {
    intervals: Option<IntervalStats>,
    sizes: Option<SizeStats>,
}

impl Stats {
    fn enabled() -> Self {
        Self {
            intervals: Some(IntervalStats::default()),
            sizes: Some(SizeStats::default()),
        }
    }

    fn disabled() -> Self {
        Self {
            intervals: None,
            sizes: None,
        }
    }

    fn add_interval(&mut self, id: Id, interval: Interval) {
        if let Some(intervals) = &mut self.intervals {
            intervals.add(id, interval);
        }
    }

    fn add_size(&mut self, id: Id, size: usize) {
        if let Some(sizes) = &mut self.sizes {
            sizes.add(id, size);
        }
    }
}

impl fmt::Display for Stats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if let Some(intervals) = &self.intervals {
            write!(f, "\nintervals\n---------\n{}", intervals)?;
        }
        if let Some(sizes) = &self.sizes {
            write!(f, "\nsizes\n-----\n{}", sizes)?;
        }
        Ok(())
    }
}

pub struct Output {
    pub id: Option<Id>,
    pub lines: Vec<String>,
    pub stats: Stats,
}

impl Output {
    fn with_message(id: Option<Id>, message: String) -> Output {
        Output {
            id,
            lines: vec![message],
            stats: Stats::disabled(),
        }
    }

    fn with_results(lines: Vec<String>, stats: Stats) -> Output {
        Output {
            id: None,
            lines,
            stats,
        }
    }
}

struct ReadIntervals {
    index: usize,
    next: usize,
    max: usize,
}

impl ReadIntervals {
    fn new(min: usize, max: usize) -> ReadIntervals {
        ReadIntervals {
            index: 0,
            next: std::cmp::min(min, max),
            max,
        }
    }
}

impl Iterator for ReadIntervals {
    type Item = Interval;

    fn next(&mut self) -> Option<Self::Item> {
        let interval = Interval(self.index, self.index + self.next);
        self.index += self.next;
        self.next = std::cmp::min(self.max, self.next * 2);
        Some(interval)
    }
}

const MAX_BATCH_SIZE: usize = 1024;

#[derive(Debug)]
struct Plan {
    steps: Vec<Id>,
}

impl Plan {
    fn new(steps: Vec<Id>) -> Plan {
        Plan { steps }
    }

    fn file_id(&self) -> FileId {
        match self.steps[0] {
            Id::File(file_id) => file_id,
            _ => panic!(),
        }
    }

    fn filter_ids(&self) -> Vec<FilterId> {
        self.steps
            .iter()
            .filter_map(|step| match step {
                Id::Filter(filter_id) => Some(*filter_id),
                _ => None,
            })
            .collect()
    }

    fn distinct_ids(&self) -> Vec<DistinctId> {
        self.steps
            .iter()
            .filter_map(|step| match step {
                Id::Distinct(distinct_id) => Some(*distinct_id),
                _ => None,
            })
            .collect()
    }
}

pub struct Engine {
    debug: bool,
    last_id: usize,
    lua: rlua::Lua,

    files: HashMap<FileId, File>,
    file_caches: HashMap<FileId, FileCache>,

    tags: HashMap<TagId, Tag>,
    tag_caches: HashMap<TagId, TagCache>,
    tag_to_file: HashMap<TagId, FileId>,

    filters: HashMap<FilterId, Filter>,
    filter_caches: HashMap<FilterId, FilterCache>,
    filter_to_parent: HashMap<FilterId, Id>,

    distinct_caches: HashMap<DistinctId, DistinctCache>,
    distinct_to_parent: HashMap<DistinctId, Id>,
}

impl Engine {
    pub fn new() -> Self {
        Engine {
            debug: false,
            last_id: 0,
            lua: rlua::Lua::new(),

            files: HashMap::new(),
            file_caches: HashMap::new(),

            tags: HashMap::new(),
            tag_caches: HashMap::new(),
            tag_to_file: HashMap::new(),

            filters: HashMap::new(),
            filter_caches: HashMap::new(),
            filter_to_parent: HashMap::new(),

            distinct_caches: HashMap::new(),
            distinct_to_parent: HashMap::new(),
        }
    }

    pub fn new_debug() -> Self {
        let mut engine = Self::new();
        engine.debug = true;
        engine
    }

    pub fn run_command(&mut self, command: &Command) -> Result<Output> {
        match command {
            Command::Load(path) => {
                let id = self.next_file_id();
                self.files.insert(id, File::new(path.clone())?);
                Ok(Output::with_message(
                    Some(Id::File(id)),
                    format!("file loaded: {:?} {:?}", id, path),
                ))
            }
            Command::Script(script) => {
                self.run_script(script)?;
                Ok(Output::with_message(None, "script loaded".to_string()))
            }

            Command::Tag(file_id, tag_name) => {
                let tag_id = self.next_tag_id();
                self.tags.insert(tag_id, Tag::new(tag_name));
                self.tag_to_file.insert(tag_id, *file_id);
                Ok(Output::with_message(
                    Some(Id::Tag(tag_id)),
                    format!("tag loaded: {} {}", tag_id.0, tag_name),
                ))
            }
            Command::Regex(tag_id, regex) => {
                let tag = self
                    .tags
                    .get_mut(tag_id)
                    .ok_or_else(|| Error::MissingId(Id::Tag(*tag_id)))?;
                tag.with_regex(regex)?;
                Ok(Output::with_message(
                    Some(Id::Tag(*tag_id)),
                    format!("regex added to: {}", tag_id.0),
                ))
            }
            Command::Transform(tag_id, transform) => {
                let tag = self
                    .tags
                    .get_mut(tag_id)
                    .ok_or_else(|| Error::MissingId(Id::Tag(*tag_id)))?;
                tag.with_transform(transform.clone());
                Ok(Output::with_message(
                    Some(Id::Tag(*tag_id)),
                    format!("transform added to: {}", tag_id.0),
                ))
            }

            Command::DirectFilter(id, comparator, value) => {
                let filter_id = self.next_filter_id();
                let filter = Filter::Direct(*comparator, value.clone());

                self.filters.insert(filter_id, filter);
                self.filter_to_parent.insert(filter_id, *id);

                Ok(Output::with_message(
                    Some(Id::Filter(filter_id)),
                    format!("filter loaded: {}", filter_id.0),
                ))
            }
            Command::ScriptedFilter(id, test) => {
                let filter_id = self.next_filter_id();
                let filter = Filter::Scripted(test.clone());

                self.filters.insert(filter_id, filter);
                self.filter_to_parent.insert(filter_id, *id);

                Ok(Output::with_message(
                    Some(Id::Filter(filter_id)),
                    format!("filter loaded: {}", filter_id.0),
                ))
            }

            Command::Distinct(id) => {
                let distinct_id = self.next_distinct_id();
                self.distinct_to_parent.insert(distinct_id, *id);
                Ok(Output::with_message(
                    Some(Id::Distinct(distinct_id)),
                    format!("distinct loaded: {}", distinct_id.0),
                ))
            }

            Command::Group(id, aggregator) => unimplemented!(),

            Command::Take(id, count) => Ok(self.take(&self.plan(*id), *count)?),
        }
    }

    fn next_distinct_id(&mut self) -> DistinctId {
        self.last_id += 1;
        DistinctId(self.last_id)
    }

    fn next_file_id(&mut self) -> FileId {
        self.last_id += 1;
        FileId(self.last_id)
    }

    fn next_filter_id(&mut self) -> FilterId {
        self.last_id += 1;
        FilterId(self.last_id)
    }

    fn next_tag_id(&mut self) -> TagId {
        self.last_id += 1;
        TagId(self.last_id)
    }

    fn plan(&self, id: Id) -> Plan {
        Plan::new(self.plan_steps(id))
    }

    fn plan_steps(&self, id: Id) -> Vec<Id> {
        match id {
            Id::File(_) => vec![id],
            Id::Distinct(distinct_id) => {
                let mut parent = self.plan_steps(self.distinct_to_parent[&distinct_id]);
                parent.push(id);
                parent
            }
            Id::Filter(filter_id) => {
                let mut parent = self.plan_steps(self.filter_to_parent[&filter_id]);
                parent.push(id);
                parent
            }
            Id::Tag(tag_id) => {
                let mut parent = self.plan_steps(Id::File(self.tag_to_file[&tag_id]));
                parent.push(id);
                parent
            }
        }
    }

    fn take(&mut self, plan: &Plan, count: usize) -> Result<Output> {
        let mut interval = Interval(0, 0);
        let mut stats = if self.debug {
            Stats::enabled()
        } else {
            Stats::disabled()
        };

        'outer: for batch_interval in ReadIntervals::new(count, MAX_BATCH_SIZE) {
            for id in &plan.steps {
                match id {
                    Id::File(file_id) => {
                        let read_count = self.ensure_file(&mut stats, *file_id, batch_interval)?;
                        if read_count == 0 {
                            break 'outer;
                        }
                        interval.1 += read_count;
                    }
                    Id::Distinct(distinct_id) => {
                        self.ensure_distinct(
                            &mut stats,
                            self.find_parent_tag(Id::Distinct(*distinct_id)).unwrap(),
                            *distinct_id,
                            interval,
                        )?;
                    }
                    Id::Filter(filter_id) => {
                        self.ensure_filter(
                            &mut stats,
                            self.find_parent_tag(Id::Filter(*filter_id)).unwrap(),
                            *filter_id,
                            interval,
                        )?;
                    }
                    Id::Tag(tag_id) => {
                        self.ensure_tag(&mut stats, self.tag_to_file[tag_id], *tag_id, interval)?;
                    }
                }
            }

            match plan.steps.last().unwrap() {
                Id::Distinct(distinct_id) => {
                    if self.distinct_caches[distinct_id].count() >= count {
                        break;
                    }
                }
                Id::File(file_id) => {
                    if self.file_caches[file_id].bounds().len() >= count {
                        break;
                    }
                }
                Id::Filter(filter_id) => {
                    if self.filter_caches[filter_id].count() >= count {
                        break;
                    }
                }
                Id::Tag(tag_id) => {
                    if self.tag_caches[tag_id].bounds().len() >= count {
                        break;
                    }
                }
            }
        }

        let file_id = plan.file_id();
        self.ensure_all_tags(&mut stats, plan.file_id(), interval)?;

        let lines = self.read_lines(file_id, interval);
        let tags = self.read_all_tags(file_id, interval);

        let mut combined_filter: Option<bit_set::BitSet> = None;
        for filter_id in plan.filter_ids() {
            match combined_filter {
                Some(ref mut filter) => filter.intersect_with(self.read_filter(filter_id)),
                None => combined_filter = Some(self.read_filter(filter_id).clone()),
            }
        }
        for distinct_id in plan.distinct_ids() {
            match combined_filter {
                Some(ref mut filter) => filter.intersect_with(self.read_distinct(distinct_id)),
                None => combined_filter = Some(self.read_distinct(distinct_id).clone()),
            }
        }

        let mut results = vec![];
        let mut current_count = 0;

        for (idx, line) in lines.iter().enumerate() {
            if let Some(filter) = &combined_filter {
                if !filter.contains(idx) {
                    continue;
                }
            }

            results.push(line.to_string());
            for (name, tag_values) in &tags {
                if let Some(value) = &tag_values[idx] {
                    results.push(format!("    {: <15} {:?}", format!("[{}]", name), value,))
                } else {
                    results.push(format!("    [{: <15}] N/A", name))
                }
            }
            results.push("".to_string());

            current_count += 1;
            if current_count >= count {
                break;
            }
        }

        Ok(Output::with_results(results, stats))
    }

    fn run_script(&mut self, script: &str) -> Result<()> {
        self.lua.context(|lua_ctx| {
            lua_ctx.load(script).eval()?;
            Ok(())
        })
    }

    fn ensure_file(
        &mut self,
        stats: &mut Stats,
        file_id: FileId,
        interval: Interval,
    ) -> Result<usize> {
        let cache = self
            .file_caches
            .entry(file_id)
            .or_insert_with(FileCache::default);
        let cache_bounds = cache.bounds();

        if cache_bounds.contains(interval) {
            stats.add_size(Id::File(file_id), cache.size());
            return Ok(std::cmp::min(cache_bounds.1 - interval.0, interval.len()));
        }

        if let Some(file) = self.files.get_mut(&file_id) {
            let missing_before = cache_bounds.missing_before(interval);
            if !missing_before.is_empty() {
                stats.add_interval(Id::File(file_id), missing_before);

                let mut lines = file.read(missing_before)?;
                lines.extend(cache.loaded.iter().cloned());
                cache.loaded = lines;
                cache.start = missing_before.0;
            }

            let missing_after = cache_bounds.missing_after(interval);
            if !missing_after.is_empty() {
                stats.add_interval(Id::File(file_id), missing_after);

                let lines = file.read(missing_after)?;
                cache.loaded.extend(lines.into_iter());
            }

            stats.add_size(Id::File(file_id), cache.size());
            Ok(std::cmp::min(
                cache.bounds().1 - std::cmp::min(cache.bounds().1, interval.0),
                interval.len(),
            ))
        } else {
            Err(Error::FileNotLoaded(format!("{:?}", file_id)))
        }
    }

    fn read_lines(&self, file_id: FileId, interval: Interval) -> &[String] {
        &self.file_caches[&file_id].loaded[interval.0..interval.1]
    }

    fn ensure_tag(
        &mut self,
        stats: &mut Stats,
        file_id: FileId,
        tag_id: TagId,
        interval: Interval,
    ) -> Result<()> {
        let cache_opt = self.tag_caches.get(&tag_id);
        let cache_bounds = cache_opt
            .map(|cache| cache.bounds())
            .unwrap_or(Interval(0, 0));

        if cache_bounds.contains(interval) {
            stats.add_size(
                Id::Tag(tag_id),
                cache_opt.map(|cache| cache.size()).unwrap_or(0),
            );
            return Ok(());
        }

        let tag = self
            .tags
            .get(&tag_id)
            .ok_or_else(|| Error::MissingId(Id::Tag(tag_id)))?;

        let mut prefix = None;
        let mut suffix = None;

        let missing_before = cache_bounds.missing_before(interval);
        if !missing_before.is_empty() {
            stats.add_interval(Id::Tag(tag_id), missing_before);
            let lines = self.read_lines(file_id, missing_before);
            prefix = Some(Engine::parse_tag_from_lines(&self.lua, tag, lines));
        }

        let missing_after = cache_bounds.missing_after(interval);
        if !missing_after.is_empty() {
            stats.add_interval(Id::Tag(tag_id), missing_after);
            let lines = self.read_lines(file_id, missing_after);
            suffix = Some(Engine::parse_tag_from_lines(&self.lua, tag, lines));
        }

        let cache = self
            .tag_caches
            .entry(tag_id)
            .or_insert_with(TagCache::default);

        if let Some(mut prefix) = prefix {
            prefix.extend(cache.loaded.iter().cloned());
            cache.loaded = prefix;
            cache.start = interval.0;
        }

        if let Some(suffix) = suffix {
            cache.loaded.extend(suffix.into_iter());
        }

        stats.add_size(Id::Tag(tag_id), cache.size());
        Ok(())
    }

    fn ensure_all_tags(
        &mut self,
        stats: &mut Stats,
        file_id: FileId,
        interval: Interval,
    ) -> Result<()> {
        for tag_id in self.file_to_tags(file_id) {
            self.ensure_tag(stats, file_id, tag_id, interval)?;
        }
        Ok(())
    }

    fn read_tag(&self, tag_id: TagId, interval: Interval) -> &[TagValue] {
        &self.tag_caches[&tag_id].loaded[interval.0..interval.1]
    }

    fn read_all_tags(&self, file_id: FileId, interval: Interval) -> Vec<(String, &[TagValue])> {
        let tags_name_and_id = self
            .file_to_tags(file_id)
            .into_iter()
            .map(|tag_id| (self.tags[&tag_id].name.clone(), tag_id))
            .collect::<Vec<(String, TagId)>>();

        let mut result = Vec::with_capacity(tags_name_and_id.len());
        for (name, tag_id) in tags_name_and_id {
            result.push((name, self.read_tag(tag_id, interval)));
        }
        result
    }

    fn ensure_filter(
        &mut self,
        stats: &mut Stats,
        tag_id: TagId,
        filter_id: FilterId,
        interval: Interval,
    ) -> Result<()> {
        let cache_opt = self.filter_caches.get(&filter_id);
        let cache_bounds = cache_opt
            .map(|cache| cache.bounds())
            .unwrap_or(Interval(0, 0));

        if cache_bounds.contains(interval) {
            stats.add_size(
                Id::Filter(filter_id),
                cache_opt.map(|cache| cache.size()).unwrap_or(0),
            );
            return Ok(());
        }

        let filter = self
            .filters
            .get(&filter_id)
            .ok_or_else(|| Error::MissingId(Id::Filter(filter_id)))?;

        let mut prefix = None;
        let mut suffix = None;

        let missing_before = cache_bounds.missing_before(interval);
        if !missing_before.is_empty() {
            stats.add_interval(Id::Filter(filter_id), missing_before);
            let tag_values = self.read_tag(tag_id, missing_before);
            prefix = Some(Engine::filter_values(
                &self.lua,
                filter,
                tag_values,
                missing_before.0,
            )?)
        }

        let missing_after = cache_bounds.missing_after(interval);
        if !missing_after.is_empty() {
            stats.add_interval(Id::Filter(filter_id), missing_after);
            let tag_values = self.read_tag(tag_id, missing_after);
            suffix = Some(Engine::filter_values(
                &self.lua,
                filter,
                tag_values,
                missing_after.0,
            )?)
        }

        let cache = self
            .filter_caches
            .entry(filter_id)
            .or_insert_with(FilterCache::default);

        if let Some(mut prefix) = prefix {
            prefix.union_with(&cache.loaded);
            cache.loaded = prefix;
            cache.start = interval.0;
        }

        if let Some(suffix) = suffix {
            cache.loaded.union_with(&suffix);
            cache.end = interval.1;
        }

        stats.add_size(Id::Filter(filter_id), cache.size());
        Ok(())
    }

    fn read_filter(&self, filter_id: FilterId) -> &bit_set::BitSet {
        &self.filter_caches[&filter_id].loaded
    }

    fn ensure_distinct(
        &mut self,
        stats: &mut Stats,
        tag_id: TagId,
        distinct_id: DistinctId,
        interval: Interval,
    ) -> Result<()> {
        let cache_opt = self.distinct_caches.get(&distinct_id);
        let cache_bounds = cache_opt
            .map(|cache| cache.bounds())
            .unwrap_or(Interval(0, 0));

        if cache_bounds.contains(interval) {
            stats.add_size(
                Id::Distinct(distinct_id),
                cache_opt.map(|cache| cache.size()).unwrap_or(0),
            );
            return Ok(());
        }

        let mut bloom = self
            .distinct_caches
            .get(&distinct_id)
            .map(|cache| cache.bloom)
            .unwrap_or_else(ethbloom::Bloom::zero);

        let mut prefix = None;
        let mut suffix = None;

        let missing_before = cache_bounds.missing_before(interval);
        if !missing_before.is_empty() {
            stats.add_interval(Id::Distinct(distinct_id), missing_before);
            let tag_values = self.read_tag(tag_id, missing_before);
            prefix = Some(Engine::distinct_values(
                &mut bloom,
                tag_values,
                missing_before.0,
            ));
        }

        let missing_after = cache_bounds.missing_after(interval);
        if !missing_after.is_empty() {
            stats.add_interval(Id::Distinct(distinct_id), missing_after);
            let tag_values = self.read_tag(tag_id, missing_after);
            suffix = Some(Engine::distinct_values(
                &mut bloom,
                tag_values,
                missing_after.0,
            ));
        }

        let cache = self
            .distinct_caches
            .entry(distinct_id)
            .or_insert_with(DistinctCache::default);

        if let Some(mut prefix) = prefix {
            prefix.union_with(&cache.loaded);
            cache.loaded = prefix;
            cache.start = interval.0;
        }

        if let Some(suffix) = suffix {
            cache.loaded.union_with(&suffix);
            cache.end = interval.1;
        }

        cache.bloom = bloom;

        stats.add_size(Id::Distinct(distinct_id), cache.size());
        Ok(())
    }

    fn read_distinct(&self, distinct_id: DistinctId) -> &bit_set::BitSet {
        &self.distinct_caches[&distinct_id].loaded
    }

    fn parse_tag_from_lines(lua: &rlua::Lua, tag: &Tag, lines: &[String]) -> Vec<TagValue> {
        let transform = tag.transform.as_ref().map(|s| s.as_str());
        lines
            .iter()
            .map(|line| {
                if let Some(ref regex) = tag.regex {
                    regex.captures(line).and_then(|captures| {
                        captures
                            .get(1)
                            .and_then(|m| Engine::transform_chunk(&lua, transform, m.as_str()).ok())
                    })
                } else {
                    Engine::transform_chunk(&lua, transform, line).ok()
                }
            })
            .collect()
    }

    fn filter_values(
        lua: &rlua::Lua,
        filter: &Filter,
        values: &[TagValue],
        start: usize,
    ) -> Result<bit_set::BitSet> {
        match filter {
            Filter::Direct(comp, right) => {
                let mut result = bit_set::BitSet::new();
                for (idx, left_option) in values.iter().enumerate() {
                    match (comp, left_option) {
                        (Comparator::Equal, Some(left)) if left == right => {
                            result.insert(start + idx)
                        }
                        (Comparator::NotEqual, Some(left)) if left != right => {
                            result.insert(start + idx)
                        }
                        (Comparator::GreaterThan, Some(left)) if left > right => {
                            result.insert(start + idx)
                        }
                        (Comparator::GreaterThanEqual, Some(left)) if left >= right => {
                            result.insert(start + idx)
                        }
                        (Comparator::LessThan, Some(left)) if left < right => {
                            result.insert(start + idx)
                        }
                        (Comparator::LessThanEqual, Some(left)) if left <= right => {
                            result.insert(start + idx)
                        }
                        (_, None) => continue,
                        (_, Some(_)) => continue,
                    };
                }
                Ok(result)
            }
            Filter::Scripted(script) => {
                let mut result = bit_set::BitSet::new();
                for (idx, value_option) in values.iter().enumerate() {
                    if let Some(value) = value_option {
                        if Self::test_chunk(lua, script, value)? {
                            result.insert(start + idx);
                        }
                    }
                }
                Ok(result)
            }
        }
    }

    fn distinct_values(
        bloom: &mut ethbloom::Bloom,
        tag_values: &[Option<String>],
        start: usize,
    ) -> bit_set::BitSet {
        let mut result = bit_set::BitSet::new();
        for (idx, value_option) in tag_values.iter().enumerate() {
            if let Some(value) = value_option {
                let bytes = value.as_bytes();
                if !bloom.contains_input(ethbloom::Input::Raw(bytes)) {
                    result.insert(start + idx);
                    bloom.accrue(ethbloom::Input::Raw(bytes));
                }
            }
        }
        result
    }

    fn transform_chunk(lua: &rlua::Lua, transform: Option<&str>, chunk: &str) -> Result<String> {
        match transform {
            Some(eval_src) => Ok(lua.context(|lua_ctx| {
                lua_ctx.globals().set("chunk", chunk)?;
                lua_ctx.load(eval_src).eval()
            })?),
            None => Ok(chunk.to_string()),
        }
    }

    fn test_chunk(lua: &rlua::Lua, test: &str, chunk: &str) -> Result<bool> {
        Ok(lua.context(|lua_ctx| {
            lua_ctx.globals().set("chunk", chunk)?;
            lua_ctx.load(test).eval()
        })?)
    }

    fn file_to_tags(&self, file_id: FileId) -> Vec<TagId> {
        self.tag_to_file
            .iter()
            .filter(|(_, &fid)| fid == file_id)
            .map(|(tid, _)| *tid)
            .collect()
    }

    fn find_parent_tag(&self, id: Id) -> Option<TagId> {
        match id {
            Id::Distinct(did) => self.find_parent_tag(self.distinct_to_parent[&did]),
            Id::Filter(fid) => self.find_parent_tag(self.filter_to_parent[&fid]),
            Id::Tag(tid) => Some(tid),
            _ => None,
        }
    }
}
