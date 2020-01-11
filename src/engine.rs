use std::collections::HashMap;
use std::fs;
use std::io;
use std::io::prelude::*;
use std::path;

use bit_set::BitSet;
use regex::Regex;

use crate::base::{Bounded, Comparator, FileId, FilterId, Id, Interval, TagId};
use crate::error::{Error, Result};

#[derive(Debug)]
pub enum Command {
    Load(path::PathBuf),

    Tag(FileId, String),
    Regex(TagId, String),
    Transform(TagId, String, Option<String>),

    DirectFilter(Id, Comparator, String),
    ScriptedFilter(Id, String, Option<String>),

    // TODO
    Distinct(TagId, usize),
    DistinctCounts(TagId, usize),

    Take(Id, usize),
}

#[derive(Clone, Debug, Default)]
struct LuaScript {
    setup: Option<String>,
    eval: Option<String>,
}

impl LuaScript {
    fn new<S: Into<String>>(eval: S, setup: Option<S>) -> LuaScript {
        LuaScript {
            setup: setup.map(|s| s.into()),
            eval: Some(eval.into()),
        }
    }
}

struct Tag {
    name: String,
    regex: Option<Regex>,
    transform: LuaScript,
}

impl Tag {
    fn new<S: Into<String>>(name: S) -> Tag {
        Tag {
            name: name.into(),
            regex: None,
            transform: LuaScript::default(),
        }
    }

    fn with_regex(&mut self, regex: &str) -> Result<()> {
        self.regex = Some(Regex::new(regex)?);
        Ok(())
    }

    fn with_script(&mut self, script: LuaScript) {
        self.transform = script;
    }
}

enum Filter {
    Direct(Comparator, String),
    Scripted(LuaScript),
}

#[derive(Default)]
struct FileCache {
    start: usize,
    loaded: Vec<String>,
}

impl Bounded for FileCache {
    fn bounds(&self) -> Interval {
        Interval(self.start, self.loaded.len())
    }
}

type TagValue = Option<String>;

#[derive(Default)]
struct TagCache {
    start: usize,
    loaded: Vec<TagValue>,
}

impl Bounded for TagCache {
    fn bounds(&self) -> Interval {
        Interval(self.start, self.loaded.len())
    }
}

#[derive(Default)]
struct FilterCache {
    start: usize,
    end: usize,
    loaded: BitSet,
}

impl FilterCache {
    fn count(&self) -> usize {
        self.loaded.iter().count()
    }
}

impl Bounded for FilterCache {
    fn bounds(&self) -> Interval {
        Interval(self.start, self.end)
    }
}

pub struct Output {
    pub id: Id,
    pub lines: Vec<String>,
}

impl Output {
    fn new(id: Id, lines: Vec<String>) -> Output {
        Output { id, lines }
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
            max: max,
        }
    }
}

impl Iterator for ReadIntervals {
    type Item = Interval;

    fn next(&mut self) -> Option<Interval> {
        let interval = Interval(self.index, self.index + self.next);
        self.index += self.next;
        self.next = std::cmp::min(self.max, self.next * 2);
        Some(interval)
    }
}

const MAX_BATCH_SIZE: usize = 1024;

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
}

pub struct Engine {
    last_id: usize,
    lua: rlua::Lua,

    files: HashMap<FileId, fs::File>,
    file_caches: HashMap<FileId, FileCache>,

    tags: HashMap<TagId, Tag>,
    tag_caches: HashMap<TagId, TagCache>,
    file_to_tags: HashMap<FileId, Vec<TagId>>,

    filters: HashMap<FilterId, Filter>,
    filter_caches: HashMap<FilterId, FilterCache>,
    filter_to_parent: HashMap<FilterId, Id>,
}

impl Engine {
    pub fn new() -> Engine {
        Engine {
            last_id: 0,
            lua: rlua::Lua::new(),

            files: HashMap::new(),
            file_caches: HashMap::new(),

            tags: HashMap::new(),
            tag_caches: HashMap::new(),
            file_to_tags: HashMap::new(),

            filters: HashMap::new(),
            filter_caches: HashMap::new(),
            filter_to_parent: HashMap::new(),
        }
    }

    pub fn run_command(&mut self, command: &Command) -> Result<Output> {
        match command {
            Command::Load(path) => {
                let id = self.next_file_id();
                self.files.insert(id, fs::File::open(path)?);
                Ok(Output::new(
                    Id::File(id),
                    vec![format!("loaded {:?}", path)],
                ))
            }

            Command::Tag(file_id, tag_name) => {
                let tag_id = self.next_tag_id();
                self.tags.insert(tag_id, Tag::new(tag_name));

                let tags = self.file_to_tags.entry(*file_id).or_insert_with(|| vec![]);
                tags.push(tag_id);

                Ok(Output::new(Id::Tag(tag_id), vec![]))
            }
            Command::Regex(tag_id, regex) => {
                let tag = self
                    .tags
                    .get_mut(tag_id)
                    .ok_or_else(|| Error::MissingId(Id::Tag(*tag_id)))?;
                tag.with_regex(regex)?;
                Ok(Output::new(Id::Tag(*tag_id), vec![]))
            }
            Command::Transform(tag_id, transform, setup) => {
                let script = LuaScript::new(transform, setup.as_ref());
                self.run_setup(&script)?;

                let tag = self
                    .tags
                    .get_mut(tag_id)
                    .ok_or_else(|| Error::MissingId(Id::Tag(*tag_id)))?;
                tag.with_script(script);

                Ok(Output::new(Id::Tag(*tag_id), vec![]))
            }

            Command::DirectFilter(id, comparator, value) => {
                let filter_id = self.next_filter_id();

                let filter = Filter::Direct(*comparator, value.clone());
                self.filters.insert(filter_id, filter);
                self.filter_to_parent.insert(filter_id, *id);

                Ok(Output::new(Id::Filter(filter_id), vec![]))
            }
            Command::ScriptedFilter(id, test, setup) => {
                let filter_id = self.next_filter_id();

                let script = LuaScript::new(test, setup.as_ref());
                self.run_setup(&script)?;

                let filter = Filter::Scripted(script);
                self.filters.insert(filter_id, filter);
                self.filter_to_parent.insert(filter_id, *id);

                Ok(Output::new(Id::Filter(filter_id), vec![]))
            }

            Command::Distinct(tag_id, max) => Ok(Output::new(Id::Tag(*tag_id), vec![])),
            Command::DistinctCounts(tag_id, max) => Ok(Output::new(Id::Tag(*tag_id), vec![])),

            Command::Take(id, count) => Ok(Output::new(*id, self.take(&self.plan(*id), *count)?)),
        }
    }

    fn next_file_id(&mut self) -> FileId {
        self.last_id += 1;
        FileId(self.last_id)
    }

    fn next_tag_id(&mut self) -> TagId {
        self.last_id += 1;
        TagId(self.last_id)
    }

    fn next_filter_id(&mut self) -> FilterId {
        self.last_id += 1;
        FilterId(self.last_id)
    }

    fn plan(&self, id: Id) -> Plan {
        Plan::new(self.plan_steps(id))
    }

    fn plan_steps(&self, id: Id) -> Vec<Id> {
        match id {
            Id::File(_) => vec![id],
            Id::Tag(tag_id) => {
                let mut parent = self.plan_steps(Id::File(self.tag_to_file(tag_id).unwrap()));
                parent.push(id);
                parent
            }
            Id::Filter(filter_id) => {
                let mut parent = self.plan_steps(self.filter_to_parent[&filter_id]);
                parent.push(id);
                parent
            }
        }
    }

    fn take(&mut self, plan: &Plan, count: usize) -> Result<Vec<String>> {
        let mut interval = Interval(0, 0);

        'outer: for batch_interval in ReadIntervals::new(count, MAX_BATCH_SIZE) {
            for id in &plan.steps {
                match id {
                    Id::File(file_id) => {
                        let read_count = self.ensure_file(*file_id, batch_interval)?;
                        if read_count == 0 {
                            break 'outer;
                        }
                        interval.1 += read_count;
                    }
                    Id::Tag(tag_id) => {
                        self.ensure_tag(self.tag_to_file(*tag_id).unwrap(), *tag_id, interval)?;
                    }
                    Id::Filter(filter_id) => {
                        self.ensure_filter(self.filter_to_tag(*filter_id).unwrap(), *filter_id, interval)?;
                    }
                }
            }

            match plan.steps.last().unwrap() {
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
        self.ensure_all_tags(plan.file_id(), interval)?;

        let lines = self.read_lines(file_id, interval);
        let tags = self.read_all_tags(file_id, interval);

        let mut combined_filter: Option<BitSet> = None;
        for filter_id in plan.filter_ids() {
            match combined_filter {
                Some(ref mut filter) => filter.intersect_with(self.read_filter(filter_id)),
                None => combined_filter = Some(self.read_filter(filter_id).clone()),
            }
        }

        let mut output = vec![];
        let mut current_count = 0;

        for (idx, line) in lines.iter().enumerate() {
            if let Some(filter) = &combined_filter {
                if !filter.contains(idx) {
                    continue;
                }
            }
            output.push(line.to_string());
            for (name, tag_values) in &tags {
                output.push(format!(
                    "    {: <15} {:?}",
                    format!("[{}]", name),
                    tag_values[idx]
                ))
            }

            current_count += 1;
            if current_count >= count {
                break;
            }
        }
        Ok(output)
    }

    fn run_setup(&mut self, script: &LuaScript) -> Result<()> {
        self.lua.context(|lua_ctx| {
            if let Some(ref setup) = script.setup {
                lua_ctx.load(setup).eval()?;
            }
            Ok(())
        })
    }

    fn ensure_file(&mut self, file_id: FileId, interval: Interval) -> Result<usize> {
        let cache = self
            .file_caches
            .entry(file_id)
            .or_insert_with(FileCache::default);
        let cache_bounds = cache.bounds();

        if cache_bounds.contains(interval) {
            return Ok(std::cmp::min(cache_bounds.1 - interval.0, interval.len()));
        }

        if let Some(file) = self.files.get(&file_id) {
            let missing_before = cache_bounds.missing_before(interval);
            if !missing_before.is_empty() {
                let mut lines = io::BufReader::new(file)
                    .lines()
                    .skip(missing_before.0)
                    .take(missing_before.1)
                    .collect::<io::Result<Vec<String>>>()?;
                lines.extend(cache.loaded.iter().cloned());
                cache.loaded = lines;
                cache.start = missing_before.0;
            }

            let missing_after = cache_bounds.missing_after(interval);
            if !missing_after.is_empty() {
                let lines = io::BufReader::new(file)
                    .lines()
                    .skip(missing_after.0)
                    .take(missing_after.1)
                    .collect::<io::Result<Vec<String>>>()?;
                cache.loaded.extend(lines.into_iter());
            }

            Ok(std::cmp::min(
                cache.bounds().1 - std::cmp::min(cache.bounds().1, interval.0),
                interval.len(),
            ))
        } else {
            Err(Error::FileNotLoaded(format!("{:?}", file_id)))
        }
    }

    fn read_lines(&self, file_id: FileId, interval: Interval) -> &[String] {
        &self.file_caches.get(&file_id).unwrap().loaded[interval.0..interval.1]
    }

    fn ensure_tag(&mut self, file_id: FileId, tag_id: TagId, interval: Interval) -> Result<()> {
        let cache_bounds = self
            .tag_caches
            .get(&tag_id)
            .map(|cache| cache.bounds())
            .unwrap_or(Interval(0, 0));

        if cache_bounds.contains(interval) {
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
            let lines = self.read_lines(file_id, missing_before);
            prefix = Some(Engine::parse_tag_from_lines(&self.lua, tag, lines));
        }

        let missing_after = cache_bounds.missing_after(interval);
        if !missing_after.is_empty() {
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

        Ok(())
    }

    fn ensure_all_tags(&mut self, file_id: FileId, interval: Interval) -> Result<()> {
        if let Some(tag_ids) = self.file_to_tags.get(&file_id) {
            for tag_id in tag_ids.clone() {
                self.ensure_tag(file_id, tag_id, interval)?;
            }
        }
        Ok(())
    }

    fn read_tag(&self, tag_id: TagId, interval: Interval) -> &[TagValue] {
        &self.tag_caches.get(&tag_id).unwrap().loaded[interval.0..interval.1]
    }

    fn read_all_tags(&self, file_id: FileId, interval: Interval) -> Vec<(String, &[TagValue])> {
        let tags_name_and_id = self
            .file_to_tags
            .get(&file_id)
            .map(|tag_ids| {
                tag_ids
                    .iter()
                    .map(|tag_id| (self.tags.get(tag_id).unwrap().name.clone(), *tag_id))
                    .collect()
            })
            .unwrap_or_else(|| vec![]);

        let mut result = Vec::with_capacity(tags_name_and_id.len());
        for (name, tag_id) in tags_name_and_id {
            result.push((name, self.read_tag(tag_id, interval)));
        }
        result
    }

    fn ensure_filter(
        &mut self,
        tag_id: TagId,
        filter_id: FilterId,
        interval: Interval,
    ) -> Result<()> {
        let cache = self
            .filter_caches
            .entry(filter_id)
            .or_insert_with(FilterCache::default);
        let cache_bounds = cache.bounds();

        if cache_bounds.contains(interval) {
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
            let tag_values = self.read_tag(tag_id, missing_before);
            prefix = Some(Engine::filter_values(
                &self.lua,
                filter,
                tag_values,
                missing_before.0,
            ))
        }

        let missing_after = cache_bounds.missing_after(interval);
        if !missing_after.is_empty() {
            let tag_values = self.read_tag(tag_id, missing_after);
            suffix = Some(Engine::filter_values(
                &self.lua,
                filter,
                tag_values,
                missing_after.0,
            ))
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

        Ok(())
    }

    fn read_filter(&self, filter_id: FilterId) -> &BitSet {
        &self.filter_caches.get(&filter_id).unwrap().loaded
    }

    fn parse_tag_from_lines(lua: &rlua::Lua, tag: &Tag, lines: &[String]) -> Vec<TagValue> {
        lines
            .iter()
            .map(|line| {
                if let Some(ref regex) = tag.regex {
                    regex.captures(line).and_then(|captures| {
                        captures.get(1).and_then(|m| {
                            Engine::transform_chunk(&lua, &tag.transform, m.as_str()).ok()
                        })
                    })
                } else {
                    Engine::transform_chunk(&lua, &tag.transform, line).ok()
                }
            })
            .collect()
    }

    fn filter_values(
        lua: &rlua::Lua,
        filter: &Filter,
        values: &[TagValue],
        start: usize,
    ) -> BitSet {
        match filter {
            Filter::Direct(comp, right) => {
                let mut result = BitSet::new();
                for (idx, left_option) in values.iter().enumerate() {
                    match (comp, left_option) {
                        (Comparator::Equal, Some(left)) if left == right => result.insert(start + idx),
                        (Comparator::NotEqual, Some(left)) if left != right => result.insert(start + idx),
                        (Comparator::GreaterThan, Some(left)) if left > right => {
                            result.insert(start + idx)
                        }
                        (Comparator::GreaterThanEqual, Some(left)) if left >= right => {
                            result.insert(start + idx)
                        }
                        (Comparator::LessThan, Some(left)) if left < right => result.insert(start + idx),
                        (Comparator::LessThanEqual, Some(left)) if left <= right => {
                            result.insert(start + idx)
                        }
                        (_, None) => continue,
                        (_, Some(_)) => continue,
                    };
                }
                result
            }
            Filter::Scripted(script) => unimplemented!(),
        }
    }

    fn transform_chunk(lua: &rlua::Lua, transform: &LuaScript, chunk: &str) -> Result<String> {
        match transform.eval {
            Some(ref eval_src) => Ok(lua.context(|lua_ctx| {
                lua_ctx.globals().set("chunk", chunk)?;
                lua_ctx.load(eval_src).eval()
            })?),
            None => Ok(chunk.to_string()),
        }
    }

    fn tag_to_file(&self, tag_id: TagId) -> Option<FileId> {
        for (file_id, tag_ids) in &self.file_to_tags {
            if tag_ids.contains(&tag_id) {
                return Some(*file_id);
            }
        }
        None
    }

    fn filter_to_tag(&self, filter_id: FilterId) -> Option<TagId> {
        match self.filter_to_parent[&filter_id] {
            Id::File(fid) => None,
            Id::Filter(fid) => self.filter_to_tag(fid),
            Id::Tag(tid) => Some(tid)
        }
    }
}
