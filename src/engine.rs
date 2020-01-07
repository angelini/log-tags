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

    DirectFilter(TagId, Comparator, String),
    ScriptedFilter(TagId, String, Option<String>),

    // TODO
    Distinct(TagId, usize),
    DistinctCounts(TagId, usize),

    Take(Id, usize),
}

#[derive(Clone, Debug, Default)]
pub struct LuaScript {
    setup: Option<String>,
    eval: Option<String>,
}

impl LuaScript {
    pub fn new<S: Into<String>>(eval: S, setup: Option<S>) -> LuaScript {
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

enum Filter {
    Direct(Comparator, String),
    Scripted(LuaScript),
}

#[derive(Default)]
struct FilterCache {
    start: usize,
    end: usize,
    loaded: BitSet,
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
        Output {
            id: id,
            lines: lines,
        }
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
    filter_to_tag: HashMap<FilterId, TagId>,
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
            filter_to_tag: HashMap::new(),
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

                let tags = self.file_to_tags.entry(*file_id).or_insert(vec![]);
                tags.push(tag_id);

                Ok(Output::new(Id::Tag(tag_id), vec!["".to_string()]))
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

            Command::DirectFilter(tag_id, comparator, value) => {
                let filter_id = self.next_filter_id();

                let filter = Filter::Direct(*comparator, value.clone());
                self.filters.insert(filter_id, filter);
                self.filter_to_tag.insert(filter_id, *tag_id);

                Ok(Output::new(Id::Filter(filter_id), vec![]))
            }
            Command::ScriptedFilter(tag_id, test, setup) => {
                let filter_id = self.next_filter_id();

                let script = LuaScript::new(test, setup.as_ref());
                self.run_setup(&script)?;

                let filter = Filter::Scripted(script);
                self.filters.insert(filter_id, filter);
                self.filter_to_tag.insert(filter_id, *tag_id);

                Ok(Output::new(Id::Filter(filter_id), vec![]))
            }

            Command::Distinct(tag_id, max) => Ok(Output::new(Id::Tag(*tag_id), vec![])),
            Command::DistinctCounts(tag_id, max) => Ok(Output::new(Id::Tag(*tag_id), vec![])),

            Command::Take(id, count) => {
                let interval = Interval(0, *count);

                match id {
                    Id::File(file_id) => {
                        self.ensure_file(*file_id, interval)?;
                        self.ensure_all_tags(*file_id, interval)?;

                        let lines = self.read_lines(*file_id, interval);
                        let tags = self.read_all_tags(*file_id, interval);

                        let mut output = vec![];
                        for (idx, line) in lines.iter().enumerate() {
                            output.push(line.to_string());
                            for (name, tag_values) in &tags {
                                output.push(format!(
                                    "    {: <15} {:?}",
                                    format!("[{}]", name),
                                    tag_values[idx]
                                ))
                            }
                        }
                        Ok(Output::new(Id::File(*file_id), output))
                    }
                    Id::Filter(filter_id) => {
                        let file_id = self
                            .filter_to_file(filter_id)
                            .ok_or_else(|| Error::MissingId(Id::Filter(*filter_id)))?;
                        let tag_id = *self
                            .filter_to_tag
                            .get(filter_id)
                            .ok_or_else(|| Error::MissingId(Id::Filter(*filter_id)))?;

                        self.ensure_file(file_id, interval)?;
                        self.ensure_tag(file_id, tag_id, interval)?;
                        self.ensure_filter(tag_id, *filter_id, interval)?;

                        let filter = self.read_filter(*filter_id);
                        let lines = self.read_lines(file_id, interval);
                        let tags = self.read_all_tags(file_id, interval);

                        let mut output = vec![];
                        for (idx, line) in lines.iter().enumerate() {
                            if !filter.contains(idx) {
                                continue;
                            }
                            output.push(line.to_string());
                            for (name, tag_values) in &tags {
                                output.push(format!(
                                    "    {: <15} {:?}",
                                    format!("[{}]", name),
                                    tag_values[idx]
                                ))
                            }
                        }
                        Ok(Output::new(Id::Filter(*filter_id), output))
                    }
                    Id::Tag(_) => panic!(),
                }
            }
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

    fn run_setup(&mut self, script: &LuaScript) -> Result<()> {
        self.lua.context(|lua_ctx| {
            if let Some(ref setup) = script.setup {
                lua_ctx.load(setup).eval()?;
            }
            Ok(())
        })
    }

    fn ensure_file(&mut self, file_id: FileId, interval: Interval) -> Result<()> {
        let cache = self
            .file_caches
            .entry(file_id)
            .or_insert(FileCache::default());
        let cache_bounds = cache.bounds();

        if cache_bounds.contains(interval) {
            return Ok(());
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

            Ok(())
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

        let cache = self.tag_caches.entry(tag_id).or_insert(TagCache::default());

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
            .or_insert(FilterCache::default());
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
            .or_insert(FilterCache::default());

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
                        captures.get(1).map_or(None, |m| {
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
            Filter::Direct(comp, value) => {
                let mut result = BitSet::new();
                for (idx, tag_value) in values.iter().enumerate() {
                    match (comp, tag_value) {
                        (Comparator::Equal, Some(v)) if value == v => result.insert(start + idx),
                        (Comparator::NotEqual, Some(v)) if value != v => result.insert(start + idx),
                        (Comparator::GreaterThan, Some(v)) if value > v => {
                            result.insert(start + idx)
                        }
                        (Comparator::GreaterThanEqual, Some(v)) if value >= v => {
                            result.insert(start + idx)
                        }
                        (Comparator::LessThan, Some(v)) if value < v => result.insert(start + idx),
                        (Comparator::LessThanEqual, Some(v)) if value <= v => {
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

    fn filter_to_file(&self, filter_id: &FilterId) -> Option<FileId> {
        self.filter_to_tag.get(&filter_id).and_then(|tag_id| {
            for (file_id, tag_ids) in &self.file_to_tags {
                if tag_ids.contains(tag_id) {
                    return Some(*file_id);
                }
            }
            None
        })
    }
}
