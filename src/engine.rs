use std::collections::HashMap;
use std::fs;
use std::io;
use std::io::prelude::*;
use std::path;

use bit_set::BitSet;
use regex::Regex;

use crate::error::{Error, Result};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct FileId(pub usize);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct FilterId(pub usize);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TagId(pub usize);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Id {
    File(FileId),
    Filter(FilterId),
    Tag(TagId),
}

#[derive(Debug)]
pub enum Comparator {
    Equal,
    NotEqual,
    GreaterThan,
    GreaterThanEqual,
    LessThan,
    LessThanEqual,
}

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

    Take(FileId, usize),
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

struct Filter;

#[derive(Default)]
struct FileCache {
    start: usize,
    loaded: Vec<String>,
}

impl FileCache {
    fn end(&self) -> usize {
        self.start + self.loaded.len()
    }
}

type TagValue = Option<String>;

#[derive(Default)]
struct TagCache {
    start: usize,
    loaded: Vec<TagValue>,
}

impl TagCache {
    fn end(&self) -> usize {
        self.start + self.loaded.len()
    }

    fn bounds(&self) -> (usize, usize) {
        (self.start, self.end())
    }
}

#[derive(Default)]
struct FilterCache {
    start: usize,
    end: usize,
    loaded: BitSet,
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
                    .ok_or_else(|| Error::MissingId(format!("{:?}", tag_id)))?;
                tag.with_regex(regex)?;
                Ok(Output::new(Id::Tag(*tag_id), vec![]))
            }
            Command::Transform(tag_id, transform, setup) => {
                let script = LuaScript::new(transform, setup.as_ref());
                self.run_setup(&script)?;

                let tag = self
                    .tags
                    .get_mut(tag_id)
                    .ok_or_else(|| Error::MissingId(format!("{:?}", tag_id)))?;
                tag.with_script(script);

                Ok(Output::new(Id::Tag(*tag_id), vec![]))
            }

            Command::DirectFilter(tag_id, comparator, value) => {
                let filter_id = self.next_filter_id();
                Ok(Output::new(Id::Filter(filter_id), vec![]))
            }
            Command::ScriptedFilter(tag_id, test, setup) => {
                let filter_id = self.next_filter_id();

                let script = LuaScript::new(test, setup.as_ref());
                self.run_setup(&script)?;

                Ok(Output::new(Id::Filter(filter_id), vec![]))
            }

            Command::Distinct(tag_id, max) => Ok(Output::new(Id::Tag(*tag_id), vec![])),
            Command::DistinctCounts(tag_id, max) => Ok(Output::new(Id::Tag(*tag_id), vec![])),

            Command::Take(file_id, count) => {
                self.ensure_lines(*file_id, 0, *count)?;
                self.ensure_all_tags(*file_id, 0, *count)?;

                let lines = self.read_lines(*file_id, 0, *count);
                let tags = self.read_all_tags(*file_id, 0, *count);

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

    fn ensure_lines(&mut self, file_id: FileId, start: usize, end: usize) -> Result<()> {
        let cache = self
            .file_caches
            .entry(file_id)
            .or_insert(FileCache::default());
        if cache.start > start || cache.end() < end {
            match self.files.get(&file_id) {
                Some(file) => Engine::cache_lines_from_disk(cache, file, start, end),
                None => Err(Error::FileNotLoaded(format!("{:?}", file_id))),
            }
        } else {
            Ok(())
        }
    }

    fn read_lines(&self, file_id: FileId, start: usize, end: usize) -> &[String] {
        assert!(end >= start, "Read end must be larger than start");
        &self.file_caches.get(&file_id).unwrap().loaded[start..end]
    }

    fn ensure_tag(
        &mut self,
        file_id: FileId,
        tag_id: TagId,
        start: usize,
        end: usize,
    ) -> Result<()> {
        let (tags_start, tags_end): (usize, usize) = self
            .tag_caches
            .get(&tag_id)
            .map(|cache| cache.bounds())
            .unwrap_or((0, 0));

        if tags_start <= start && tags_end >= end {
            return Ok(());
        }

        let tag = self
            .tags
            .get(&tag_id)
            .ok_or_else(|| Error::MissingId(format!("{:?}", tag_id)))?;

        let mut prefix = None;
        let mut suffix = None;

        if tags_start > start {
            let lines = self.read_lines(file_id, start, tags_start);
            prefix = Some(Engine::parse_tag_from_lines(&self.lua, tag, lines));
        }

        if tags_end < end {
            let lines = self.read_lines(file_id, tags_end, end);
            suffix = Some(Engine::parse_tag_from_lines(&self.lua, tag, lines));
        }

        let cache = self.tag_caches.entry(tag_id).or_insert(TagCache::default());

        if let Some(mut prefix) = prefix {
            prefix.extend(cache.loaded.iter().cloned());
            cache.loaded = prefix;
            cache.start = start;
        }

        if let Some(suffix) = suffix {
            cache.loaded.extend(suffix.into_iter());
        }

        Ok(())
    }

    fn ensure_all_tags(&mut self, file_id: FileId, start: usize, end: usize) -> Result<()> {
        if let Some(tag_ids) = self.file_to_tags.get(&file_id) {
            for tag_id in tag_ids.clone() {
                self.ensure_tag(file_id, tag_id, start, end)?;
            }
        }
        Ok(())
    }

    fn read_tag(&self, file_id: FileId, tag_id: TagId, start: usize, end: usize) -> &[TagValue] {
        &self.tag_caches.get(&tag_id).unwrap().loaded[start..end]
    }

    fn read_all_tags(
        &self,
        file_id: FileId,
        start: usize,
        end: usize,
    ) -> Vec<(String, &[TagValue])> {
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
            result.push((name, self.read_tag(file_id, tag_id, start, end)));
        }
        result
    }

    fn parse_tag_from_lines(
        lua: &rlua::Lua,
        tag: &Tag,
        lines: &[String],
    ) -> Vec<TagValue> {
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

    fn cache_lines_from_disk(
        cache: &mut FileCache,
        file: &fs::File,
        start: usize,
        end: usize,
    ) -> Result<()> {
        if cache.start > start {
            let mut lines = io::BufReader::new(file)
                .lines()
                .skip(start)
                .take(cache.start - start)
                .collect::<io::Result<Vec<String>>>()?;
            lines.extend(cache.loaded.iter().cloned());
            cache.loaded = lines;
            cache.start = start;
        }
        if cache.end() < end {
            let lines = io::BufReader::new(file)
                .lines()
                .skip(end)
                .take(end - cache.end())
                .collect::<io::Result<Vec<String>>>()?;
            cache.loaded.extend(lines.into_iter());
        }
        Ok(())
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
}
