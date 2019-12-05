use std::collections::HashMap;
use std::fs;
use std::io;
use std::io::prelude::*;
use std::path;

use regex::Regex;

use crate::error::{Error, Result};

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct FileId(pub usize);

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct TagId(pub usize);

#[derive(Clone, Debug, Default)]
pub struct LuaScript {
    setup: Option<String>,
    eval: Option<String>,
}

impl LuaScript {
    pub fn new<S: Into<String>>(setup: Option<S>, eval: Option<S>) -> LuaScript {
        LuaScript {
            setup: setup.map(|s| s.into()),
            eval: eval.map(|s| s.into()),
        }
    }
}

struct TagDefinition {
    regex: Regex,
    name: String,
    transform: LuaScript,
}

impl TagDefinition {
    fn new<S: Into<String>>(name: S, pattern: &str, transform: LuaScript) -> Result<TagDefinition> {
        Ok(TagDefinition {
            regex: Regex::new(&pattern)?,
            name: name.into(),
            transform: transform,
        })
    }
}

#[derive(Default)]
struct TagDefinitions {
    definitions: HashMap<TagId, TagDefinition>,
    order: Vec<TagId>,
}

impl TagDefinitions {
    fn get(&self, id: &TagId) -> Option<&TagDefinition> {
        self.definitions.get(id)
    }

    fn insert(&mut self, id: TagId, definition: TagDefinition) -> Option<TagDefinition> {
        self.order.push(id);
        self.definitions.insert(id, definition)
    }

    fn iter(&self) -> TagDefinitionsIterator {
        TagDefinitionsIterator {
            definitions: self,
            index: 0,
        }
    }
}

struct TagDefinitionsIterator<'a> {
    definitions: &'a TagDefinitions,
    index: usize,
}

impl<'a> Iterator for TagDefinitionsIterator<'a> {
    type Item = (TagId, &'a TagDefinition);

    fn next(&mut self) -> Option<(TagId, &'a TagDefinition)> {
        if self.index >= self.definitions.order.len() {
            return None;
        }

        let id = self.definitions.order[self.index];
        let result = self.definitions.get(&id).unwrap();
        self.index += 1;

        Some((id, result))
    }
}

type TagValue = Option<String>;

#[derive(Default)]
struct Tags {
    start: usize,
    loaded: Vec<TagValue>,
}

impl Tags {
    fn end(&self) -> usize {
        self.start + self.loaded.len()
    }

    fn bounds(&self) -> (usize, usize) {
        (self.start, self.end())
    }
}

#[derive(Debug)]
pub enum Command {
    Load(path::PathBuf),
    Tag(FileId, String, String, LuaScript),
    Take(FileId, usize),
}

#[derive(Default)]
struct LineCache {
    start: usize,
    loaded: Vec<String>,
}

impl LineCache {
    fn end(&self) -> usize {
        self.start + self.loaded.len()
    }
}

pub struct Engine {
    last_id: usize,
    file_ids: HashMap<String, FileId>,
    tag_ids: HashMap<String, TagId>,

    files: HashMap<FileId, fs::File>,
    lines: HashMap<FileId, LineCache>,
    definitions: HashMap<FileId, TagDefinitions>,
    tags: HashMap<FileId, HashMap<TagId, Tags>>,

    lua: rlua::Lua,
}

impl Engine {
    pub fn new() -> Engine {
        Engine {
            last_id: 0,
            file_ids: HashMap::new(),
            tag_ids: HashMap::new(),
            files: HashMap::new(),
            lines: HashMap::new(),
            definitions: HashMap::new(),
            tags: HashMap::new(),
            lua: rlua::Lua::new(),
        }
    }

    pub fn run_command(&mut self, command: &Command) -> Result<Vec<String>> {
        match command {
            Command::Load(path) => self
                .load_file(path)
                .map(|_| vec![format!("loaded {:?}", path)]),
            Command::Tag(file_id, tag_name, regex_str, script) => {
                self.run_setup(script)?;

                let tag_id = self.get_or_create_tag_id(tag_name);
                let definitions = self
                    .definitions
                    .entry(*file_id)
                    .or_insert(TagDefinitions::default());

                definitions.insert(
                    tag_id,
                    TagDefinition::new(tag_name, regex_str, script.clone())?,
                );
                Ok(vec!["".to_string()])
            }
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
                Ok(output)
            }
        }
    }

    fn get_or_create_file_id(&mut self, key: &str) -> FileId {
        if !self.file_ids.contains_key(key) {
            self.last_id += 1;
            self.file_ids.insert(key.to_string(), FileId(self.last_id));
        }

        *self.file_ids.get(key).unwrap()
    }

    fn get_or_create_tag_id(&mut self, key: &str) -> TagId {
        if !self.tag_ids.contains_key(key) {
            self.last_id += 1;
            self.tag_ids.insert(key.to_string(), TagId(self.last_id));
        }

        *self.tag_ids.get(key).unwrap()
    }

    fn debug_fid(&self, id: FileId) -> String {
        self.file_ids
            .iter()
            .find(|(_, &val)| val == id)
            .map(|(k, _)| format!("file:{}", k))
            .unwrap_or_else(|| "FILE_ID_NOT_FOUND".to_string())
    }

    fn debug_tid(&self, id: TagId) -> String {
        self.tag_ids
            .iter()
            .find(|(_, &val)| val == id)
            .map(|(k, _)| format!("tag:{}", k))
            .unwrap_or_else(|| "TAG_ID_NOT_FOUND".to_string())
    }

    fn run_setup(&mut self, script: &LuaScript) -> Result<()> {
        self.lua.context(|lua_ctx| {
            if let Some(ref setup) = script.setup {
                lua_ctx.load(setup).eval()?;
            }
            Ok(())
        })
    }

    fn load_file(&mut self, path: &path::Path) -> Result<()> {
        let full_path = path.canonicalize()?;
        let id = self.get_or_create_file_id(&full_path.to_string_lossy());
        self.files.insert(id, fs::File::open(path)?);
        Ok(())
    }

    fn ensure_lines(&mut self, file_id: FileId, start: usize, end: usize) -> Result<()> {
        let cache = self.lines.entry(file_id).or_insert(LineCache::default());
        if cache.start > start || cache.end() < end {
            match self.files.get(&file_id) {
                Some(file) => Engine::cache_lines_from_disk(cache, file, start, end),
                None => Err(Error::FileNotLoaded(self.debug_fid(file_id))),
            }
        } else {
            Ok(())
        }
    }

    fn read_lines(&self, file_id: FileId, start: usize, end: usize) -> &[String] {
        assert!(end >= start, "Read end must be larger than start");
        &self.lines.get(&file_id).unwrap().loaded[start..end]
    }

    fn ensure_tag(
        &mut self,
        file_id: FileId,
        tag_id: TagId,
        start: usize,
        end: usize,
    ) -> Result<()> {
        let (tags_start, tags_end): (usize, usize) = self
            .tags
            .get(&file_id)
            .and_then(|all_tags| all_tags.get(&tag_id))
            .map(|tags| tags.bounds())
            .unwrap_or((0, 0));

        if tags_start <= start && tags_end >= end {
            return Ok(());
        }

        let definition = self
            .definitions
            .get(&file_id)
            .ok_or_else(|| Error::MissingId(self.debug_fid(file_id)))?
            .get(&tag_id)
            .ok_or_else(|| Error::MissingId(self.debug_tid(tag_id)))?;

        let mut prefix = None;
        let mut suffix = None;

        if tags_start > start {
            let lines = self.read_lines(file_id, start, tags_start);
            prefix = Some(Engine::parse_tag_from_lines(&self.lua, definition, lines));
        }

        if tags_end < end {
            let lines = self.read_lines(file_id, tags_end, end);
            suffix = Some(Engine::parse_tag_from_lines(&self.lua, definition, lines));
        }

        let tags = self
            .tags
            .entry(file_id)
            .or_insert(HashMap::new())
            .entry(tag_id)
            .or_insert(Tags::default());

        if let Some(mut prefix) = prefix {
            prefix.extend(tags.loaded.iter().cloned());
            tags.loaded = prefix;
            tags.start = start;
        }

        if let Some(suffix) = suffix {
            tags.loaded.extend(suffix.into_iter());
        }

        Ok(())
    }

    fn ensure_all_tags(&mut self, file_id: FileId, start: usize, end: usize) -> Result<()> {
        if let Some(tag_ids) = self
            .definitions
            .get(&file_id)
            .map(|definitions| definitions.order.clone())
        {
            for tag_id in tag_ids {
                self.ensure_tag(file_id, tag_id, start, end)?;
            }
        }
        Ok(())
    }

    fn read_tag(&self, file_id: FileId, tag_id: TagId, start: usize, end: usize) -> &[TagValue] {
        &self
            .tags
            .get(&file_id)
            .unwrap()
            .get(&tag_id)
            .unwrap()
            .loaded[start..end]
    }

    fn read_all_tags(
        &self,
        file_id: FileId,
        start: usize,
        end: usize,
    ) -> Vec<(String, &[TagValue])> {
        let tag_ids = self
            .definitions
            .get(&file_id)
            .map(|definitions| {
                definitions
                    .iter()
                    .map(|(tag_id, definition)| (definition.name.to_string(), tag_id))
                    .collect()
            })
            .unwrap_or_else(|| vec![]);

        let mut result = Vec::with_capacity(tag_ids.len());
        for (name, tag_id) in tag_ids {
            result.push((name, self.read_tag(file_id, tag_id, start, end)));
        }
        result
    }

    fn parse_tag_from_lines(
        lua: &rlua::Lua,
        definition: &TagDefinition,
        lines: &[String],
    ) -> Vec<TagValue> {
        lines
            .iter()
            .map(|line| {
                definition.regex.captures(line).and_then(|captures| {
                    captures.get(1).map_or(None, |m| {
                        Engine::transform_chunk(&lua, &definition.transform, m.as_str()).ok()
                    })
                })
            })
            .collect()
    }

    fn cache_lines_from_disk(
        cache: &mut LineCache,
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
