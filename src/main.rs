use std::fs;
use std::io;
use std::io::prelude::*;
use std::fmt;

use regex::Regex;

#[derive(Debug)]
enum Error {
    Io(std::io::Error),
    Regex(regex::Error),
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Error {
        Error::Io(err)
    }
}

impl From<regex::Error> for Error {
    fn from(err: regex::Error) -> Error {
        Error::Regex(err)
    }
}


impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::Io(ref err) => write!(f, "{}", err),
            Error::Regex(ref err) => write!(f, "{}", err),
        }
    }
}

struct TagDefinition {
    r: Regex,
    name: String,
    transform: Option<String>,
}

impl TagDefinition {
    fn new<S: Into<String>>(
        name: S,
        pattern: &str,
        transform: Option<S>,
    ) -> Result<TagDefinition, Error> {
        Ok(TagDefinition {
            r: Regex::new(&pattern)?,
            name: name.into(),
            transform: transform.map(|t| t.into()),
        })
    }
}

fn transform_chunk(lua: &rlua::Lua, transform: &str, chunk: &str) -> rlua::Result<String> {
    lua.context(|lua_ctx| {
        lua_ctx.globals().set("chunk", chunk)?;
        lua_ctx.load(transform).eval()
    })
}

fn line_tags(lua: &rlua::Lua, line: &str, definitions: &[TagDefinition]) -> Vec<Option<String>> {
    definitions
        .iter()
        .map(|definition| if let Some(captures) = definition
            .r
            .captures(line)
        {
            captures.get(1).map_or(
                None,
                |m| match &definition.transform {
                    Some(transform) => {
                        match transform_chunk(lua, &transform, m.as_str()) {
                            Ok(value) => Some(value),
                            Err(_) => None,
                        }
                    }
                    None => Some(m.as_str().to_string()),
                },
            )
        } else {
            None
        })
        .collect()
}

fn main() -> Result<(), Error> {
    let tag_defs = vec![
        TagDefinition::new("date", r#"\[(\S+ \S+ \d+ \d+:\d+:\d+ \d+)\]"#, None)?,
        TagDefinition::new(
            "parsed-date",
            r#"\[(\S+ \S+ \d+ \d+:\d+:\d+ \d+)\]"#,
            Some(
                r#"
string.sub(chunk, 9, 10) .. "-" .. string.sub(chunk, 5, 7) .. "-" .. string.sub(chunk, 21, 24)
"#,
            )
        )?,
        TagDefinition::new("error-level", r#"\[(error|notice)\]"#, None)?,
    ];

    let lua = rlua::Lua::new();

    let file = fs::File::open("apache.log")?;
    let reader = io::BufReader::new(file);

    for line_result in reader.lines().take(5) {
        match line_result {
            Ok(line) => {
                println!("line: {:?}", line);
                for (def, result) in tag_defs.iter().zip(line_tags(&lua, &line, &tag_defs)) {
                    println!("  {: <12} {:?}", def.name, result)
                }
            }
            Err(e) => println!("error: {:?}", e),
        }
    }
    Ok(())
}
