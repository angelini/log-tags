use std::fs;
use std::io;
use std::io::prelude::*;
use std::fmt;

use regex::Regex;

#[derive(Debug)]
enum Error {
    Lua(rlua::Error),
    Io(std::io::Error),
    Regex(regex::Error),
}

impl From<rlua::Error> for Error {
    fn from(err: rlua::Error) -> Error {
        Error::Lua(err)
    }
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
            Error::Lua(ref err) => write!(f, "{}", err),
            Error::Io(ref err) => write!(f, "{}", err),
            Error::Regex(ref err) => write!(f, "{}", err),
        }
    }
}

type Result<T> = std::result::Result<T, Error>;

#[derive(Default)]
struct LuaScript {
    setup: Option<String>,
    eval: Option<String>,
}

impl LuaScript {
    fn new<S: Into<String>>(setup: Option<S>, eval: Option<S>) -> LuaScript {
        LuaScript {
            setup: setup.map(|s| s.into()),
            eval: eval.map(|s| s.into()),
        }
    }
}

struct TagDefinition {
    r: Regex,
    name: String,
    transform: LuaScript,
}

impl TagDefinition {
    fn new<S: Into<String>>(name: S, pattern: &str, transform: LuaScript) -> Result<TagDefinition> {
        Ok(TagDefinition {
            r: Regex::new(&pattern)?,
            name: name.into(),
            transform: transform,
        })
    }
}

fn transform_chunk(lua: &rlua::Lua, transform: &LuaScript, chunk: &str) -> Result<String> {
    match transform.eval {
        Some(ref eval_src) => {
            Ok(lua.context(|lua_ctx| {
                lua_ctx.globals().set("chunk", chunk)?;
                lua_ctx.load(eval_src).eval()
            })?)
        }
        None => Ok(chunk.to_string()),
    }

}

fn line_tags(lua: &rlua::Lua, line: &str, definitions: &[TagDefinition]) -> Vec<Option<String>> {
    definitions
        .iter()
        .map(|definition| {
            definition.r.captures(line).and_then(|captures| {
                captures.get(1).map_or(None, |m| {
                    transform_chunk(lua, &definition.transform, m.as_str()).ok()
                })
            })
        })
        .collect()
}

fn run_setups(lua: &rlua::Lua, definitions: &[TagDefinition]) -> Result<()> {
    Ok(lua.context(|lua_ctx| {
        definitions
            .iter()
            .map(|definition| {
                if let Some(ref setup) = definition.transform.setup {
                    lua_ctx.load(setup).eval()?;
                }
                Ok(())
            })
            .collect::<rlua::Result<()>>()
    })?)
}

fn main() -> Result<()> {
    let tag_defs =
        vec![
            TagDefinition::new(
                "date",
                r#"\[(\S+ \S+ \d+ \d+:\d+:\d+ \d+)\]"#,
                LuaScript::default()
            )?,
            TagDefinition::new(
                "parsed-date",
                r#"\[(\S+ \S+ \d+ \d+:\d+:\d+ \d+)\]"#,
                LuaScript::new(
                    Some(
                        r#"
months = {}
months["Dec"] = 12

function parse_month (m)
  return months[m]
end
"#,
                    ),
                    Some(
                        r#"
string.sub(chunk, 9, 10) .. "-" .. parse_month(string.sub(chunk, 5, 7)) .. "-" .. string.sub(chunk, 21, 24)
"#,
                    ),
                )
            )?,
            TagDefinition::new("error-level", r#"\[(error|notice)\]"#, LuaScript::default())?,
        ];

    let lua = rlua::Lua::new();

    run_setups(&lua, &tag_defs)?;

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
