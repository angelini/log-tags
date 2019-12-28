use std::collections::HashMap;
use std::path::PathBuf;

use nom::error::{convert_error, VerboseError};
use rustyline::error::ReadlineError;

use crate::engine::{Command, Engine, Id, LuaScript};
use crate::error::{Error, SyntaxError};
use crate::parser::{self, Expression};

#[derive(Debug)]
pub enum Application {
    Load(String, String),
    Regex(String, String),
    Tag(String, String),
    Take(String, usize),
}

impl Application {
    fn from_expression(exp: &Expression) -> Result<Application, SyntaxError> {
        if let Expression::Application(func, args) = exp {
            match (func.as_str(), args.as_slice()) {
                ("load", [Expression::Symbol(name), Expression::String(path)]) => {
                    Ok(Application::Load(name.clone(), path.clone()))
                }
                ("regex", [Expression::Symbol(name), Expression::String(path)]) => {
                    Ok(Application::Regex(name.clone(), path.clone()))
                }
                ("tag", [Expression::Symbol(log), Expression::Symbol(name)]) => {
                    Ok(Application::Tag(log.clone(), name.clone()))
                }
                ("take", [Expression::Symbol(log), Expression::Int(count)]) => {
                    Ok(Application::Take(log.clone(), *count))
                }
                _ => Err(SyntaxError::UnknownFunction),
            }
        } else {
            Err(SyntaxError::ExpectedApplication)
        }
    }
}

struct Repl {
    symbols: HashMap<String, Id>,
}

impl Repl {
    fn new() -> Repl {
        Repl {
            symbols: HashMap::new(),
        }
    }

    fn execute(&mut self, engine: &mut Engine, func: &Application) -> Result<Vec<String>, Error> {
        match func {
            Application::Load(name, path_str) => {
                let (id, output) = engine.run_command(&Command::Load(PathBuf::from(path_str)))?;
                let file_id = id.ok_or_else(|| {
                    Error::ExpectedReturnedId(format!("Load {} {}", name, path_str))
                })?;
                *self.symbols.entry(name.to_string()).or_insert(file_id) = file_id;
                Ok(output)
            }
            Application::Tag(file_name, tag_name) => {
                if let Some(Id::File(file_id)) = self.symbols.get(file_name) {
                    let (id, output) = engine.run_command(&Command::Tag(
                        *file_id,
                        tag_name.to_string(),
                        "".to_string(),
                        LuaScript::default(),
                    ))?;
                    let tag_id = id.ok_or_else(|| {
                        Error::ExpectedReturnedId(format!("Tag {} {}", file_name, tag_name))
                    })?;
                    *self.symbols.entry(tag_name.to_string()).or_insert(tag_id) = tag_id;
                    Ok(output)
                } else {
                    Err(Error::FileNotLoaded(file_name.to_string()))
                }
            }
            Application::Take(file_name, count) => {
                if let Some(Id::File(file_id)) = self.symbols.get(file_name) {
                    let (_, output) = engine.run_command(&Command::Take(*file_id, *count))?;
                    Ok(output)
                } else {
                    Err(Error::FileNotLoaded(file_name.to_string()))
                }
            }
            _ => unimplemented!(),
        }
    }
}

fn parse_line(line: String) -> Result<Option<Application>, Error> {
    if line == "" {
        return Ok(None);
    }

    parser::parse_expression(&line)
        .map_err(|e: nom::Err<VerboseError<&str>>| match e {
            nom::Err::Error(e) | nom::Err::Failure(e) => {
                // FIXME: https://github.com/Geal/nom/issues/1027
                let default = format!("{:#?}", e);
                let converted = std::panic::catch_unwind(|| convert_error(&line, e));
                Error::Parser(converted.unwrap_or(default))
            }
            _ => panic!("Incomplete error"),
        })
        .and_then(|(_, exp)| match Application::from_expression(&exp) {
            Ok(func) => Ok(Some(func)),
            Err(err) => Err(Error::Syntax(err, line.clone())),
        })
}

pub fn start(mut engine: &mut Engine) -> Result<(), Error> {
    let mut rl = rustyline::Editor::<()>::new();
    if rl.load_history("history.txt").is_err() {
        println!("No previous history.");
    }

    let mut repl = Repl::new();
    let mut buffer: Vec<Application> = vec![];

    loop {
        let readline = if buffer.is_empty() {
            rl.readline("> ")
        } else {
            rl.readline("| ")
        };

        match readline {
            Ok(line) => {
                rl.add_history_entry(line.as_str());
                println!("Line: {}", line);

                match parse_line(line)? {
                    Some(func) => buffer.push(func),
                    None => {
                        for func in &buffer {
                            println!("Executing: {:?}", func);
                            let output = repl.execute(&mut engine, func)?;
                            println!("Output:");
                            for line in output {
                                println!("  {}", line);
                            }
                        }
                        buffer = vec![];
                    }
                }
            }
            Err(ReadlineError::Interrupted) => {
                println!("CTRL-C");
                break;
            }
            Err(ReadlineError::Eof) => {
                println!("CTRL-D");
                break;
            }
            Err(err) => {
                println!("Error: {:?}", err);
                break;
            }
        }
    }
    rl.save_history("history.txt").unwrap();
    Ok(())
}
