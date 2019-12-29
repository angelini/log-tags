use std::collections::HashMap;
use std::path::PathBuf;

use nom::error::{convert_error, VerboseError};
use rustyline::error::ReadlineError;

use crate::engine::{Command, Engine, Id, Output};
use crate::error::{Error, Result, SyntaxError};
use crate::parser::{self, Expression};

#[derive(Debug)]
pub enum Application {
    Load(String, String),

    Regex(String, String),
    RegexPipe(String),

    Tag(String, String),
    TagPipe(String),

    Take(String, usize),
    TakePipe(usize),

    Transform(String, String, Option<String>),
    TransformPipe(String, Option<String>),
}

impl Application {
    fn from_expression(exp: &Expression) -> std::result::Result<Application, SyntaxError> {
        if let Expression::Application(func, args) = exp {
            match (func.as_str(), args.as_slice()) {
                ("load", [Expression::Symbol(file), Expression::String(path)]) => {
                    Ok(Application::Load(file.clone(), path.clone()))
                }

                ("regex", [Expression::Symbol(tag), Expression::String(path)]) => {
                    Ok(Application::Regex(tag.clone(), path.clone()))
                }
                ("regex", [Expression::String(path)]) => Ok(Application::RegexPipe(path.clone())),

                ("tag", [Expression::Symbol(file), Expression::Symbol(tag)]) => {
                    Ok(Application::Tag(file.clone(), tag.clone()))
                }
                ("tag", [Expression::Symbol(tag)]) => Ok(Application::TagPipe(tag.clone())),

                ("take", [Expression::Symbol(log), Expression::Int(count)]) => {
                    Ok(Application::Take(log.clone(), *count))
                }
                ("take", [Expression::Int(count)]) => Ok(Application::TakePipe(*count)),

                (
                    "transform",
                    [Expression::Symbol(tag), Expression::String(transform), Expression::String(setup)],
                ) => Ok(Application::Transform(
                    tag.clone(),
                    transform.clone(),
                    Some(setup.clone()),
                )),
                ("transform", [Expression::Symbol(tag), Expression::String(transform)]) => {
                    Ok(Application::Transform(tag.clone(), transform.clone(), None))
                }
                ("transform", [Expression::String(transform), Expression::String(setup)]) => Ok(
                    Application::TransformPipe(transform.clone(), Some(setup.clone())),
                ),
                ("transform", [Expression::String(transform)]) => {
                    Ok(Application::TransformPipe(transform.clone(), None))
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

    fn execute(
        &mut self,
        engine: &mut Engine,
        func: Application,
        target: Option<Id>,
    ) -> Result<Output> {
        match func {
            Application::Load(name, path_str) => {
                let output = engine.run_command(&Command::Load(PathBuf::from(path_str)))?;
                *self.symbols.entry(name.to_string()).or_insert(output.id) = output.id;
                Ok(output)
            }
            Application::Regex(tag_name, regex) => {
                if let Some(Id::Tag(tag_id)) = self.symbols.get(&tag_name) {
                    engine.run_command(&Command::Regex(*tag_id, regex))
                } else {
                    Err(Error::TagDoesNotExist(tag_name))
                }
            }
            Application::RegexPipe(regex) => {
                if let Some(Id::Tag(tag_id)) = target {
                    engine.run_command(&Command::Regex(tag_id, regex))
                } else {
                    Err(Error::InvalidTarget(format!("{:?}", target)))
                }
            }
            Application::Tag(file_name, tag_name) => {
                if let Some(Id::File(file_id)) = self.symbols.get(&file_name) {
                    let output =
                        engine.run_command(&Command::Tag(*file_id, tag_name.clone()))?;
                    *self
                        .symbols
                        .entry(tag_name)
                        .or_insert(output.id) = output.id;
                    Ok(output)
                } else {
                    Err(Error::FileNotLoaded(file_name))
                }
            }
            Application::TagPipe(tag_name) => {
                if let Some(Id::File(file_id)) = target {
                    let output =
                        engine.run_command(&Command::Tag(file_id, tag_name.to_string()))?;
                    *self
                        .symbols
                        .entry(tag_name.to_string())
                        .or_insert(output.id) = output.id;
                    Ok(output)
                } else {
                    Err(Error::InvalidTarget(format!("{:?}", target)))
                }
            }
            Application::Take(file_name, count) => {
                if let Some(Id::File(file_id)) = self.symbols.get(&file_name) {
                    engine.run_command(&Command::Take(*file_id, count))
                } else {
                    Err(Error::FileNotLoaded(file_name))
                }
            }
            Application::TakePipe(count) => {
                if let Some(Id::File(file_id)) = target {
                    engine.run_command(&Command::Take(file_id, count))
                } else {
                    Err(Error::InvalidTarget(format!("{:?}", target)))
                }
            }
            Application::Transform(tag_name, transform, setup) => {
                if let Some(Id::Tag(tag_id)) = self.symbols.get(&tag_name) {
                    engine.run_command(&Command::Transform(*tag_id, transform, setup))
                } else {
                    Err(Error::TagDoesNotExist(tag_name))
                }
            }
            Application::TransformPipe(transform, setup) => {
                if let Some(Id::Tag(tag_id)) = target {
                    engine.run_command(&Command::Transform(tag_id, transform, setup))
                } else {
                    Err(Error::InvalidTarget(format!("{:?}", target)))
                }
            }
        }
    }
}

fn parse_line(line: String) -> Result<Option<Application>> {
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

pub fn start(mut engine: &mut Engine) -> Result<()> {
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

                match parse_line(line)? {
                    Some(func) => buffer.push(func),
                    None => {
                        let mut target = None;
                        for func in buffer {
                            println!("Executing: {:?}", func);
                            let output = repl.execute(&mut engine, func, target)?;
                            target = Some(output.id);

                            println!("Output:");
                            for line in output.lines {
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
