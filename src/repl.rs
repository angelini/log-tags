use std::collections::HashMap;
use std::path::PathBuf;

use nom::error::convert_error;
use rustyline::error::ReadlineError;

use crate::base::{Comparator, Id};
use crate::engine::{Command, Engine, Output};
use crate::error::{Error, Result, SyntaxError};
use crate::parser::{self, Expression};

#[derive(Debug)]
pub enum Application {
    Load(String, String),

    Tag(String, String),
    TagPiped(String),

    Regex(String, String),
    RegexPiped(String),

    Transform(String, String, Option<String>),
    TransformPiped(String, Option<String>),

    DirectFilter(String, Comparator, String),
    DirectFilterNamed(String, String, Comparator, String),
    DirectFilterPiped(Comparator, String),
    DirectFilterPipedNamed(String, Comparator, String),

    ScriptedFilter(String, String, Option<String>),
    ScriptedFilterNamed(String, String, String, Option<String>),
    ScriptedFilterPiped(String, Option<String>),
    ScriptedFilterPipedNamed(String, String, Option<String>),

    Take(String, usize),
    TakePipe(usize),
}

impl Application {
    #[rustfmt::skip]
    fn from_expression(
        exp: &Expression,
        is_pipelined: bool,
    ) -> std::result::Result<Application, SyntaxError> {
        if let Expression::Application(func, args) = exp {
            match (func.as_str(), args.as_slice()) {
                ("load",
                 [Expression::Symbol(file), Expression::String(path)]) => {
                    Ok(Application::Load(file.clone(), path.clone()))
                }

                ("tag",
                 [Expression::Symbol(file), Expression::Symbol(tag)]) => {
                    Ok(Application::Tag(file.clone(), tag.clone()))
                }
                ("tag",
                 [Expression::Symbol(tag)]) => {
                    Ok(Application::TagPiped(tag.clone()))
                }

                ("regex",
                 [Expression::Symbol(tag), Expression::String(path)]) => {
                    Ok(Application::Regex(tag.clone(), path.clone()))
                }
                ("regex",
                 [Expression::String(path)]) => {
                    Ok(Application::RegexPiped(path.clone()))
                }

                ("transform",
                 [Expression::Symbol(tag), Expression::String(transform), Expression::String(setup)]) => {
                    Ok(Application::Transform(tag.clone(), transform.clone(), Some(setup.clone())))
                }
                ("transform",
                 [Expression::Symbol(tag), Expression::String(transform)]) => {
                    Ok(Application::Transform(tag.clone(), transform.clone(), None))
                }
                ("transform",
                 [Expression::String(transform), Expression::String(setup)]) => Ok(
                    Application::TransformPiped(transform.clone(), Some(setup.clone())),
                ),
                ("transform",
                 [Expression::String(transform)]) => {
                    Ok(Application::TransformPiped(transform.clone(), None))
                }

                ("filter",
                 [Expression::Symbol(tag_or_name), Expression::Comparator(comp), Expression::String(value)]) => {
                    if is_pipelined {
                        Ok(Application::DirectFilterPipedNamed(tag_or_name.clone(), *comp, value.clone()))
                    } else {
                        Ok(Application::DirectFilter(tag_or_name.clone(), *comp, value.clone()))
                    }
                }
                ("filter",
                 [Expression::Symbol(tag), Expression::Symbol(name), Expression::Comparator(comp), Expression::String(value)]) => {
                    Ok(Application::DirectFilterNamed(tag.clone(), name.clone(), *comp, value.clone()))
                }
                ("filter",
                 [Expression::Comparator(comp), Expression::String(value)]) => {
                    Ok(Application::DirectFilterPiped(*comp, value.clone()))
                }
                ("filter",
                 [Expression::Symbol(tag), Expression::String(test)]) => {
                    Ok(Application::ScriptedFilter(tag.clone(), test.clone(), None))
                }
                ("filter",
                 [Expression::Symbol(tag_or_name), Expression::String(test), Expression::String(setup)]) => {
                    if is_pipelined {
                        Ok(Application::ScriptedFilterPipedNamed(tag_or_name.clone(), test.clone(), Some(setup.clone())))
                    } else {
                        Ok(Application::ScriptedFilter(tag_or_name.clone(), test.clone(), Some(setup.clone())))
                    }
                }
                ("filter",
                 [Expression::Symbol(tag), Expression::Symbol(name), Expression::String(test), Expression::String(setup)]) => {
                    Ok(Application::ScriptedFilterNamed(tag.clone(), name.clone(), test.clone(), Some(setup.clone())))
                }
                ("filter", [Expression::String(test)]) => {
                    Ok(Application::ScriptedFilterPiped(test.clone(), None))
                }
                ("filter", [Expression::String(test), Expression::String(setup)]) => {
                    Ok(Application::ScriptedFilterPiped(test.clone(), Some(setup.clone())))
                }

                ("take", [Expression::Symbol(log), Expression::Int(count)]) => {
                    Ok(Application::Take(log.clone(), *count))
                }
                ("take", [Expression::Int(count)]) => {
                    Ok(Application::TakePipe(*count))
                }

                _ => Err(SyntaxError::UnknownFunction),
            }
        } else {
            Err(SyntaxError::ExpectedApplication)
        }
    }

    fn is_pipelined(&self) -> bool {
        match self {
            Application::Load(_, _) => false,
            Application::Tag(_, _) => false,
            Application::Regex(_, _) => false,
            Application::Transform(_, _, _) => false,
            Application::DirectFilter(_, _, _) => false,
            Application::DirectFilterNamed(_, _, _, _) => false,
            Application::ScriptedFilter(_, _, _) => false,
            Application::ScriptedFilterNamed(_, _, _, _) => false,
            Application::Take(_, _) => false,

            Application::TagPiped(_) => true,
            Application::RegexPiped(_) => true,
            Application::TransformPiped(_, _) => true,
            Application::DirectFilterPiped(_, _) => true,
            Application::DirectFilterPipedNamed(_, _, _) => true,
            Application::ScriptedFilterPiped(_, _) => true,
            Application::ScriptedFilterPipedNamed(_, _, _) => true,
            Application::TakePipe(_) => true,
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

    fn invoke(
        &mut self,
        engine: &mut Engine,
        app: Application,
        target: Option<Id>,
    ) -> Result<Output> {
        match app {
            Application::Load(name, path_str) => {
                let output = engine.run_command(&Command::Load(PathBuf::from(path_str)))?;
                *self.symbols.entry(name.to_string()).or_insert(output.id) = output.id;
                Ok(output)
            }

            Application::Tag(file_name, tag_name) => {
                if let Some(Id::File(file_id)) = self.symbols.get(&file_name) {
                    let output = engine.run_command(&Command::Tag(*file_id, tag_name.clone()))?;
                    *self.symbols.entry(tag_name).or_insert(output.id) = output.id;
                    Ok(output)
                } else {
                    Err(Error::FileNotLoaded(file_name))
                }
            }
            Application::TagPiped(tag_name) => {
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

            Application::Regex(tag_name, regex) => {
                if let Some(Id::Tag(tag_id)) = self.symbols.get(&tag_name) {
                    engine.run_command(&Command::Regex(*tag_id, regex))
                } else {
                    Err(Error::SymbolNotFound(tag_name))
                }
            }
            Application::RegexPiped(regex) => {
                if let Some(Id::Tag(tag_id)) = target {
                    engine.run_command(&Command::Regex(tag_id, regex))
                } else {
                    Err(Error::InvalidTarget(format!("{:?}", target)))
                }
            }

            Application::Transform(tag_name, transform, setup) => {
                if let Some(Id::Tag(tag_id)) = self.symbols.get(&tag_name) {
                    engine.run_command(&Command::Transform(*tag_id, transform, setup))
                } else {
                    Err(Error::SymbolNotFound(tag_name))
                }
            }
            Application::TransformPiped(transform, setup) => {
                if let Some(Id::Tag(tag_id)) = target {
                    engine.run_command(&Command::Transform(tag_id, transform, setup))
                } else {
                    Err(Error::InvalidTarget(format!("{:?}", target)))
                }
            }

            Application::DirectFilter(tag_name, comparator, value) => {
                if let Some(Id::Tag(tag_id)) = self.symbols.get(&tag_name) {
                    engine.run_command(&Command::DirectFilter(*tag_id, comparator, value))
                } else {
                    Err(Error::SymbolNotFound(tag_name))
                }
            }
            Application::DirectFilterNamed(tag_name, filter_name, comparator, value) => {
                if let Some(Id::Tag(tag_id)) = self.symbols.get(&tag_name) {
                    let output =
                        engine.run_command(&Command::DirectFilter(*tag_id, comparator, value))?;
                    *self.symbols.entry(filter_name).or_insert(output.id) = output.id;
                    Ok(output)
                } else {
                    Err(Error::SymbolNotFound(tag_name))
                }
            }
            Application::DirectFilterPiped(comparator, value) => {
                if let Some(Id::Tag(tag_id)) = target {
                    engine.run_command(&Command::DirectFilter(tag_id, comparator, value))
                } else {
                    Err(Error::InvalidTarget(format!("{:?}", target)))
                }
            }
            Application::DirectFilterPipedNamed(filter_name, comparator, value) => {
                if let Some(Id::Tag(tag_id)) = target {
                    let output =
                        engine.run_command(&Command::DirectFilter(tag_id, comparator, value))?;
                    *self.symbols.entry(filter_name).or_insert(output.id) = output.id;
                    Ok(output)
                } else {
                    Err(Error::InvalidTarget(format!("{:?}", target)))
                }
            }

            Application::ScriptedFilter(tag_name, test, setup) => {
                if let Some(Id::Tag(tag_id)) = self.symbols.get(&tag_name) {
                    engine.run_command(&Command::ScriptedFilter(*tag_id, test, setup))
                } else {
                    Err(Error::SymbolNotFound(tag_name))
                }
            }
            Application::ScriptedFilterNamed(tag_name, filter_name, test, setup) => {
                if let Some(Id::Tag(tag_id)) = self.symbols.get(&tag_name) {
                    let output =
                        engine.run_command(&Command::ScriptedFilter(*tag_id, test, setup))?;
                    *self.symbols.entry(filter_name).or_insert(output.id) = output.id;
                    Ok(output)
                } else {
                    Err(Error::SymbolNotFound(tag_name))
                }
            }
            Application::ScriptedFilterPiped(test, setup) => {
                if let Some(Id::Tag(tag_id)) = target {
                    engine.run_command(&Command::ScriptedFilter(tag_id, test, setup))
                } else {
                    Err(Error::InvalidTarget(format!("{:?}", target)))
                }
            }
            Application::ScriptedFilterPipedNamed(filter_name, test, setup) => {
                if let Some(Id::Tag(tag_id)) = target {
                    let output =
                        engine.run_command(&Command::ScriptedFilter(tag_id, test, setup))?;
                    *self.symbols.entry(filter_name).or_insert(output.id) = output.id;
                    Ok(output)
                } else {
                    Err(Error::InvalidTarget(format!("{:?}", target)))
                }
            }

            Application::Take(name, count) => {
                if let Some(id) = self.symbols.get(&name) {
                    engine.run_command(&Command::Take(*id, count))
                } else {
                    Err(Error::SymbolNotFound(name))
                }
            }
            Application::TakePipe(count) => {
                if let Some(Id::File(file_id)) = target {
                    engine.run_command(&Command::Take(Id::File(file_id), count))
                } else {
                    Err(Error::InvalidTarget(format!("{:?}", target)))
                }
            }
        }
    }
}

enum ParseState {
    Empty,
    Incomplete,
    Root(Application),
    Pipelined(Application),
}

fn parse_line(line: &str, is_pipelined: bool) -> Result<ParseState> {
    if !is_pipelined && line == "" {
        return Ok(ParseState::Empty);
    }

    match parser::parse_expression(&line) {
        Ok((_, exp)) => match Application::from_expression(&exp, is_pipelined) {
            Ok(func) if func.is_pipelined() => Ok(ParseState::Pipelined(func)),
            Ok(func) => Ok(ParseState::Root(func)),
            Err(err) => Err(Error::Syntax(err, line.to_string())),
        },
        Err(err) => match err {
            nom::Err::Error(e) | nom::Err::Failure(e) => {
                // FIXME: https://github.com/Geal/nom/issues/1027
                let default = format!("{:#?}", e);
                let converted = std::panic::catch_unwind(|| convert_error(&line, e));
                Err(Error::Parser(converted.unwrap_or(default)))
            }
            nom::Err::Incomplete(_) => Ok(ParseState::Incomplete),
        },
    }
}

pub fn start(mut engine: &mut Engine) -> Result<()> {
    let mut rl = rustyline::Editor::<()>::new();
    if rl.load_history("history.txt").is_err() {
        println!("No previous history.");
    }

    let mut repl = Repl::new();
    let mut line = String::new();
    let mut applications: Vec<Application> = vec![];

    loop {
        let readline = if applications.is_empty() {
            rl.readline("> ")
        } else {
            if line.len() == 0 {
                rl.readline("| ")
            } else {
                rl.readline("")
            }
        };

        match readline {
            Ok(segment) => {
                let is_continuation = !line.is_empty();
                line.push_str(&segment);

                match parse_line(&line, is_continuation)? {
                    ParseState::Incomplete => {
                        line.push_str("\n");
                    }
                    ParseState::Root(app) => {
                        if !applications.is_empty() {
                            return Err(Error::ApplicationOrder);
                        }
                        applications.push(app);
                        rl.add_history_entry(line);
                        line = String::new();
                    }
                    ParseState::Pipelined(app) => {
                        if applications.is_empty() {
                            return Err(Error::ApplicationOrder);
                        }
                        println!("app: {:?}", app);
                        applications.push(app);
                        rl.add_history_entry(line);
                        line = String::new();
                    }
                    ParseState::Empty => {
                        let mut target = None;
                        for app in applications {
                            let output = repl.invoke(&mut engine, app, target)?;
                            target = Some(output.id);

                            for line in output.lines {
                                println!("  {}", line);
                            }
                            println!();
                        }
                        applications = vec![];
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
