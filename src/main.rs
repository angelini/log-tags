mod base;
mod engine;
mod error;
mod interpreter;
mod parser;
mod repl;

use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;

use clap::{App, Arg};

use engine::Engine;
use error::Result;
use interpreter::{CursorState, Interpreter};

fn main() -> Result<()> {
    let args = App::new("Log-Tags")
        .arg(
            Arg::with_name("file")
                .short("f")
                .help("Parse and run expressions in this file before the interactive REPL")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("debug")
                .short("d")
                .help("Track and print execution stats"),
        )
        .get_matches();

    let mut engine = if args.is_present("debug") { Engine::new_debug() } else { Engine::new() };
    let mut interpreter = Interpreter::new();

    if let Some(file_name) = args.value_of("file") {
        let file = BufReader::new(File::open(file_name)?);
        let mut state = CursorState::Root;

        for segment in file.lines().map(|l| l.unwrap()) {
            if !segment.is_empty() {
                println!("{}", segment);
            }

            match state {
                CursorState::Root => {
                    if segment.len() > 2 && &segment[0..2] == "> " {
                        state = interpreter.add_line_segment(&segment[2..])?
                    }
                }
                CursorState::Pipelined => {
                    if segment.len() > 2 && &segment[0..2] == "| " {
                        state = interpreter.add_line_segment(&segment[2..])?
                    }
                    if segment.is_empty() {
                        println!();
                        for line in interpreter.execute(&mut engine)? {
                            println!("  {}", line);
                        }
                        println!();
                        state = CursorState::Root;
                    }
                }
                CursorState::MultiLine => state = interpreter.add_line_segment(&segment)?,
            }
        }

        println!();
        for line in interpreter.execute(&mut engine)? {
            println!("  {}", line);
        }
        println!();
    }

    repl::start(&mut engine, &mut interpreter).map_err(|e| {
        println!("{}", e);
        e
    })
}
