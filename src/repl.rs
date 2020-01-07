use rustyline::error::ReadlineError;

use crate::engine::Engine;
use crate::error::Result;
use crate::interpreter::{CursorState, Interpreter};

pub fn start(mut engine: &mut Engine, interpreter: &mut Interpreter) -> Result<()> {
    let mut rl = rustyline::Editor::<()>::new();
    if rl.load_history("history.txt").is_err() {
        println!("No previous history.");
    }

    let mut state = CursorState::Root;

    loop {
        let readline = match state {
            CursorState::Root => rl.readline("> "),
            CursorState::Pipelined => rl.readline("| "),
            CursorState::MultiLine => rl.readline(""),
        };

        match readline {
            Ok(segment) => {
                state = interpreter.add_line_segment(&segment)?;

                if state == CursorState::Root {
                    for line in interpreter.execute(&mut engine)? {
                        println!("  {}", line);
                    }
                    println!();
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
            Err(err) => return Err(err.into())
        }
    }

    rl.save_history("history.txt").unwrap();
    Ok(())
}
