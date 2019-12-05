use peg;
use rustyline::error::ReadlineError;

use crate::error::Error;
use crate::engine::Command;

peg::parser!(grammar list_parser() for str {
    rule ws() = quiet!{[' ' | '\n' | '\t']*}

    pub rule str() -> String
      = "\"" s:$(['a'..='z' | 'A'..='Z' | '0'..='9' | ' ' | '/']+) "\"" { s.to_string() }

    rule load() -> 

    rule number() -> u32
      = n:$(['0'..='9']+) { n.parse().unwrap() }

    pub rule list() -> Vec<u32>
      = "[" l:number() ** "," "]" { l }
});

pub fn start() -> Result<(), Error> {
    println!("{:?}", list_parser::list("[1,1,2,3,5,8]"));
    println!("{:?}", list_parser::str(r#""hello""#));

    let mut rl = rustyline::Editor::<()>::new();
    if rl.load_history("history.txt").is_err() {
        println!("No previous history.");
    }
    loop {
        let readline = rl.readline(">> ");
        match readline {
            Ok(line) => {
                rl.add_history_entry(line.as_str());
                println!("Line: {}", line);
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
