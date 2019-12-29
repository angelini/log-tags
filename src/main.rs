mod error;
mod engine;
mod parser;
mod repl;

use error::Result;
use engine::Engine;

fn main() -> Result<()> {
    let mut engine = Engine::new();
    repl::start(&mut engine)
        .map_err(|e| {
            println!("{}", e);
            e
        })
}
