mod error;
mod engine;
mod parser;
mod repl;

use error::Result;
use engine::Engine;

fn main() -> Result<()> {
    let mut engine = Engine::new();

//     let commands = vec![
//         Command::Load(path::Path::new("apache.log").to_path_buf()),
//         Command::Tag(
//             FileId(1),
//             "level".to_string(),
//             r#"\[(error|notice)\]"#.to_string(),
//             LuaScript::default(),
//         ),
//         Command::Tag(
//             FileId(1),
//             "date".to_string(),
//             r#"\[(\S+ \S+ \d+ \d+:\d+:\d+ \d+)\]"#.to_string(),
//             LuaScript::default(),
//         ),
//         Command::Tag(
//             FileId(1),
//             "parsed_date".to_string(),
//             r#"\[(\S+ \S+ \d+ \d+:\d+:\d+ \d+)\]"#.to_string(),
//             LuaScript::new(
//                 Some(
//                     r#"
// months = {}
// months["Dec"] = 12

// function parse_month (m)
//   return months[m]
// end
// "#,),
//                 Some(
//                     r#"
// string.sub(chunk, 9, 10) .. "-" .. parse_month(string.sub(chunk, 5, 7)) .. "-" .. string.sub(chunk, 21, 24)
// "#,),
//             )
//         ),
//         Command::Take(FileId(1), 10),
//     ];

    match repl::start(&mut engine) {
        Ok(_) => println!("done"),
        Err(e) => println!("{}", e),
    };
    Ok(())
}
