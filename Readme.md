# Log-Tags

An interactive REPL to help scan and parse through large log files.

Filter and tag lines using regular expressions and Lua.

## Example

```
> load('log, "apache.log")

  loaded "apache.log"

> tag('log, 'level)
| regex("\[(error|notice)\]")
| filter(==, "error")
| take(5)

  [Sun Dec 04 04:47:44 2005] [error] mod_jk child workerEnv in error state 6
      [level]         Some("error")
  [Sun Dec 04 04:51:18 2005] [error] mod_jk child workerEnv in error state 6
      [level]         Some("error")
  [Sun Dec 04 04:51:18 2005] [error] mod_jk child workerEnv in error state 6
      [level]         Some("error")
  [Sun Dec 04 05:04:04 2005] [error] mod_jk child workerEnv in error state 6
      [level]         Some("error")
  [Sun Dec 04 05:04:04 2005] [error] mod_jk child workerEnv in error state 7
      [level]         Some("error")
```
