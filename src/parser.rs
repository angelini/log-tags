use nom::{
    branch::alt,
    bytes::complete::tag,
    bytes::streaming::take_until,
    character::complete::{alpha1, char, digit1, multispace0},
    combinator::{cut, map},
    error::VerboseError,
    multi::separated_list,
    sequence::{delimited, preceded, terminated, tuple},
    IResult,
};

type Err<'a> = VerboseError<&'a str>;

#[derive(Clone, Debug)]
pub enum Expression {
    Application(String, Vec<Expression>),
    Int(usize),
    String(String),
    Symbol(String),
}

fn parse_int<'a>(i: &'a str) -> IResult<&'a str, usize, Err<'a>> {
    map(digit1, |int_str: &str| int_str.parse::<usize>().unwrap())(i)
}

fn parse_symbol<'a>(i: &'a str) -> IResult<&'a str, String, Err<'a>> {
    map(preceded(tag("'"), cut(alpha1)), |sym_str: &str| {
        sym_str.to_string()
    })(i)
}

fn parse_str<'a>(i: &'a str) -> IResult<&'a str, &str, Err<'a>> {
    take_until("\"")(i)
}

fn parse_double_quoted_str<'a>(i: &'a str) -> IResult<&'a str, String, Err<'a>> {
    map(
        preceded(char('\"'), cut(terminated(parse_str, char('\"')))),
        |s: &str| s.to_string(),
    )(i)
}

fn parse_arguments<'a>(i: &'a str) -> IResult<&'a str, Vec<Expression>, Err<'a>> {
    delimited(
        char('('),
        separated_list(
            preceded(multispace0, tag(",")),
            preceded(multispace0, parse_expression),
        ),
        cut(preceded(multispace0, char(')'))),
    )(i)
}

fn parse_application<'a>(i: &'a str) -> IResult<&'a str, (&'a str, Vec<Expression>), Err<'a>> {
    tuple((alpha1, parse_arguments))(i)
}

pub fn parse_expression<'a>(i: &'a str) -> IResult<&'a str, Expression, Err<'a>> {
    alt((
        map(parse_application, |(func, args)| {
            Expression::Application(func.to_string(), args)
        }),
        map(parse_int, Expression::Int),
        map(parse_double_quoted_str, Expression::String),
        map(parse_symbol, Expression::Symbol),
    ))(i)
}
