use std::{rc::Rc, ops::Deref};

use super::{Value, Token, sym};

#[cfg(test)]
use super::{strt, numt};

use once_cell::sync::{OnceCell, Lazy};
use regex::{Regex, Captures};

const VALUE_RE: &str = r#"'(?:[\].|[^'])*'|\d+(?:[.]\d+)|NULL"#;
const INSERT_RE: &str = "^INSERT INTO `[^`]*` VALUES (.*);$";

static INSERT_MATCHER: Lazy<Regex> = Lazy::new(|| Regex::new(INSERT_RE).unwrap());
static TUPLE_MATCHER: Lazy<Regex> = Lazy::new(|| Regex::new(&tuple_re()).unwrap());
static VALUE_MATCHER: Lazy<Regex> = Lazy::new(|| Regex::new(VALUE_RE).unwrap());

fn tuple_re() -> String {
    format!("[(]{0}(:?,{0})*[)],?", VALUE_RE)
}

#[derive(Clone,Copy)]
struct Tuple<'s>(&'s str);

impl <'s> IntoIterator for &Tuple<'s> {
    type Item = &'s str;

    type IntoIter = Box<dyn Iterator<Item=Self::Item> + 's>;

    fn into_iter(self) -> Self::IntoIter {
        Box::new(VALUE_MATCHER.find_iter(self.0)
        .map(|m| m.as_str()))
    }
}

impl <'s> Tuple<'s> {
    pub fn values(&self) -> Vec<&'s str> {
        match_tuple(self)
    }
}

fn match_line(s: &str) -> Option<Vec<Tuple<'_>>> {

    let blob = INSERT_MATCHER.captures(s)?
                       .get(1).unwrap().as_str();

    let slices = TUPLE_MATCHER.find_iter(blob)
         .map(|tup| { Tuple(tup.as_str()) })
         .collect();

    Some(slices)

}

fn match_tuple<'s>(tuple: &Tuple<'s>) -> Vec<&'s str> {
    VALUE_MATCHER.find_iter(&tuple.0)
        .map(|m| m.as_str())
        .collect()
}


#[test]
fn sample_tokenization() {

    //let sym = |s| { Token::Symbol(SmolStr::new_inline(s)) };

    let sample_statement = "INSERT INTO `my table` VALUES (1,'l o l',0),(2,'o\\''escape','es\\\"ca\\\' ped',0.5,NULL);";

    let tokens: Vec<Token> = todo!();

    assert_eq!(&tokens, 
        &[sym("INSERT"),
          sym("INTO"),
          sym("my table"),
          sym("VALUES"),
          sym("("),
          numt(1),
          sym(","),
          strt("l o l"),
          sym(","),
          numt(0),
          sym(")"),
          sym(","),
          sym("("),
          numt(2),
          sym(","),
          strt("o'escape"),
          sym(","),
          strt("es\"ca' ped"),
          sym(")"),

        ]
    )

}