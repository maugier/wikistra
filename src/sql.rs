//! Streaming SQL tokenizer for loading Wikipedia mysql dumps

use std::{fs::File, path::Path, io::{Error, BufReader, BufRead, Bytes, Read}, iter::{Peekable, Fuse}};
use flate2::bufread::GzDecoder;
use smol_str::SmolStr;
use thiserror::Error;
use utf8_decode::UnsafeDecoder;

//pub mod regex;

pub struct Loader {
    source: Peekable<Fuse<Tokenizer>>,
    expecting_tuple: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    String(String),
    Integer(i64),
    Float(f64),
    Null,
}

#[derive(Error, Debug)]
#[error("type error")]
pub struct TypeError(Value);

impl Value {
    pub fn string(self) -> Result<String, TypeError> {
        match self {
            Value::String(s) => Ok(s),
            other => Err(TypeError(other)),
        }
    }

    pub fn int(self) -> Result<i64, TypeError> {
        match self {
            Value::Integer(n) => Ok(n),
            other => Err(TypeError(other)),
        }
    }

}

#[derive(Debug, Error)]
pub enum LoaderError {
    #[error("tokenizer error: {0:?}")]
    Tokenizer(#[from] TokenizerError),
    #[error("i/o: {0:?}")]
    IO(#[from] std::io::Error),
    #[error("syntax error: unexpected token {0:?}, expecting {1}")]
    Syntax(Token, SmolStr),
    #[error("EOF")]
    EOF,
}

impl Loader {
    pub fn load_gz_file<P: AsRef<Path> + ?Sized>(path: &P) -> Result<Self, LoaderError> {
        let compressed = BufReader::new(File::open(path)?);
        let source = BufReader::new(GzDecoder::new(compressed));
        Self::load(source)
    }

    pub fn load<R: BufRead + 'static>(mut source: R) -> Result<Self, LoaderError> {

        let mut linebuf = String::new();
        let linebuf = &mut linebuf;

        loop {
            source.read_line(linebuf)?;
            if linebuf.contains("DISABLE KEYS") { break }
        }

        let source = tokenize(source).fuse().peekable();
        Ok(Self { source, expecting_tuple: false })

    }

    fn peek(&mut self) -> Result<Option<&Token>, TokenizerError> {
        // This unwrap fuckery is required because capturing in the pattern can't pass borrowck
        match self.source.peek() {
            Some(Err(_)) => Err(self.source.next().unwrap().unwrap_err()),
            Some(Ok(_)) => Ok(Some(self.source.peek().unwrap().as_ref().unwrap())),
            None => Ok(None),
        }
    }

    fn maybe_token(&mut self) -> Result<Option<Token>, TokenizerError> {
        self.source.next().transpose()
    }

    fn token(&mut self) -> Result<Token, LoaderError> {
        self.maybe_token()?.ok_or(LoaderError::EOF)
    }

    fn expect(&mut self, token: Token) -> Result<(), LoaderError> {
        match self.token()? {
            t if t == token => Ok(()),
            other => Err(LoaderError::Syntax(other, format!("{:?}", token).into())),
        }
    }

    fn expect_insert_into(&mut self) -> Result<(), LoaderError> {
        self.expect(sym("INSERT"))?;
        self.expect(sym("INTO"))?;
        self.token()?;
        self.expect(sym("VALUES"))
    }

    fn expect_tuple(&mut self) -> Result<Vec<Value>, LoaderError> {
        self.expect(sym("("))?;
        let mut tuple = vec![];

        loop {
            let v = self.token()?
                .value().map_err(|t| LoaderError::Syntax(t, "a literal value".into()))?;
            tuple.push(v);

            match self.token()? {
                Token::Symbol(s) if s == "," => continue,
                Token::Symbol(s) if s == ")" => break,
                other => return Err(LoaderError::Syntax(other, "`)` or `,`".into()))
            }
        }

        match self.token()? {
            Token::Symbol(s) if s == "," => (),
            Token::Symbol(s) if s == ";" => {
                self.expecting_tuple = false;
            }
            other => return Err(LoaderError::Syntax(other, "`,` or `;`".into()))
        }

        Ok(tuple)
    }

    fn next_tuple(&mut self) -> Result<Option<Vec<Value>>, LoaderError> {
        if !self.expecting_tuple {
            if self.peek()? == Some(&sym("INSERT")) {
                self.expect_insert_into()?;
                self.expecting_tuple = true;
            } else {
                return Ok(None)
            }
        }

        self.expect_tuple().map(Option::Some)
    }


}


impl Iterator for Loader {
    type Item = Result<Vec<Value>, LoaderError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_tuple().transpose()
    }
}


/// Tokenization errors
#[derive(Debug, Error)]
pub enum TokenizerError {
    #[error("i/o error: {0:?}")]
    IO(#[from] std::io::Error),
    #[error("parsing integer: {0:?}")]
    ParseInt(#[from] std::num::ParseIntError),
    #[error("parsing float: {0:?}")]
    ParseFloat(#[from] std::num::ParseFloatError),
    #[error("unexpected end of stream, expected {expected}")]
    Eof { expected: char },
    #[error("incomplete string")]
    IncompleteString,
    #[error("invalid escape sequence `\\{0}`")]
    InvalidEscape(char)
}

/// Output type for the tokenizer
#[derive(Debug, Clone, PartialEq)]
pub enum Token {
    /// keywords, table names (including quoted), or operators
    Symbol(SmolStr),

    /// Values
    Value(Value),
}

pub const fn sym(s: &str) -> Token { Token::Symbol(SmolStr::new_inline(s)) }
#[cfg(test)]
pub fn strt<S: Into<String>>(s: S) -> Token { Token::Value(Value::String(s.into()))}
#[cfg(test)]
pub const fn numt(n: i64) -> Token { Token::Value( Value::Integer(n) ) }

impl Token {
    fn value(self) -> Result<Value, Token> {
        match self {
            Token::Value(v) => Ok(v),
            other => Err(other),
        }
    }
}

/// A streaming SQL tokenizer. Wraps a byte stream and provides iteration over tokens.
pub struct Tokenizer {
    source: Peekable<UnsafeDecoder<Bytes<Box<dyn Read>>>>,
    buffer: String,
}

impl Tokenizer {

    /// Create a tokenizer reading from a given source
    pub fn new(source: Box<dyn Read>) -> Self {
        Self { source: UnsafeDecoder::new(source.bytes()).peekable(), buffer: String::with_capacity(4096) }
    }

    /// Consume white space at the start of the stream
    fn skip_white(&mut self) -> Result<(), Error> {
        while let Some(Ok(c)) = self.source.peek() {
            if c.is_ascii_whitespace() {
                self.source.next();
            } else {
                break
            }
        }
        Ok(())
    }

    /// Read into the internal buffer until a stop character failing the predicate is reached.
    /// 
    /// The internal buffer is accessible as `self.buffer` but is also returned as a reference
    /// for convenience.
    /// Does not consume the stop character.
    fn collect_while<P>(&mut self, p: P) -> Result<&str, TokenizerError>
        where P: Fn(char) -> bool
    {
        loop {
            match self.source.peek() {
                Some(Err(_)) => {
                    self.source.next().unwrap()?;
                },
                Some(Ok(c)) if p(*c) => {
                    self.buffer.push(*c as char);
                    self.source.next();
                },
                _ => {
                    return Ok(&self.buffer)
                }
            }
        }       
    }

    /// Parse a number
    fn parse_number(&mut self) -> Result<Token, TokenizerError> {
        self.buffer.clear();
        self.collect_while(|c| c == '-')?;
        self.collect_while(|c| c.is_ascii_digit())?;

        let v = if self.source.peek().and_then(|t| t.as_ref().ok()) == Some(&'.') {

            self.buffer.push(self.source.next().unwrap().unwrap() as char);
            self.collect_while(|c| c.is_ascii_digit())?;
            Value::Float(self.buffer.parse()?)

        } else {
            Value::Integer(self.buffer.parse()?)
        };

        Ok(Token::Value(v))
    }

    /// Parse an identifier
    fn parse_identifier(&mut self) -> Result<Token, TokenizerError> {
        self.buffer.clear();
        self.collect_while(|c| c.is_ascii_alphanumeric())?;

        let token = if self.buffer == "NULL" {
            Token::Value(Value::Null)
        } else {
            Token::Symbol(SmolStr::new(&self.buffer))
        };

        Ok(token)

    }

    /// Parse a quoted string
    fn parse_string(&mut self) -> Result<Token, TokenizerError> {
        self.buffer.clear();

        loop {
            self.source.next(); // initial ' 

            loop {
                let c = self.source.next().ok_or(TokenizerError::Eof { expected: '\'' })??;

                match c {
                    '\\' => match self.source.next().ok_or(TokenizerError::IncompleteString)?? {
                        c@('\'' | '\\' | '"') => self.buffer.push(c),
                        other => return Err(TokenizerError::InvalidEscape(other))
                    },
                    '\'' => break,
                    other => self.buffer.push(other)
                }

            }

            if let Some(Ok('\'')) = self.source.peek() { // Double quote escape
                self.buffer.push('\'')
            } else { // actual end of quote
                return Ok(Token::Value(Value::String(self.buffer.clone())))
            }
        }
        
    }

    /// Parse a quoted identifier
    fn parse_quoted_identifier(&mut self) -> Result<Token, TokenizerError> {
        self.buffer.clear();
        self.source.next();
        self.collect_while(|c| c != '`')?;
        self.source.next().ok_or(TokenizerError::Eof { expected: '`' })??;
        Ok(Token::Symbol(SmolStr::from(&self.buffer)))
    }

    fn next_token(&mut self) -> Result<Option<Token>, TokenizerError> {
        self.skip_white()?;
        let next = match self.source.peek() { 
            None => return Ok(None),
            Some(Err(_)) => self.source.next().unwrap()?,
            Some(Ok(c)) => *c,
        };
        
        let tok = match next {
            c if c.is_ascii_digit() => self.parse_number(),
            '-' => self.parse_number(),
            c if c.is_ascii_alphabetic() => self.parse_identifier(),
            '`' => self.parse_quoted_identifier(),
            '\'' => self.parse_string(),
            c => {
                self.source.next();
                self.buffer.clear();
                Ok(Token::Symbol(SmolStr::new_inline(c.encode_utf8(&mut [0; 4]))))
            }           
        }?;

        Ok(Some(tok))

    }

}

impl Iterator for Tokenizer{
    type Item = Result<Token, TokenizerError>;


    fn next(&mut self) -> Option<Self::Item> {
        self.next_token().transpose() 
    }

}

/// Create a tokenizer over the given source
pub fn tokenize<R: Read + 'static>(source: R) -> Tokenizer {
    Tokenizer::new(Box::new(source))
}

#[test]
fn sample_tokenization() {

    //let sym = |s| { Token::Symbol(SmolStr::new_inline(s)) };

    let sample_statement = b"  INSERT   INTO `my table` VALUES (1,'l o l', 0), (2, 'o''escape', 'es\\\"ca\\\' ped'   )     ";

    let tokens: Result<Vec<_>,_> = tokenize(&sample_statement[..]).collect();
    let tokens = tokens.unwrap();

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