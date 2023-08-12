//! Streaming SQL tokenizer for loading Wikipedia mysql dumps

use std::{fs::File, path::Path, io::{Error, BufReader, BufRead, Bytes, Read}, iter::Peekable};
use flate2::bufread::GzDecoder;
use smol_str::SmolStr;
use thiserror::Error;

pub struct Loader {
    source: BufReader<GzDecoder<BufReader<File>>>,
    bytebuf: Vec<u8>,
}

impl Loader {
    pub fn load<P: AsRef<Path> + ?Sized>(path: &P) -> Result<Self, Error> {
        let compressed = BufReader::new(File::open(path)?);
        let mut source = BufReader::new(GzDecoder::new(compressed));

        let mut linebuf = String::new();
        let linebuf = &mut linebuf;

        loop {
            source.read_line(linebuf)?;
            if linebuf.contains("DISABLE KEYS") { break }
        }

        Ok(Self { source, bytebuf: Vec::with_capacity(1024) })
    }
}


impl Iterator for Loader {
    type Item = Result<Vec<String>, Error>;

    fn next(&mut self) -> Option<Self::Item> {

        let Self { source, bytebuf } = self;

        let _size = match source.read_until(b'(', bytebuf) {
            Ok(size) => size,
            Err(e) => return Some(Err(e)),
        };

        bytebuf.clear();

        let size = match source.read_until(b')', bytebuf) {
            Ok(size) => size,
            Err(e) => return Some(Err(e)),
        };

        if size == 0 { return None }

        let line = match String::from_utf8(bytebuf[..(size-1)].to_owned()) {
            Ok(line) => line,
            Err(e) => return Some(Err(Error::new(std::io::ErrorKind::InvalidData, e))),
        };

        Some(Ok(line.split(",")
            .map(str::to_owned)
            .collect()
        ))
    }
}


/// Tokenization errors
#[derive(Debug, Error)]
pub enum TokenizerError {
    #[error("i/o")]
    IO(#[from] std::io::Error),
    #[error("parse")]
    Parse(#[from] std::num::ParseIntError),
    #[error("unexpected end of stream, expected {expected}")]
    Eof { expected: char },
}

/// Output type for the tokenizer
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Token {
    /// keywords, table names (including quoted), or operators
    Symbol(SmolStr),
    /// Quoted strings
    String(String),
    /// raw numbers
    Number(i64),
}

/// A streaming SQL tokenizer. Wraps a byte stream and provides iteration over tokens.
pub struct Tokenizer<'r> {
    source: Peekable<Bytes<&'r mut dyn Read>>,
    buffer: String,
}

impl <'r> Tokenizer<'r> {

    /// Create a tokenizer reading from a given source
    pub fn new(source: &'r mut dyn Read) -> Self {
        Self { source: source.bytes().peekable(), buffer: String::with_capacity(4096) }
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
        where P: Fn(u8) -> bool
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
        Ok(Token::Number(self.collect_while(|c| c.is_ascii_digit())?.parse()?))
    }

    /// Parse an identifier
    fn parse_identifier(&mut self) -> Result<Token, TokenizerError> {
        self.buffer.clear();
        Ok(Token::Symbol(SmolStr::new(self.collect_while(|c| c.is_ascii_alphanumeric())?)))
    }

    /// Parse a quoted string
    fn parse_string(&mut self) -> Result<Token, TokenizerError> {
        self.buffer.clear();

        loop {
            self.source.next();
            self.collect_while(|c| c != b'\'')?;
            self.source.next().ok_or(TokenizerError::Eof { expected: '\'' })??;

            if let Some(Ok(b'\'')) = self.source.peek() {
                self.buffer.push('\'')
            } else {
                return Ok(Token::String(self.buffer.clone()))
            }
        }
        
    }

    /// Parse a quoted identifier
    fn parse_quoted_identifier(&mut self) -> Result<Token, TokenizerError> {
        self.buffer.clear();
        self.source.next();
        self.collect_while(|c| c != b'`')?;
        self.source.next().ok_or(TokenizerError::Eof { expected: '`' })??;
        Ok(Token::Symbol(SmolStr::from(&self.buffer)))
    }

}

impl Iterator for Tokenizer<'_> {
    type Item = Result<Token, TokenizerError>;


    fn next(&mut self) -> Option<Self::Item> {

        if let Err(e) = self.skip_white() {
            return Some(Err(e.into()))
        }

        let &Ok(c) = self.source.peek()? else {
            return Some(Err(self.source.next().unwrap().unwrap_err().into()))
        };

        let tok = match c {
            c if c.is_ascii_digit() => self.parse_number(),
            c if c.is_ascii_alphabetic() => self.parse_identifier(),
            b'`' => self.parse_quoted_identifier(),
            b'\'' => self.parse_string(),
            c => {
                self.source.next();
                Ok(Token::Symbol(SmolStr::new_inline(std::str::from_utf8(&[c]).unwrap())))
            }
        };

        Some(tok)

    }

}

/// Create a tokenizer over the given source
pub fn tokenize(source: &mut dyn Read) -> Tokenizer<'_> {
    Tokenizer::new(source)
}

#[test]
fn sample_tokenization() {
    use Token::*;
    let sym = |s| { Token::Symbol(SmolStr::new_inline(s)) };

    let sample_statement = b"  INSERT   INTO `my table` VALUES (1,'l o l', 0), (2, 'o''escape', 'yourmom'   )     ";

    let tokens: Result<Vec<_>,_> = tokenize(&mut &sample_statement[..]).collect();
    let tokens = tokens.unwrap();

    assert_eq!(&tokens, 
        &[sym("INSERT"),
          sym("INTO"),
          sym("my table"),
          sym("VALUES"),
          sym("("),
          Number(1),
          sym(","),
          Token::String("l o l".into()),
          sym(","),
          Number(0),
          sym(")"),
          sym(","),
          sym("("),
          Number(2),
          sym(","),
          Token::String("o'escape".into()),
          sym(","),
          Token::String("yourmom".into()),
          sym(")"),

        ]
    )

}