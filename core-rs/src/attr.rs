//! Attribute value type. Node and edge attributes are typed key-value maps.

use crate::ternary::T;
use rustc_hash::FxHashMap;

/// A typed attribute value.
#[derive(Clone, Debug, PartialEq)]
pub enum Val {
    Ternary(T),
    Int(i64),
    Float(f64),
    Text(String),
    Bool(bool),
}

impl Val {
    pub fn as_ternary(&self) -> Option<T> {
        if let Val::Ternary(t) = self { Some(*t) } else { None }
    }
    pub fn as_int(&self) -> Option<i64> {
        if let Val::Int(n) = self { Some(*n) } else { None }
    }
}

impl From<T> for Val { fn from(t: T) -> Val { Val::Ternary(t) } }
impl From<i64> for Val { fn from(n: i64) -> Val { Val::Int(n) } }
impl From<f64> for Val { fn from(f: f64) -> Val { Val::Float(f) } }
impl From<bool> for Val { fn from(b: bool) -> Val { Val::Bool(b) } }
impl From<String> for Val { fn from(s: String) -> Val { Val::Text(s) } }
impl From<&str> for Val { fn from(s: &str) -> Val { Val::Text(s.to_owned()) } }

/// An attribute map: String key → Val.
#[derive(Clone, Debug, Default)]
pub struct Attrs(FxHashMap<String, Val>);

impl Attrs {
    pub fn new() -> Self { Self::default() }

    pub fn get(&self, key: &str) -> Option<&Val> { self.0.get(key) }

    pub fn set(&mut self, key: impl Into<String>, val: impl Into<Val>) {
        self.0.insert(key.into(), val.into());
    }

    pub fn ternary(&self, key: &str) -> Option<T> {
        self.get(key)?.as_ternary()
    }

    pub fn iter(&self) -> impl Iterator<Item = (&str, &Val)> {
        self.0.iter().map(|(k, v)| (k.as_str(), v))
    }

    /// Reflect all ternary-valued attributes (flip polarity).
    /// Implements the Reflection primitive on attribute maps.
    pub fn reflect_ternary(&self) -> Self {
        let mut out = self.clone();
        for v in out.0.values_mut() {
            if let Val::Ternary(t) = v {
                *t = t.mv_neg();
            }
        }
        out
    }
}

impl std::fmt::Display for Val {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Val::Ternary(t) => write!(f, "{t}"),
            Val::Int(n)     => write!(f, "{n}"),
            Val::Float(x)   => write!(f, "{x}"),
            Val::Text(s)    => write!(f, "{s:?}"),
            Val::Bool(b)    => write!(f, "{b}"),
        }
    }
}
