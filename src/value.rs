pub use std::borrow::Borrow;
use std::ops::Deref;
use std::rc::Rc;

#[derive(Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
pub struct Value(Rc<str>);

impl Value {
    pub fn new(string: String) -> Value {
        Value(string.into())
    }

    pub fn as_str(&self) -> &str {
        self.borrow()
    }
}

impl Deref for Value {
    type Target = str;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl From<&str> for Value {
    fn from(v: &str) -> Value {
        Value::new(v.to_string())
    }
}

impl Borrow<str> for Value {
    fn borrow(&self) -> &str {
        self.0.borrow()
    }
}
