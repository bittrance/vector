use crate::event::Event;

use bytes::Bytes;
use string_cache::DefaultAtom as Atom;
use regex::bytes::{Captures, Regex};
use tracing::field;

#[derive(Debug, Clone)]
pub enum Partition {
    /// A static field that doesn't create dynamic partitions
    Static(Bytes),

    /// Represents the ability to extract a key/value from the event
    /// via the provided interpolated stream name.
    Field(Regex, Bytes, Atom),
}

pub fn interpolate(text: &str) -> Option<Partition> {
    let pattern = Regex::new(r"\{\{(?P<key>\D+)\}\}").unwrap();

    if let Some(cap) = pattern.captures(text.as_bytes()) {
        if let Some(entry) = cap.name("key") {
            return String::from_utf8(Vec::from(entry.as_bytes()))
                .map(|key| Partition::Field(pattern, text.into(), key.into()))
                .ok();
        }
    }

    Some(Partition::Static(text.into()))
}

pub fn partition(event: Event, stream: &Partition) -> Option<Bytes> {
    match stream {
        Partition::Static(source) =>
            Some(source.clone()),

        Partition::Field(pattern, source, key) => {
            let result = event.as_log()
                .get(&key)
                .map(|value| {
                    let cap = pattern.replace(source, |_cap: &Captures| value.as_bytes().clone());
                    Bytes::from(&cap[..])
                });

            if result.is_none() {
                warn!(
                    message = "Event key does not exist on the event and the event will be dropped.",
                    key = field::debug(key)
                );
            }

            result
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn interpolate_static() {
        if let Partition::Static(key) = interpolate("static_text").unwrap() {
            assert_eq!(key, "static_text".to_string());
        } else {
            panic!("Expected Partition::Static");
        }
    }

    #[test]
    fn interpolate_event() {
        let result1 = interpolate("{{some_key}}");
        let result2 = interpolate("prefix{{some_key}}");
        let result3 = interpolate("{{some_key}}suffix");
        let result4 = interpolate("prefix{{some_key}}suffix");

        assert_eq!(result1.map(key), result2.map(key));
        assert_eq!(result2.map(key), result3.map(key));
        assert_eq!(result3.map(key), result4.map(key));

        if let Partition::Field(_, _, key) = result1.unwrap() {
            assert_eq!(key, "some_key".to_string());
        } else {
            panic!("Expected Partition::Field");
        }
    }

    #[test]
    fn interpolate_event_multiple() {
        if let Partition::Field(_, _, key) = interpolate("{{key1}} {{key2}}").unwrap() {
            assert_eq!(key, "some_key".to_string());
        } else {
            panic!("Expected Partition::Field");
        }
    }

    fn key(field: &Partition) -> Atom {
        match field {
            Partition::Field(_, _, key) => key,
            Partition::Static(_) => panic!("Static partitions don't have keys")
        }
    }

}