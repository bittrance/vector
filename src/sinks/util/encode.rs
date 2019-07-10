use crate::event::{self, Event};

use bytes::Bytes;
use serde::{Deserialize, Serialize};

#[derive(Deserialize, Serialize, Debug, Eq, PartialEq, Clone)]
#[serde(rename_all = "snake_case")]
pub enum BasicEncoding {
    Text,
    Json,
}

pub fn event_as_string(event: Event, encoding: &Option<BasicEncoding>) -> Result<String, ()> {
    let log = event.into_log();

    if (log.is_structured() && encoding != &Some(BasicEncoding::Text))
        || encoding == &Some(BasicEncoding::Json)
    {
        let bytes =
            serde_json::to_vec(&log.all_fields()).map_err(|e| panic!("Error encoding: {}", e))?;
        String::from_utf8(bytes).map_err(|e| panic!("Unable to convert json to utf8: {}", e))
    } else {
        let string = log
            .get(&event::MESSAGE)
            .map(|v| v.to_string_lossy())
            .unwrap_or_else(|| "".into());
        Ok(string)
    }
}

pub fn event_as_bytes(event: Event, encoding: &Option<BasicEncoding>) -> Result<Bytes, ()> {
    event_as_raw_bytes(event, encoding).map(Bytes::from)
}

pub fn event_as_bytes_with_nl(event: Event, encoding: &Option<BasicEncoding>) -> Result<Bytes, ()> {
    event_as_raw_bytes(event, encoding).map(|mut bytes| {
        bytes.push(b'\n');
        Bytes::from(bytes)
    })
}

fn event_as_raw_bytes(event: Event, encoding: &Option<BasicEncoding>) -> Result<Vec<u8>, ()> {
    let log = event.into_log();

    match (encoding, log.is_structured()) {
        (&Some(BasicEncoding::Json), _) | (_, true) => {
            serde_json::to_vec(&log.all_fields()).map_err(|e| panic!("Error encoding: {}", e))
        }

        (&Some(BasicEncoding::Text), _) | (_, false) => {
            let bytes = log
                .get(&event::MESSAGE)
                .map(|v| v.as_bytes().to_vec())
                .unwrap_or(Vec::new());
            Ok(bytes)
        }
    }
}
