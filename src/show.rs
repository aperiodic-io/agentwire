//! `logpak log show`: records as JSON lines. An OTLP export (JSON or protobuf) becomes one line per
//! span or log record, attributes as plain JSON; any other record comes out whole. Also the trace
//! ids `collect` lists in a record's meta, so `show --trace` skips the records of other traces.

use serde_json::{Map, Value};

/// The OTLP signal a request path carries: `/v1/traces` and `/v1/logs`, or their gRPC methods.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Signal {
    Traces,
    Logs,
}

pub fn signal(path: &str) -> Option<Signal> {
    let path = path.split('?').next().unwrap_or(path);
    if path.ends_with("/v1/traces") || path == "/opentelemetry.proto.collector.trace.v1.TraceService/Export" {
        Some(Signal::Traces)
    } else if path.ends_with("/v1/logs") || path == "/opentelemetry.proto.collector.logs.v1.LogsService/Export" {
        Some(Signal::Logs)
    } else {
        None
    }
}

/// More distinct trace ids than this in one batch are listed as `?`: read the body to know.
const MAX_IDS: usize = 64;

/// The meta field `collect` adds to an OTLP record: its trace ids comma-separated, in order of
/// first appearance; `-` for none; `?` when the body is not OTLP or holds more than [`MAX_IDS`].
/// It runs on every batch a collector takes, so it reads only the ids: linear in the body, at most
/// [`MAX_IDS`] kept, and only 16-byte ids (32 hex digits), so the meta stays small.
pub fn trace_ids(signal: Signal, body: &[u8]) -> String {
    let mut ids = Vec::new();
    let read = if body.first() == Some(&b'{') {
        json_ids(body, &mut ids)
    } else {
        // the export's resources, their scopes, their spans or log records, and the id in each
        let leaf = if signal == Signal::Traces { 1 } else { 9 };
        proto_ids(body, &[1, 2, 2], leaf, &mut ids)
    };
    match (read, ids.is_empty()) {
        (false, _) => "?".into(),
        (true, true) => "-".into(),
        (true, false) => ids.join(","),
    }
}

/// Adds `id` once; false when it would be one more than [`MAX_IDS`].
fn add(ids: &mut Vec<String>, id: String) -> bool {
    if ids.contains(&id) {
        return true;
    }
    ids.push(id);
    ids.len() <= MAX_IDS
}

/// The 16-byte values of field `leaf` in the messages `path` leads to, fields of each step on the
/// way; false when the body is not that, or holds too many.
fn proto_ids(mut buf: &[u8], path: &[u64], leaf: u64, ids: &mut Vec<String>) -> bool {
    while !buf.is_empty() {
        let Some((n, wire)) = next_field(&mut buf) else { return false };
        let read = match (path.split_first(), wire) {
            (Some((&step, rest)), Wire::Bytes(b)) if n == step => proto_ids(b, rest, leaf, ids),
            (None, Wire::Bytes(b)) if n == leaf => b.len() != 16 || add(ids, logpak::hex(b)),
            (Some((&step, _)), _) if n == step => false,
            (None, _) if n == leaf => false,
            _ => true,
        };
        if !read {
            return false;
        }
    }
    true
}

/// The ids after each `"traceId"` key of an OTLP/JSON body. A key's quotes are bare; inside a
/// string they are escaped, so text that looks like a key is never read as one. A link's trace id
/// is listed too: a batch listed under a trace it does not hold is only read in vain.
fn json_ids(body: &[u8], ids: &mut Vec<String>) -> bool {
    const KEY: &[u8] = b"\"traceId\"";
    let mut at = 0;
    while let Some(found) = body[at..].windows(KEY.len()).position(|w| w == KEY) {
        at += found + KEY.len();
        let rest = body[at..].trim_ascii_start();
        let Some(rest) = rest.strip_prefix(b":") else { continue };
        let Some(rest) = rest.trim_ascii_start().strip_prefix(b"\"") else { continue };
        let id = rest.split(|&b| b == b'"').next().unwrap_or(rest);
        if id.len() == 32 && id.iter().all(u8::is_ascii_hexdigit) && rest.get(32) == Some(&b'"') {
            let id = String::from_utf8_lossy(id).to_ascii_lowercase();
            if !add(ids, id) {
                return false;
            }
        }
    }
    true
}

/// The spans or log records of an OTLP export, one JSON object each; `None` when `body` is not one.
/// Protobuf needs the signal, which only the request path says.
pub fn rows(body: &[u8], signal: Option<Signal>) -> Option<Vec<Value>> {
    // a protobuf export may start "\n{" (field 1, 123 bytes long): what does not parse as JSON is
    // tried as protobuf
    let json = (body.trim_ascii_start().first() == Some(&b'{')).then(|| serde_json::from_slice(body).ok());
    let export = match json.flatten() {
        Some(export) => export,
        None => {
            let m = match signal? {
                Signal::Traces => Msg::TraceExport,
                Signal::Logs => Msg::LogsExport,
            };
            decode(body, m, 0)?
        }
    };
    flatten(&export)
}

/// A record that is not OTLP: its body as JSON when it is JSON, as text when it is UTF-8, else only
/// its length.
pub fn whole(body: &[u8]) -> Value {
    let mut row = Map::new();
    match (serde_json::from_slice::<Value>(body), std::str::from_utf8(body)) {
        (Ok(v), _) => row.insert("body".into(), v),
        (_, Ok(s)) => row.insert("body".into(), s.into()),
        _ => row.insert("body_bytes".into(), body.len().into()),
    };
    Value::Object(row)
}

/// What `show` keeps, from what a record's meta says: `time method path content-type encoding
/// [trace-ids]`, as `collect` and `server --record-dir` write it (`pack` writes none).
#[derive(Debug, Default)]
pub struct Filter {
    pub trace: Option<String>,
    /// Unix ms, inclusive
    pub since: Option<u64>,
    pub until: Option<u64>,
}

impl Filter {
    /// Whether the record may hold rows to show, read from its meta alone.
    pub fn wants(&self, meta: &Meta) -> bool {
        let in_time =
            |t: Option<u64>| t.is_some_and(|t| self.since.is_none_or(|s| t >= s) && self.until.is_none_or(|u| t <= u));
        if (self.since.is_some() || self.until.is_some()) && !in_time(meta.time) {
            return false;
        }
        match (&self.trace, meta.traces) {
            (Some(t), Some(ids)) if ids != "?" => ids.split(',').any(|id| id == t),
            _ => true,
        }
    }

    /// Whether a row of a wanted record is shown.
    pub fn keeps(&self, row: &Value) -> bool {
        self.trace.as_deref().is_none_or(|t| row["trace_id"] == t)
    }
}

/// A record's meta, as far as `show` reads it.
#[derive(Debug, Default, PartialEq, Eq)]
pub struct Meta<'a> {
    pub time: Option<u64>,
    pub path: Option<&'a str>,
    /// the content coding the body is still in: `-` for none
    pub encoding: Option<&'a str>,
    pub traces: Option<&'a str>,
}

impl<'a> Meta<'a> {
    pub fn parse(meta: &'a [u8]) -> Self {
        let Ok(meta) = std::str::from_utf8(meta) else { return Meta::default() };
        let fields: Vec<&str> = meta.split(' ').collect();
        Meta {
            time: fields[0].parse().ok(),
            path: fields.get(2).copied(),
            encoding: fields.get(4).copied(),
            traces: fields.get(5).copied(),
        }
    }

    /// Whether the body is stored decoded, so that it can be read as OTLP.
    pub fn decoded(&self) -> bool {
        self.encoding.is_none_or(|e| e == "-")
    }
}

/// A time as `--since` and `--until` take it: Unix ms, or a duration before `now` (`90s`, `15m`,
/// `2h`, `7d`).
pub fn when(s: &str, now: u64) -> Result<u64, String> {
    let bad = || format!("{s:?} is neither Unix ms nor a duration such as 90s, 15m, 2h or 7d");
    if let Ok(ms) = s.parse() {
        return Ok(ms);
    }
    let (n, unit) = [("s", 1_000), ("m", 60_000), ("h", 3_600_000), ("d", 86_400_000)]
        .into_iter()
        .find_map(|(suffix, ms)| Some((s.strip_suffix(suffix)?, ms)))
        .ok_or_else(bad)?;
    let n: u64 = n.parse().map_err(|_| bad())?;
    Ok(now.saturating_sub(n.saturating_mul(unit)))
}

// ---- OTLP, flattened ------------------------------------------------------------------------------

/// OTLP/JSON's shape (as JSON exporters send it, and as [`decode`] builds it from protobuf) to rows.
fn flatten(export: &Value) -> Option<Vec<Value>> {
    let (resources, scopes, items, signal) = match (export.get("resourceSpans"), export.get("resourceLogs")) {
        (Some(r), _) => (r, "scopeSpans", "spans", "span"),
        (_, Some(r)) => (r, "scopeLogs", "logRecords", "log"),
        _ => return None,
    };
    let mut rows = Vec::new();
    for rs in resources.as_array()? {
        let resource = attributes(&rs["resource"]["attributes"]);
        for ss in list(&rs[scopes]) {
            for item in list(&ss[items]) {
                let mut row = if signal == "span" { span(item) } else { log_record(item) };
                row.insert("signal".into(), signal.into());
                row.insert("resource".into(), resource.clone());
                row.insert("scope".into(), ss["scope"]["name"].clone());
                rows.push(Value::Object(row));
            }
        }
    }
    Some(rows)
}

fn list(v: &Value) -> &[Value] {
    v.as_array().map_or(&[], Vec::as_slice)
}

fn span(s: &Value) -> Map<String, Value> {
    let events = list(&s["events"])
        .iter()
        .map(|e| {
            let mut ev = Map::new();
            ev.insert("name".into(), e["name"].clone());
            ev.insert("time".into(), nanos(&e["timeUnixNano"]));
            ev.insert("attributes".into(), attributes(&e["attributes"]));
            Value::Object(ev)
        })
        .collect();
    let mut row = Map::new();
    row.insert("trace_id".into(), id(&s["traceId"]));
    row.insert("span_id".into(), id(&s["spanId"]));
    row.insert("parent_span_id".into(), id(&s["parentSpanId"]));
    row.insert("name".into(), s["name"].clone());
    row.insert("kind".into(), s["kind"].clone());
    row.insert("start".into(), nanos(&s["startTimeUnixNano"]));
    row.insert("end".into(), nanos(&s["endTimeUnixNano"]));
    row.insert("status".into(), s["status"].clone());
    row.insert("attributes".into(), attributes(&s["attributes"]));
    row.insert("events".into(), Value::Array(events));
    row
}

fn log_record(l: &Value) -> Map<String, Value> {
    let mut row = Map::new();
    row.insert("trace_id".into(), id(&l["traceId"]));
    row.insert("span_id".into(), id(&l["spanId"]));
    row.insert("time".into(), nanos(&l["timeUnixNano"]));
    row.insert("observed".into(), nanos(&l["observedTimeUnixNano"]));
    row.insert("severity".into(), l["severityNumber"].clone());
    row.insert("severity_text".into(), l["severityText"].clone());
    row.insert("event_name".into(), l["eventName"].clone());
    row.insert("body".into(), any(&l["body"]));
    row.insert("attributes".into(), attributes(&l["attributes"]));
    row
}

/// A trace or span id as lowercase hex; `null` when absent, empty or not hex (it would not match a
/// `--trace` and must not reach a meta, whose fields are split on spaces and commas).
fn id(v: &Value) -> Value {
    match v.as_str() {
        Some(s) if !s.is_empty() && s.bytes().all(|b| b.is_ascii_hexdigit()) => s.to_ascii_lowercase().into(),
        _ => Value::Null,
    }
}

/// OTLP/JSON writes 64-bit times as strings; protobuf decodes to numbers. Both come out numbers.
fn nanos(v: &Value) -> Value {
    match v {
        Value::String(s) => s.parse::<u64>().map_or(Value::Null, Value::from),
        Value::Number(_) => v.clone(),
        _ => Value::Null,
    }
}

/// OTLP's `[{key, value: AnyValue}]` as a plain JSON object.
fn attributes(kvs: &Value) -> Value {
    Value::Object(list(kvs).iter().filter_map(|kv| Some((kv["key"].as_str()?.to_owned(), any(&kv["value"])))).collect())
}

/// An AnyValue as plain JSON: a string, bool, number, array or object. `bytesValue` stays as sent
/// (base64 from JSON exporters, hex from protobuf).
fn any(v: &Value) -> Value {
    if let Some(s) = v.get("intValue").and_then(Value::as_str) {
        return s.parse::<i64>().map_or(Value::Null, Value::from);
    }
    if let Some(values) = v.get("arrayValue") {
        return Value::Array(list(&values["values"]).iter().map(any).collect());
    }
    if let Some(values) = v.get("kvlistValue") {
        return attributes(&values["values"]);
    }
    ["stringValue", "boolValue", "intValue", "doubleValue", "bytesValue"]
        .iter()
        .find_map(|k| v.get(*k).cloned())
        .unwrap_or(Value::Null)
}

// ---- OTLP protobuf, to OTLP/JSON's shape ------------------------------------------------------------

/// The messages of opentelemetry-proto's trace and logs exports, as far as `show` reads them.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Msg {
    TraceExport,
    LogsExport,
    ResourceSpans,
    ResourceLogs,
    Resource,
    ScopeSpans,
    ScopeLogs,
    Scope,
    Span,
    Event,
    Link,
    Status,
    LogRecord,
    KeyValue,
    Any,
    Array,
    KvList,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Kind {
    Str,
    /// bytes, as lowercase hex (ids)
    Hex,
    U64,
    I64,
    Bool,
    /// fixed64, as a number (times in ns)
    Fixed64,
    Fixed32,
    Double,
    Message(Msg),
}

/// Field `n` of `m`: its OTLP/JSON name, its kind, and whether it repeats. `None`: a field `show`
/// does not read (the dropped counts, schema URLs), skipped.
fn field(m: Msg, n: u64) -> Option<(&'static str, Kind, bool)> {
    use Kind::*;
    let one = |name, kind| Some((name, kind, false));
    let many = |name, kind| Some((name, kind, true));
    match (m, n) {
        (Msg::TraceExport, 1) => many("resourceSpans", Message(Msg::ResourceSpans)),
        (Msg::LogsExport, 1) => many("resourceLogs", Message(Msg::ResourceLogs)),
        (Msg::ResourceSpans | Msg::ResourceLogs, 1) => one("resource", Message(Msg::Resource)),
        (Msg::ResourceSpans, 2) => many("scopeSpans", Message(Msg::ScopeSpans)),
        (Msg::ResourceLogs, 2) => many("scopeLogs", Message(Msg::ScopeLogs)),
        (Msg::Resource, 1) | (Msg::Scope, 3) | (Msg::Span, 9) | (Msg::Event, 3) | (Msg::Link, 4) => {
            many("attributes", Message(Msg::KeyValue))
        }
        (Msg::LogRecord, 6) => many("attributes", Message(Msg::KeyValue)),
        (Msg::ScopeSpans | Msg::ScopeLogs, 1) => one("scope", Message(Msg::Scope)),
        (Msg::ScopeSpans, 2) => many("spans", Message(Msg::Span)),
        (Msg::ScopeLogs, 2) => many("logRecords", Message(Msg::LogRecord)),
        (Msg::Scope, 1) | (Msg::Span, 5) | (Msg::Event, 2) => one("name", Str),
        (Msg::Scope, 2) => one("version", Str),
        (Msg::Span | Msg::Link, 1) | (Msg::LogRecord, 9) => one("traceId", Hex),
        (Msg::Span | Msg::Link, 2) | (Msg::LogRecord, 10) => one("spanId", Hex),
        (Msg::Span | Msg::Link, 3) => one("traceState", Str),
        (Msg::Span, 4) => one("parentSpanId", Hex),
        (Msg::Span, 6) => one("kind", U64),
        (Msg::Span, 7) => one("startTimeUnixNano", Fixed64),
        (Msg::Span, 8) => one("endTimeUnixNano", Fixed64),
        (Msg::Span, 11) => many("events", Message(Msg::Event)),
        (Msg::Span, 13) => many("links", Message(Msg::Link)),
        (Msg::Span, 15) => one("status", Message(Msg::Status)),
        (Msg::Span, 16) | (Msg::Link, 6) | (Msg::LogRecord, 8) => one("flags", Fixed32),
        (Msg::Event, 1) | (Msg::LogRecord, 1) => one("timeUnixNano", Fixed64),
        (Msg::Status, 2) => one("message", Str),
        (Msg::Status, 3) => one("code", U64),
        (Msg::LogRecord, 2) => one("severityNumber", U64),
        (Msg::LogRecord, 3) => one("severityText", Str),
        (Msg::LogRecord, 5) => one("body", Message(Msg::Any)),
        (Msg::LogRecord, 11) => one("observedTimeUnixNano", Fixed64),
        (Msg::LogRecord, 12) => one("eventName", Str),
        (Msg::KeyValue, 1) => one("key", Str),
        (Msg::KeyValue, 2) => one("value", Message(Msg::Any)),
        (Msg::Any, 1) => one("stringValue", Str),
        (Msg::Any, 2) => one("boolValue", Bool),
        (Msg::Any, 3) => one("intValue", I64),
        (Msg::Any, 4) => one("doubleValue", Double),
        (Msg::Any, 5) => one("arrayValue", Message(Msg::Array)),
        (Msg::Any, 6) => one("kvlistValue", Message(Msg::KvList)),
        (Msg::Any, 7) => one("bytesValue", Hex),
        (Msg::Array, 1) => many("values", Message(Msg::Any)),
        (Msg::KvList, 1) => many("values", Message(Msg::KeyValue)),
        _ => None,
    }
}

/// AnyValue nests (arrays of key-value lists of arrays …): deeper than this is not an export.
const MAX_DEPTH: u32 = 64;

/// A field's payload as the wire carries it.
enum Wire<'a> {
    Varint(u64),
    Bytes(&'a [u8]),
}

/// One message. `None`: not this message (a truncated field, a wire type its field does not have,
/// nesting past [`MAX_DEPTH`]).
fn decode(mut buf: &[u8], m: Msg, depth: u32) -> Option<Value> {
    if depth > MAX_DEPTH {
        return None;
    }
    let mut obj = Map::new();
    while !buf.is_empty() {
        let (n, wire) = next_field(&mut buf)?;
        let Some((name, kind, repeated)) = field(m, n) else { continue };
        let v = value(kind, wire, depth)?;
        if repeated {
            obj.entry(name).or_insert_with(|| Value::Array(Vec::new())).as_array_mut()?.push(v);
        } else {
            obj.insert(name.into(), v);
        }
    }
    Some(Value::Object(obj))
}

/// The next field's number and payload; `None` when it is truncated or of a wire type OTLP does not
/// use (groups).
fn next_field<'a>(buf: &mut &'a [u8]) -> Option<(u64, Wire<'a>)> {
    let key = varint(buf)?;
    let wire = match key & 7 {
        0 => Wire::Varint(varint(buf)?),
        1 => Wire::Bytes(take(buf, 8)?),
        2 => {
            let len = usize::try_from(varint(buf)?).ok()?;
            Wire::Bytes(take(buf, len)?)
        }
        5 => Wire::Bytes(take(buf, 4)?),
        _ => return None,
    };
    Some((key >> 3, wire))
}

fn value(kind: Kind, wire: Wire, depth: u32) -> Option<Value> {
    Some(match (kind, wire) {
        (Kind::Str, Wire::Bytes(b)) => String::from_utf8_lossy(b).into(),
        (Kind::Hex, Wire::Bytes(b)) => logpak::hex(b).into(),
        (Kind::U64, Wire::Varint(v)) => v.into(),
        (Kind::I64, Wire::Varint(v)) => (v as i64).into(),
        (Kind::Bool, Wire::Varint(v)) => (v != 0).into(),
        (Kind::Fixed64, Wire::Bytes(b)) => u64::from_le_bytes(b.try_into().ok()?).into(),
        (Kind::Double, Wire::Bytes(b)) => f64::from_le_bytes(b.try_into().ok()?).into(),
        (Kind::Fixed32, Wire::Bytes(b)) => u32::from_le_bytes(b.try_into().ok()?).into(),
        (Kind::Message(m), Wire::Bytes(b)) => decode(b, m, depth + 1)?,
        _ => return None,
    })
}

fn varint(buf: &mut &[u8]) -> Option<u64> {
    let mut v = 0u64;
    for shift in (0..64).step_by(7) {
        let (&b, rest) = buf.split_first()?;
        *buf = rest;
        v |= u64::from(b & 0x7f) << shift;
        if b & 0x80 == 0 {
            return Some(v);
        }
    }
    None
}

fn take<'a>(buf: &mut &'a [u8], n: usize) -> Option<&'a [u8]> {
    let (head, rest) = buf.split_at_checked(n)?;
    *buf = rest;
    Some(head)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// A protobuf field: its key, then its value (length-prefixed for wire type 2).
    fn pb(field: u64, wire: u64, value: &[u8]) -> Vec<u8> {
        let mut out = enc((field << 3) | wire);
        if wire == 2 {
            out.extend(enc(value.len() as u64));
        }
        out.extend_from_slice(value);
        out
    }

    fn enc(mut v: u64) -> Vec<u8> {
        let mut out = Vec::new();
        while v >= 0x80 {
            out.push(v as u8 | 0x80);
            v >>= 7;
        }
        out.push(v as u8);
        out
    }

    fn kv(field: u64, key: &str, any: Vec<u8>) -> Vec<u8> {
        pb(field, 2, &[pb(1, 2, key.as_bytes()), pb(2, 2, &any)].concat())
    }

    fn s(v: &str) -> Vec<u8> {
        pb(1, 2, v.as_bytes())
    }

    /// A trace export with every field `show` reads, and one it does not (a dropped count).
    fn full_trace_export() -> Vec<u8> {
        let any_all = [
            kv(1, "s", s("x")),
            kv(1, "b", pb(2, 0, &enc(1))),
            kv(1, "i", pb(3, 0, &enc(-5i64 as u64))),
            kv(1, "d", pb(4, 1, &1.5f64.to_le_bytes())),
            kv(1, "a", pb(5, 2, &[pb(1, 2, &s("p")), pb(1, 2, &pb(3, 0, &enc(2)))].concat())),
            kv(1, "m", pb(6, 2, &kv(1, "k", s("v")))),
            kv(1, "y", pb(7, 2, &[0xab, 0x01])),
        ]
        .concat();
        let event = [pb(1, 1, &7u64.to_le_bytes()), pb(2, 2, b"retry"), kv(3, "n", pb(3, 0, &enc(1)))].concat();
        let link = [
            pb(1, 2, &[0x0a; 16]),
            pb(2, 2, &[0x0b; 8]),
            pb(3, 2, b"ls"),
            kv(4, "l", s("1")),
            pb(5, 0, &enc(9)),
            pb(6, 5, &3u32.to_le_bytes()),
        ]
        .concat();
        let span = [
            pb(1, 2, &[0xAB; 16]),
            pb(2, 2, &[0xCD; 8]),
            pb(3, 2, b"st"),
            pb(4, 2, &[0xEF; 8]),
            pb(5, 2, b"chat"),
            pb(6, 0, &enc(3)),
            pb(7, 1, &10u64.to_le_bytes()),
            pb(8, 1, &20u64.to_le_bytes()),
            pb(9, 2, &any_all[..0]), // an empty KeyValue: no key, so no attribute
            [kv(9, "s", s("x"))].concat(),
            pb(10, 0, &enc(4)), // dropped_attributes_count: not read
            pb(11, 2, &event),
            pb(13, 2, &link),
            pb(15, 2, &[pb(2, 2, b"boom"), pb(3, 0, &enc(2))].concat()),
            pb(16, 5, &1u32.to_le_bytes()),
        ]
        .concat();
        let scope = [pb(1, 2, b"agent"), pb(2, 2, b"1.0"), kv(3, "sa", s("1"))].concat();
        let scope_spans = [pb(1, 2, &scope), pb(2, 2, &span), pb(3, 2, b"https://schema")].concat();
        let resource = [kv(1, "service.name", s("swe")), kv(1, "all", pb(5, 2, &[]))].concat();
        let rs = [pb(1, 2, &resource), pb(2, 2, &scope_spans)].concat();
        // the attributes of every AnyValue kind ride on the resource of a second ResourceSpans
        let rs2 = pb(1, 2, &any_all);
        [pb(1, 2, &rs), pb(1, 2, &rs2)].concat()
    }

    #[test]
    fn a_protobuf_trace_export_decodes_to_otlp_json() {
        let decoded = decode(&full_trace_export(), Msg::TraceExport, 0).unwrap();
        let ab = "ab".repeat(16);
        assert_eq!(
            decoded["resourceSpans"][0]["scopeSpans"][0]["spans"][0],
            json!({
                "traceId": ab, "spanId": "cd".repeat(8), "traceState": "st", "parentSpanId": "ef".repeat(8),
                "name": "chat", "kind": 3, "startTimeUnixNano": 10, "endTimeUnixNano": 20,
                "attributes": [{}, {"key": "s", "value": {"stringValue": "x"}}],
                "events": [{"timeUnixNano": 7, "name": "retry", "attributes": [{"key": "n", "value": {"intValue": 1}}]}],
                "links": [{"traceId": "0a".repeat(16), "spanId": "0b".repeat(8), "traceState": "ls",
                           "attributes": [{"key": "l", "value": {"stringValue": "1"}}], "flags": 3}],
                "status": {"message": "boom", "code": 2},
                "flags": 1,
            })
        );
        let scope_spans = &decoded["resourceSpans"][0]["scopeSpans"][0];
        assert_eq!(
            scope_spans["scope"],
            json!({"name": "agent", "version": "1.0", "attributes": [{"key": "sa", "value": {"stringValue": "1"}}]})
        );
        assert_eq!(
            decoded["resourceSpans"][1]["resource"]["attributes"],
            json!([
                {"key": "s", "value": {"stringValue": "x"}},
                {"key": "b", "value": {"boolValue": true}},
                {"key": "i", "value": {"intValue": -5}},
                {"key": "d", "value": {"doubleValue": 1.5}},
                {"key": "a", "value": {"arrayValue": {"values": [{"stringValue": "p"}, {"intValue": 2}]}}},
                {"key": "m", "value": {"kvlistValue": {"values": [{"key": "k", "value": {"stringValue": "v"}}]}}},
                {"key": "y", "value": {"bytesValue": "ab01"}},
            ])
        );
    }

    #[test]
    fn a_trace_export_flattens_to_one_row_per_span() {
        let rows = rows(&full_trace_export(), Some(Signal::Traces)).unwrap();
        assert_eq!(rows.len(), 1, "the second ResourceSpans has no spans");
        let ab = "ab".repeat(16);
        assert_eq!(
            rows[0],
            json!({
                "signal": "span", "trace_id": ab, "span_id": "cd".repeat(8), "parent_span_id": "ef".repeat(8),
                "name": "chat", "kind": 3, "start": 10, "end": 20, "status": {"message": "boom", "code": 2},
                "attributes": {"s": "x"},
                "events": [{"name": "retry", "time": 7, "attributes": {"n": 1}}],
                "resource": {"service.name": "swe", "all": []}, "scope": "agent",
            })
        );
    }

    #[test]
    fn every_any_value_becomes_plain_json() {
        let export = json!({"resourceSpans": [{"resource": {"attributes": [
            {"key": "s", "value": {"stringValue": "x"}},
            {"key": "b", "value": {"boolValue": false}},
            {"key": "i", "value": {"intValue": "-7"}},
            {"key": "n", "value": {"intValue": 8}},
            {"key": "bad", "value": {"intValue": "x"}},
            {"key": "d", "value": {"doubleValue": 0.5}},
            {"key": "y", "value": {"bytesValue": "qw=="}},
            {"key": "a", "value": {"arrayValue": {"values": [{"stringValue": "p"}]}}},
            {"key": "m", "value": {"kvlistValue": {"values": [{"key": "k", "value": {"boolValue": true}}]}}},
            {"key": "empty", "value": {}},
            {"value": {"stringValue": "no key"}},
        ]}, "scopeSpans": [{"spans": [{}]}]}]});
        let rows = flatten(&export).unwrap();
        assert_eq!(
            rows[0]["resource"],
            json!({"s": "x", "b": false, "i": -7, "n": 8, "bad": null, "d": 0.5, "y": "qw==", "a": ["p"], "m": {"k": true}, "empty": null})
        );
    }

    #[test]
    fn a_json_trace_export_flattens_as_protobuf_does() {
        let body = br#" {"resourceSpans":[{"scopeSpans":[{"scope":{"name":"s"},"spans":[
            {"traceId":"ABCD","spanId":"01","parentSpanId":"","startTimeUnixNano":"5","endTimeUnixNano":"x","name":"n"}]}]}]}"#;
        let rows = rows(body, None).unwrap();
        assert_eq!(
            (&rows[0]["trace_id"], &rows[0]["parent_span_id"], &rows[0]["start"], &rows[0]["end"], &rows[0]["scope"]),
            (&json!("abcd"), &Value::Null, &json!(5), &Value::Null, &json!("s"))
        );
    }

    #[test]
    fn a_log_export_flattens_to_one_row_per_log_record() {
        let record = [
            pb(1, 1, &11u64.to_le_bytes()),
            pb(2, 0, &enc(9)),
            pb(3, 2, b"INFO"),
            pb(5, 2, &s("hello")),
            kv(6, "k", s("v")),
            pb(7, 0, &enc(1)), // dropped_attributes_count: not read
            pb(8, 5, &1u32.to_le_bytes()),
            pb(9, 2, &[0x01; 16]),
            pb(10, 2, &[0x02; 8]),
            pb(11, 1, &12u64.to_le_bytes()),
            pb(12, 2, b"gen_ai.choice"),
        ]
        .concat();
        let scope_logs = [pb(1, 2, &pb(1, 2, b"sdk")), pb(2, 2, &record)].concat();
        let body = pb(1, 2, &[pb(1, 2, &kv(1, "service.name", s("a"))), pb(2, 2, &scope_logs)].concat());
        assert_eq!(rows(&body, Some(Signal::Traces)), None, "read as traces, its wire types do not fit");
        assert_eq!(
            rows(&body, Some(Signal::Logs)).unwrap(),
            vec![json!({
                "signal": "log", "trace_id": "01".repeat(16), "span_id": "02".repeat(8), "time": 11, "observed": 12,
                "severity": 9, "severity_text": "INFO", "event_name": "gen_ai.choice", "body": "hello",
                "attributes": {"k": "v"}, "resource": {"service.name": "a"}, "scope": "sdk",
            })]
        );
    }

    #[test]
    fn what_is_not_an_otlp_export_has_no_rows() {
        assert_eq!(rows(b"\x0a\x00", None), None, "protobuf needs a signal");
        assert_eq!(rows(b"{not json", None), None);
        assert_eq!(rows(br#"{"messages":[]}"#, None), None);
        assert_eq!(rows(br#"{"resourceSpans":{}}"#, None), None);
        assert_eq!(rows(br#"{"resourceLogs":[{"scopeLogs":[{"logRecords":[{}]}]}]}"#, None).unwrap().len(), 1);
        assert_eq!(rows(br#"{"resourceSpans":[{"scopeSpans":{}}]}"#, None), Some(vec![]));
    }

    #[test]
    fn malformed_protobuf_is_refused_not_misread() {
        let t = |b: &[u8]| decode(b, Msg::TraceExport, 0);
        assert_eq!(t(b""), Some(json!({})));
        assert_eq!(t(&pb(1, 2, b"")), Some(json!({"resourceSpans": [{}]})));
        assert_eq!(t(&pb(7, 0, &enc(1))), Some(json!({})), "an unknown field is skipped");
        assert_eq!(t(&pb(7, 5, &[0; 4])), Some(json!({})), "an unknown fixed32 is skipped");
        assert_eq!(t(&pb(7, 1, &[0; 8])), Some(json!({})), "an unknown fixed64 is skipped");
        assert_eq!(t(&[0x0b]), None, "wire type 3 (groups) is not OTLP");
        assert_eq!(t(&[0x0a, 0x05, 0x00]), None, "truncated");
        assert_eq!(t(&[0x0a]), None, "a key without a value");
        assert_eq!(t(&[0x0d, 0, 0]), None, "a truncated fixed32");
        assert_eq!(t(&[0x09, 0, 0]), None, "a truncated fixed64");
        assert_eq!(t(&[0x80; 11]), None, "a varint longer than 10 bytes");
        assert_eq!(t(&[0x08, 0x80]), None, "a truncated varint");
        assert_eq!(t(&pb(1, 0, &enc(1))), None, "a varint where a message belongs");
        assert_eq!(decode(&pb(1, 0, &enc(1)), Msg::Any, 0), None, "a varint where a string belongs");
        assert_eq!(decode(&pb(4, 5, &[0; 4]), Msg::Any, 0), None, "a fixed32 where a double belongs");
        assert_eq!(decode(&pb(2, 2, b""), Msg::Any, 0), None, "bytes where a bool belongs");
        assert_eq!(decode(&pb(16, 1, &[0; 8]), Msg::Span, 0), None, "a fixed64 where a fixed32 belongs");
        assert_eq!(decode(&pb(7, 5, &[0; 4]), Msg::Span, 0), None, "a fixed32 where a fixed64 belongs");
        assert_eq!(decode(&pb(1, 2, &[0xff, 0xfe]), Msg::Scope, 0), Some(json!({"name": "\u{fffd}\u{fffd}"})));
    }

    #[test]
    fn nesting_is_bounded() {
        // an AnyValue holding an array holding an AnyValue …: two messages per level
        let nest = |levels: usize| (0..levels).fold(s("x"), |inner, _| pb(5, 2, &pb(1, 2, &inner)));
        assert!(decode(&nest(32), Msg::Any, 0).is_some(), "depth 64");
        assert_eq!(decode(&nest(33), Msg::Any, 0), None, "depth 66");
        assert!(decode(b"", Msg::Any, MAX_DEPTH).is_some());
        assert_eq!(decode(b"", Msg::Any, MAX_DEPTH + 1), None);
    }

    #[test]
    fn varints_take_up_to_ten_bytes() {
        let mut max: &[u8] = &[0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x01, 0x07];
        assert_eq!(varint(&mut max), Some(u64::MAX));
        assert_eq!(max, [0x07], "the rest is left");
        let mut two: &[u8] = &[0xac, 0x02];
        assert_eq!(varint(&mut two), Some(300));
    }

    fn json_export(ids: impl IntoIterator<Item = usize>) -> Vec<u8> {
        let spans: Vec<Value> = ids.into_iter().map(|i| json!({"traceId": format!("{i:032x}")})).collect();
        serde_json::to_vec(&json!({"resourceSpans": [{"scopeSpans": [{"spans": spans}]}]})).unwrap()
    }

    #[test]
    fn trace_ids_are_listed_once_each_in_order() {
        let id = |i: usize| format!("{i:032x}");
        assert_eq!(trace_ids(Signal::Traces, &json_export([2, 1, 2])), format!("{},{}", id(2), id(1)));
        assert_eq!(trace_ids(Signal::Traces, &json_export([])), "-");
        assert_eq!(trace_ids(Signal::Traces, b"\x0b"), "?");
        assert_eq!(trace_ids(Signal::Traces, &json_export(0..MAX_IDS)).split(',').count(), MAX_IDS);
        assert_eq!(trace_ids(Signal::Traces, &json_export(0..=MAX_IDS)), "?");
    }

    /// A span (or log record) whose trace id is `id`, in a protobuf export of `signal`.
    fn proto_export(signal: Signal, ids: &[&[u8]]) -> Vec<u8> {
        let leaf = if signal == Signal::Traces { 1 } else { 9 };
        let items: Vec<u8> = ids.iter().flat_map(|id| pb(2, 2, &[pb(leaf, 2, id), pb(5, 2, b"x")].concat())).collect();
        pb(1, 2, &[pb(1, 2, b""), pb(2, 2, &items)].concat())
    }

    #[test]
    fn trace_ids_are_read_from_protobuf_without_decoding_the_rest() {
        let (a, b) = ([0xaa; 16], [0xbb; 16]);
        assert_eq!(
            trace_ids(Signal::Traces, &proto_export(Signal::Traces, &[&a, &b, &a])),
            format!("{},{}", "aa".repeat(16), "bb".repeat(16))
        );
        assert_eq!(trace_ids(Signal::Logs, &proto_export(Signal::Logs, &[&b])), "bb".repeat(16));
        assert_eq!(trace_ids(Signal::Traces, &proto_export(Signal::Traces, &[])), "-");
        assert_eq!(trace_ids(Signal::Traces, b""), "-");
        // field 9 of a span is its attributes, not a trace id
        assert_eq!(trace_ids(Signal::Traces, &proto_export(Signal::Logs, &[&a])), "-");
        assert_eq!(trace_ids(Signal::Traces, &[0x0a, 0x05]), "?", "truncated");
        assert_eq!(trace_ids(Signal::Traces, &pb(1, 0, &enc(1))), "?", "a varint where a message belongs");
        assert_eq!(trace_ids(Signal::Traces, &pb(1, 2, &pb(2, 0, &enc(1)))), "?");
        assert_eq!(trace_ids(Signal::Traces, &pb(1, 2, &pb(2, 2, &pb(2, 0, &enc(1))))), "?");
        let span_with = |id: Vec<u8>| pb(1, 2, &pb(2, 2, &pb(2, 2, &id)));
        assert_eq!(trace_ids(Signal::Traces, &span_with(pb(1, 0, &enc(1)))), "?", "a varint where the id belongs");
        assert_eq!(trace_ids(Signal::Traces, &span_with(pb(7, 1, &[0; 8]))), "-", "other fields are skipped");
    }

    #[test]
    fn only_16_byte_trace_ids_are_listed() {
        assert_eq!(trace_ids(Signal::Traces, &proto_export(Signal::Traces, &[&[1; 8], &[2; 17], &[]])), "-");
        let json = |id: &str| format!(r#"{{"resourceSpans":[{{"scopeSpans":[{{"spans":[{{"traceId":"{id}"}}]}}]}}]}}"#);
        assert_eq!(trace_ids(Signal::Traces, json(&"a".repeat(31)).as_bytes()), "-");
        assert_eq!(trace_ids(Signal::Traces, json(&"a".repeat(33)).as_bytes()), "-");
        assert_eq!(trace_ids(Signal::Traces, json(&format!("{}g", "a".repeat(31))).as_bytes()), "-");
        assert_eq!(trace_ids(Signal::Traces, json(&"A".repeat(32)).as_bytes()), "a".repeat(32));
        assert_eq!(trace_ids(Signal::Traces, json("a b").as_bytes()), "-");
        assert_eq!(trace_ids(Signal::Traces, json(&"a".repeat(30_000_000)).as_bytes()), "-");
    }

    #[test]
    fn json_trace_ids_are_found_by_key_alone() {
        let id = "c".repeat(32);
        let spaced = format!(r#"{{"resourceLogs":[{{"scopeLogs":[{{"logRecords":[{{ "traceId" :  "{id}" }}]}}]}}]}}"#);
        assert_eq!(trace_ids(Signal::Logs, spaced.as_bytes()), id);
        // inside a string its quotes are escaped: not a key
        let quoted =
            format!(r#"{{"resourceSpans":[{{"scopeSpans":[{{"spans":[{{"name":"\"traceId\":\"{id}\""}}]}}]}}]}}"#);
        assert_eq!(trace_ids(Signal::Traces, quoted.as_bytes()), "-");
        for cut in [r#"{"traceId""#, r#"{"traceId":"#, r#"{"traceId":""#, r#"{"traceId":5}"#, r#"{"traceId""x"}"#] {
            assert_eq!(trace_ids(Signal::Traces, cut.as_bytes()), "-", "{cut}");
        }
        assert_eq!(
            trace_ids(Signal::Traces, format!(r#"{{"traceId":"{id}""#).as_bytes()),
            id,
            "an id is read wherever its key is"
        );
    }

    #[test]
    fn listing_trace_ids_stops_at_the_cap() {
        // 200,000 distinct ids: compared each against every other, this takes minutes
        let ids: Vec<[u8; 16]> = (0..200_000u128).map(u128::to_be_bytes).collect();
        let refs: Vec<&[u8]> = ids.iter().map(|i| &i[..]).collect();
        let started = std::time::Instant::now();
        assert_eq!(trace_ids(Signal::Traces, &proto_export(Signal::Traces, &refs)), "?");
        assert_eq!(trace_ids(Signal::Traces, &json_export(0..200_000)), "?");
        assert!(started.elapsed() < std::time::Duration::from_secs(5), "{:?}", started.elapsed());
    }

    #[test]
    fn a_protobuf_export_whose_first_length_is_a_brace_is_still_protobuf() {
        // ResourceSpans of exactly 123 bytes: the export starts 0a 7b, "\n{"
        let ss = pb(2, 2, &pb(2, 2, &[pb(1, 2, &[0xab; 16]), pb(5, 2, b"chat")].concat()));
        let rs = [ss.clone(), pb(3, 2, &vec![b's'; 121 - ss.len()])].concat();
        let body = pb(1, 2, &rs);
        assert_eq!(&body[..2], b"\n{");
        let rows = rows(&body, Some(Signal::Traces)).unwrap();
        assert_eq!(rows[0]["name"], "chat");
        assert_eq!(trace_ids(Signal::Traces, &body), "ab".repeat(16));
    }

    #[test]
    fn signals_are_told_by_path() {
        assert_eq!(signal("/v1/traces"), Some(Signal::Traces));
        assert_eq!(signal("/api/public/otel/v1/traces?x=1"), Some(Signal::Traces));
        assert_eq!(signal("/opentelemetry.proto.collector.trace.v1.TraceService/Export"), Some(Signal::Traces));
        assert_eq!(signal("/v1/logs"), Some(Signal::Logs));
        assert_eq!(signal("/opentelemetry.proto.collector.logs.v1.LogsService/Export"), Some(Signal::Logs));
        assert_eq!(signal("/v1/metrics"), None);
        assert_eq!(signal("/v1/traces/x"), None);
    }

    #[test]
    fn a_record_that_is_not_otlp_comes_out_whole() {
        assert_eq!(whole(br#"{"a":1}"#), json!({"body": {"a": 1}}));
        assert_eq!(whole(b"plain text"), json!({"body": "plain text"}));
        assert_eq!(whole(b"\xff\x00"), json!({"body_bytes": 2}));
    }

    #[test]
    fn meta_is_read_by_position() {
        let m = b"1700 POST /v1/traces application/json - aa,bb";
        let parsed = Meta::parse(m);
        assert_eq!(
            parsed,
            Meta { time: Some(1700), path: Some("/v1/traces"), encoding: Some("-"), traces: Some("aa,bb") }
        );
        assert!(parsed.decoded());
        assert!(!Meta::parse(b"1700 POST /v1/traces application/json lz4 ?").decoded(), "still lz4");
        assert!(Meta::parse(b"").decoded(), "pack stores lines as given");
        assert_eq!(
            Meta::parse(b"1700 POST /v1/messages - -"),
            Meta { time: Some(1700), path: Some("/v1/messages"), encoding: Some("-"), traces: None }
        );
        assert_eq!(Meta::parse(b""), Meta::default(), "pack writes no meta");
        assert_eq!(Meta::parse(b"\xff"), Meta::default());
    }

    #[test]
    fn records_are_kept_by_time_and_trace() {
        let at = |time| Meta { time: Some(time), ..Meta::default() };
        let all = Filter::default();
        assert!(all.wants(&Meta::default()));
        let window = Filter { since: Some(10), until: Some(20), ..Filter::default() };
        assert!(window.wants(&at(10)) && window.wants(&at(20)), "both ends are inclusive");
        assert!(!window.wants(&at(9)) && !window.wants(&at(21)));
        assert!(!window.wants(&Meta::default()), "a record with no time is outside every window");
        assert!(Filter { since: Some(10), ..Filter::default() }.wants(&at(99)));
        assert!(Filter { until: Some(10), ..Filter::default() }.wants(&at(1)));
        assert!(!Filter { until: Some(10), ..Filter::default() }.wants(&Meta::default()));

        let trace = Filter { trace: Some("bb".into()), ..Filter::default() };
        let listing = |traces| Meta { traces: Some(traces), ..Meta::default() };
        assert!(trace.wants(&listing("aa,bb")));
        assert!(!trace.wants(&listing("aa,cc")));
        assert!(!trace.wants(&listing("-")), "a batch with no traces");
        assert!(trace.wants(&listing("?")), "read the body to know");
        assert!(trace.wants(&Meta::default()), "no index: read the body to know");
        assert!(all.wants(&listing("-")));

        assert!(trace.keeps(&json!({"trace_id": "bb"})));
        assert!(!trace.keeps(&json!({"trace_id": "aa"})));
        assert!(!trace.keeps(&json!({"body": "x"})));
        assert!(all.keeps(&json!({"body": "x"})));
    }

    #[test]
    fn times_are_unix_ms_or_a_duration_ago() {
        let now = 1_000_000_000;
        assert_eq!(when("1700000000000", now), Ok(1_700_000_000_000));
        assert_eq!(when("90s", now), Ok(now - 90_000));
        assert_eq!(when("15m", now), Ok(now - 900_000));
        assert_eq!(when("2h", now), Ok(now - 7_200_000));
        assert_eq!(when("7d", now), Ok(now - 604_800_000));
        assert_eq!(when("99999d", now), Ok(0), "before the epoch is the epoch");
        for bad in ["", "h", "2w", "xh", "-2h", "2.5h", "2é", "é"] {
            assert!(when(bad, now).is_err(), "{bad:?}");
        }
    }
}
