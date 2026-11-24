//! `collect --forward-max-body`: an OTLP export too large for a backend, split into exports that
//! fit, at the coarsest level that does: resources, then scopes, then spans or log records. Each
//! part keeps the resource and scope its spans came with. A single span or log record larger than
//! the limit is sent alone: it cannot be split.

use serde_json::Value;

use crate::show::{self, Signal, Wire};

/// The parts of `body`, an OTLP export of `signal`, each at most `max` bytes where a span allows;
/// `None` when it is not one (it is forwarded as it came).
pub fn split(body: &[u8], signal: Signal, max: usize) -> Option<Vec<Vec<u8>>> {
    // as `show::rows` tells them: OTLP/JSON may start with whitespace
    if body.trim_ascii_start().first() == Some(&b'{') {
        let export: Value = serde_json::from_slice(body).ok()?;
        let path = match signal {
            Signal::Traces => ["resourceSpans", "scopeSpans", "spans"],
            Signal::Logs => ["resourceLogs", "scopeLogs", "logRecords"],
        };
        export.get(path[0])?.as_array()?;
        let parts = split_json(&export, &path, max);
        Some(parts.iter().map(|p| serde_json::to_vec(p).expect("a JSON value serializes")).collect())
    } else {
        // export.resource_*s = 1, resource.scope_*s = 2, scope.spans or log_records = 2
        split_proto(body, &[1, 2, 2], max)
    }
}

/// A field: its number, its whole encoding, and its payload when it is length-delimited.
type Field<'a> = (u64, &'a [u8], Option<&'a [u8]>);

/// A message's fields.
fn fields(msg: &[u8]) -> Option<Vec<Field<'_>>> {
    let mut rest = msg;
    let mut out = Vec::new();
    while !rest.is_empty() {
        let before = rest;
        let (n, wire) = show::next_field(&mut rest)?;
        let raw = &before[..before.len() - rest.len()];
        let payload = match wire {
            Wire::Bytes(b) if raw[0] & 7 == 2 => Some(b),
            _ => None,
        };
        out.push((n, raw, payload));
    }
    Some(out)
}

fn varint(out: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        out.push(v as u8 | 0x80);
        v >>= 7;
    }
    out.push(v as u8);
}

/// Field `n`, length-delimited, holding `payload`.
fn field(n: u64, payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(payload.len() + 12);
    varint(&mut out, n << 3 | 2);
    varint(&mut out, payload.len() as u64);
    out.extend_from_slice(payload);
    out
}

/// `msg` as messages that each hold some of its `path[0]` children and all its other fields.
fn split_proto(msg: &[u8], path: &[u64], max: usize) -> Option<Vec<Vec<u8>>> {
    let Some((&child, deeper)) = path.split_first() else { return Some(vec![msg.to_vec()]) };
    let fields = fields(msg)?;
    let context: Vec<u8> = fields.iter().filter(|f| f.0 != child).flat_map(|f| f.1.iter().copied()).collect();
    let mut parts = Vec::new();
    let mut part = context.clone();
    let mut children = 0;
    for (_, raw, payload) in fields.iter().filter(|f| f.0 == child) {
        let payload = (*payload)?; // a resource or scope is a message, or this is no export
        if context.len() + raw.len() > max && !deeper.is_empty() {
            // too large alone: its own children, each part with this message's context
            if children > 0 {
                parts.push(std::mem::replace(&mut part, context.clone()));
                children = 0;
            }
            let room = max.saturating_sub(context.len() + 12);
            for sub in split_proto(payload, deeper, room)? {
                parts.push([&context[..], &field(child, &sub)].concat());
            }
            continue;
        }
        if children > 0 && part.len() + raw.len() > max {
            parts.push(std::mem::replace(&mut part, context.clone()));
            children = 0;
        }
        part.extend_from_slice(raw);
        children += 1;
    }
    if children > 0 || parts.is_empty() {
        parts.push(part);
    }
    Some(parts)
}

/// The same over OTLP/JSON's shape: `path[0]` is the array of children.
fn split_json(v: &Value, path: &[&str], max: usize) -> Vec<Value> {
    let len = |v: &Value| serde_json::to_vec(v).map_or(0, |b| b.len());
    let (Some((key, deeper)), Some(items)) = (path.split_first(), path.first().and_then(|k| v.get(k)?.as_array()))
    else {
        return vec![v.clone()];
    };
    let mut context = v.clone();
    context[*key] = Value::Array(Vec::new());
    let with = |items: Vec<Value>| {
        let mut part = context.clone();
        part[*key] = Value::Array(items);
        part
    };
    let room = len(&context);
    let (mut parts, mut part, mut size) = (Vec::new(), Vec::new(), room);
    for item in items {
        let n = len(item) + 1; // and its comma
        if room + n > max && !deeper.is_empty() {
            if !part.is_empty() {
                parts.push(with(std::mem::take(&mut part)));
                size = room;
            }
            for sub in split_json(item, deeper, max.saturating_sub(room)) {
                parts.push(with(vec![sub]));
            }
            continue;
        }
        if !part.is_empty() && size + n > max {
            parts.push(with(std::mem::take(&mut part)));
            size = room;
        }
        part.push(item.clone());
        size += n;
    }
    if !part.is_empty() || parts.is_empty() {
        parts.push(with(part));
    }
    parts
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn pb(field: u64, wire: u64, value: &[u8]) -> Vec<u8> {
        let mut out = Vec::new();
        varint(&mut out, field << 3 | wire);
        if wire == 2 {
            varint(&mut out, value.len() as u64);
        }
        out.extend_from_slice(value);
        out
    }

    /// A trace export: `resources` resources of `scopes` scopes of `spans` spans, each span holding
    /// `pad` bytes of prompt; resource i is named "r{i}", scope j "s{j}".
    fn export(resources: usize, scopes: usize, spans: usize, pad: usize) -> Vec<u8> {
        let mut id = 0u128;
        let rs: Vec<u8> = (0..resources)
            .flat_map(|r| {
                let resource = pb(
                    1,
                    2,
                    &pb(1, 2, &[pb(1, 2, b"service.name"), pb(2, 2, &pb(1, 2, format!("r{r}").as_bytes()))].concat()),
                );
                let ss: Vec<u8> = (0..scopes)
                    .flat_map(|s| {
                        let spans: Vec<u8> = (0..spans)
                            .flat_map(|_| {
                                id += 1;
                                let attr = pb(9, 2, &[pb(1, 2, b"p"), pb(2, 2, &pb(1, 2, &vec![b'x'; pad]))].concat());
                                pb(2, 2, &[pb(1, 2, &id.to_be_bytes()), pb(5, 2, b"chat"), attr].concat())
                            })
                            .collect();
                        pb(2, 2, &[pb(1, 2, &pb(1, 2, format!("s{s}").as_bytes())), spans].concat())
                    })
                    .collect();
                pb(1, 2, &[resource, ss].concat())
            })
            .collect();
        rs
    }

    /// Each span as (trace id, resource, scope): what must arrive, part by part.
    fn spans(parts: &[Vec<u8>]) -> Vec<(String, String, String)> {
        parts
            .iter()
            .flat_map(|p| show::rows(p, Some(Signal::Traces)).unwrap())
            .map(|r| {
                let s = |v: &Value| v.as_str().unwrap_or_default().to_owned();
                (s(&r["trace_id"]), s(&r["resource"]["service.name"]), s(&r["scope"]))
            })
            .collect()
    }

    #[test]
    fn an_export_under_the_limit_is_one_part() {
        let body = export(2, 2, 2, 10);
        assert_eq!(split(&body, Signal::Traces, body.len()).unwrap(), vec![body.clone()]);
    }

    #[test]
    fn resources_are_split_first_and_each_part_keeps_its_context() {
        for (resources, scopes, spans_each, pad, max) in
            [(4, 1, 1, 100, 300), (1, 4, 1, 100, 300), (1, 1, 6, 100, 300), (3, 3, 3, 50, 400), (2, 2, 2, 10, 60)]
        {
            let body = export(resources, scopes, spans_each, pad);
            let parts = split(&body, Signal::Traces, max).unwrap();
            assert!(parts.len() > 1, "split: {resources} {scopes} {spans_each} {pad} {max}");
            assert_eq!(
                spans(&parts),
                spans(std::slice::from_ref(&body)),
                "every span, in order, with its resource and scope"
            );
            for p in &parts {
                // a part over the limit holds one span the limit cannot fit
                assert!(p.len() <= max || spans(std::slice::from_ref(p)).len() == 1, "{} > {max}", p.len());
            }
        }
    }

    #[test]
    fn whole_resources_are_kept_together_when_they_fit() {
        let body = export(4, 2, 2, 50);
        let one = export(1, 2, 2, 50).len();
        let parts = split(&body, Signal::Traces, one * 2 + 1).unwrap();
        assert_eq!(parts.len(), 2, "two resources a part");
    }

    #[test]
    fn a_span_larger_than_the_limit_is_sent_alone() {
        let body = export(1, 1, 3, 1000);
        let parts = split(&body, Signal::Traces, 500).unwrap();
        assert_eq!(parts.len(), 3);
        assert_eq!(spans(&parts), spans(&[body]));
    }

    #[test]
    fn an_export_without_resources_is_one_part() {
        assert_eq!(split(b"", Signal::Traces, 10).unwrap(), vec![Vec::<u8>::new()]);
        assert_eq!(split(&pb(5, 0, &[1]), Signal::Traces, 1).unwrap(), vec![pb(5, 0, &[1])]);
    }

    #[test]
    fn what_is_not_an_export_is_not_split() {
        assert_eq!(split(b"\x0b", Signal::Traces, 1), None, "not protobuf");
        assert_eq!(split(&pb(1, 0, &[1]), Signal::Traces, 1), None, "a varint where a resource belongs");
        assert_eq!(split(b"{not json", Signal::Traces, 1), None);
        assert_eq!(split(br#"{"messages":[]}"#, Signal::Traces, 1), None);
        assert_eq!(split(br#"{"resourceSpans":{}}"#, Signal::Traces, 1), None);
    }

    fn json_export(resources: usize, spans: usize, pad: usize) -> Value {
        let rs: Vec<Value> = (0..resources)
            .map(|r| {
                let spans: Vec<Value> = (0..spans)
                    .map(|i| json!({"traceId": format!("{:032x}", r * 100 + i), "name": "x".repeat(pad)}))
                    .collect();
                json!({"resource": {"attributes": [{"key": "service.name", "value": {"stringValue": format!("r{r}")}}]},
                       "scopeSpans": [{"scope": {"name": "s"}, "spans": spans}]})
            })
            .collect();
        json!({"resourceSpans": rs})
    }

    #[test]
    fn a_json_export_splits_the_same_way() {
        for (resources, spans_each, pad, max) in [(4, 1, 100, 400), (1, 6, 100, 400), (3, 3, 50, 500)] {
            let export = json_export(resources, spans_each, pad);
            let body = serde_json::to_vec(&export).unwrap();
            let parts = split(&body, Signal::Traces, max).unwrap();
            assert!(parts.len() > 1);
            assert_eq!(spans(&parts), spans(std::slice::from_ref(&body)));
            for p in &parts {
                assert!(p.len() <= max, "{} > {max}", p.len());
            }
        }
        let padded = [&b" \n"[..], &serde_json::to_vec(&json_export(3, 3, 50)).unwrap()].concat();
        assert!(split(&padded, Signal::Traces, 500).unwrap().len() > 1, "JSON after whitespace");
        let small = serde_json::to_vec(&json_export(1, 1, 1)).unwrap();
        assert_eq!(split(&small, Signal::Traces, small.len()).unwrap(), vec![small]);
    }

    #[test]
    fn json_logs_split_by_their_own_keys() {
        let records: Vec<Value> =
            (0..4).map(|i| json!({"body": {"stringValue": "y".repeat(100)}, "severityNumber": i})).collect();
        let export = json!({"resourceLogs": [{"scopeLogs": [{"logRecords": records}]}]});
        let body = serde_json::to_vec(&export).unwrap();
        let parts = split(&body, Signal::Logs, 300).unwrap();
        assert_eq!(parts.len(), 4);
        let n: usize = parts.iter().map(|p| show::rows(p, None).unwrap().len()).sum();
        assert_eq!(n, 4);
        assert_eq!(split(&body, Signal::Traces, 300), None, "traces' key: not this export");
    }
}
