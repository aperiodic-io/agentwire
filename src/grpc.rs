//! OTLP over gRPC, as `deltapack collect` serves it: one unary Export call per request, over HTTP/2.
//! The request's one length-prefixed message is what is stored, and what goes on to an OTLP/HTTP
//! backend; the outcome goes back as a gRPC status.

use bytes::Bytes;
use http::{HeaderMap, HeaderValue, Response, StatusCode, header};
use http_body_util::BodyExt;

use crate::ResBody;

/// The gRPC status codes a collector answers with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Code {
    Ok = 0,
    Unknown = 2,
    InvalidArgument = 3,
    PermissionDenied = 7,
    ResourceExhausted = 8,
    Unimplemented = 12,
    Unavailable = 14,
    Unauthenticated = 16,
}

/// A gRPC request: `application/grpc`, alone or with a `+proto` (or other) suffix.
pub fn is_grpc(headers: &HeaderMap) -> bool {
    headers.get(header::CONTENT_TYPE).is_some_and(|v| {
        let v = v.as_bytes();
        v == b"application/grpc" || v.starts_with(b"application/grpc+") || v.starts_with(b"application/grpc;")
    })
}

/// The OTLP/HTTP path an OTLP/gRPC Export method is sent on to.
pub fn otlp_http_path(grpc_path: &str) -> Option<&'static str> {
    match grpc_path {
        "/opentelemetry.proto.collector.trace.v1.TraceService/Export" => Some("/v1/traces"),
        "/opentelemetry.proto.collector.logs.v1.LogsService/Export" => Some("/v1/logs"),
        "/opentelemetry.proto.collector.metrics.v1.MetricsService/Export" => Some("/v1/metrics"),
        _ => None,
    }
}

/// A unary call's body: exactly one length-prefixed message. Returns it, and whether it is
/// compressed (with the request's `grpc-encoding`).
pub fn message(body: &[u8]) -> Result<(&[u8], bool), Code> {
    let Some((head, rest)) = body.split_first_chunk::<5>() else { return Err(Code::InvalidArgument) };
    let len = u32::from_be_bytes([head[1], head[2], head[3], head[4]]) as usize;
    match head[0] {
        0 | 1 if rest.len() == len => Ok((rest, head[0] == 1)),
        _ => Err(Code::InvalidArgument),
    }
}

/// The gRPC code for an OTLP/HTTP backend's answer: retryable (429, 502–504) as UNAVAILABLE, as the
/// OTLP specification has exporters retry both.
pub fn code_for(status: StatusCode) -> Code {
    match status.as_u16() {
        200..=299 => Code::Ok,
        400 => Code::InvalidArgument,
        401 => Code::Unauthenticated,
        403 => Code::PermissionDenied,
        404 => Code::Unimplemented,
        413 => Code::ResourceExhausted,
        429 | 502 | 503 | 504 => Code::Unavailable,
        _ => Code::Unknown,
    }
}

/// A successful answer: `message` (an Export response; empty is a full success) length-prefixed,
/// then `grpc-status: 0` in the trailers.
pub fn ok(message: &[u8]) -> Response<ResBody> {
    let mut framed = Vec::with_capacity(5 + message.len());
    framed.push(0);
    framed.extend_from_slice(&(message.len() as u32).to_be_bytes());
    framed.extend_from_slice(message);
    let mut trailers = HeaderMap::new();
    trailers.insert("grpc-status", HeaderValue::from_static("0"));
    let frames = [Ok(hyper::body::Frame::data(Bytes::from(framed))), Ok(hyper::body::Frame::trailers(trailers))];
    let body = http_body_util::StreamBody::new(futures_util::stream::iter(frames));
    with_grpc_headers(Response::new(body.boxed_unsync()))
}

/// A failed call: a trailers-only answer, its status and message in the headers.
pub fn error(code: Code, message: &'static str) -> Response<ResBody> {
    let mut r = with_grpc_headers(Response::new(ResBody::default()));
    r.headers_mut().insert("grpc-status", HeaderValue::from(code as u32));
    r.headers_mut().insert("grpc-message", HeaderValue::from_static(message));
    r
}

fn with_grpc_headers(mut r: Response<ResBody>) -> Response<ResBody> {
    r.headers_mut().insert(header::CONTENT_TYPE, HeaderValue::from_static("application/grpc"));
    r
}

#[cfg(test)]
mod tests {
    use super::*;

    fn frame(flag: u8, message: &[u8]) -> Vec<u8> {
        [&[flag][..], &(message.len() as u32).to_be_bytes(), message].concat()
    }

    #[test]
    fn a_unary_body_is_exactly_one_length_prefixed_message() {
        assert_eq!(message(&frame(0, b"span")), Ok((&b"span"[..], false)));
        assert_eq!(message(&frame(1, b"gz")), Ok((&b"gz"[..], true)));
        assert_eq!(message(&frame(0, b"")), Ok((&b""[..], false)), "an empty Export request");
        for bad in [
            &b""[..],
            b"\x00\x00\x00\x00",
            &frame(2, b"x"),
            &frame(0, b"x")[..5],
            &[frame(0, b"a"), frame(0, b"b")].concat(),
        ] {
            assert_eq!(message(bad), Err(Code::InvalidArgument), "{bad:?}");
        }
        let mut long = frame(0, b"abc");
        long[4] = 4; // says 4 bytes, holds 3
        assert_eq!(message(&long), Err(Code::InvalidArgument));
    }

    #[test]
    fn grpc_requests_are_known_by_their_content_type() {
        let with = |ct: &str| {
            let mut h = HeaderMap::new();
            h.insert(header::CONTENT_TYPE, ct.parse().unwrap());
            h
        };
        for ct in ["application/grpc", "application/grpc+proto", "application/grpc;charset=utf-8"] {
            assert!(is_grpc(&with(ct)), "{ct}");
        }
        for ct in ["application/grpc-web", "application/x-protobuf", "application/json"] {
            assert!(!is_grpc(&with(ct)), "{ct}");
        }
        assert!(!is_grpc(&HeaderMap::new()));
    }

    #[test]
    fn each_otlp_export_goes_to_its_otlp_http_path() {
        assert_eq!(otlp_http_path("/opentelemetry.proto.collector.trace.v1.TraceService/Export"), Some("/v1/traces"));
        assert_eq!(otlp_http_path("/opentelemetry.proto.collector.logs.v1.LogsService/Export"), Some("/v1/logs"));
        assert_eq!(
            otlp_http_path("/opentelemetry.proto.collector.metrics.v1.MetricsService/Export"),
            Some("/v1/metrics")
        );
        assert_eq!(otlp_http_path("/grpc.health.v1.Health/Check"), None);
        assert_eq!(otlp_http_path("/v1/traces"), None);
    }

    #[test]
    fn a_backends_answer_maps_to_the_grpc_code_an_exporter_acts_on() {
        let code = |s: u16| code_for(StatusCode::from_u16(s).unwrap());
        assert_eq!([code(200), code(202), code(299)], [Code::Ok; 3]);
        assert_eq!(code(400), Code::InvalidArgument);
        assert_eq!(code(401), Code::Unauthenticated);
        assert_eq!(code(403), Code::PermissionDenied);
        assert_eq!(code(404), Code::Unimplemented);
        assert_eq!(code(413), Code::ResourceExhausted);
        assert_eq!([code(429), code(502), code(503), code(504)], [Code::Unavailable; 4], "retryable");
        assert_eq!([code(300), code(418), code(500)], [Code::Unknown; 3]);
    }

    #[tokio::test]
    async fn answers_have_the_shapes_grpc_clients_read() {
        let r = ok(b"resp");
        assert_eq!(r.status(), StatusCode::OK);
        assert_eq!(r.headers()[header::CONTENT_TYPE], "application/grpc");
        let collected = r.into_body().collect().await.unwrap();
        assert_eq!(collected.trailers().unwrap()["grpc-status"], "0");
        assert_eq!(&collected.to_bytes()[..], &frame(0, b"resp")[..]);
        let r = error(Code::Unavailable, "backend unreachable");
        assert_eq!(r.headers()["grpc-status"], "14");
        assert_eq!(r.headers()["grpc-message"], "backend unreachable");
        assert_eq!(r.headers()[header::CONTENT_TYPE], "application/grpc");
        assert!(r.into_body().collect().await.unwrap().to_bytes().is_empty(), "trailers-only");
    }
}
