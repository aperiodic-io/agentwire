Feature: A trace is found and read back as JSON lines
  A record is a batch of spans from many traces, stored as a delta. `collect` lists each batch's
  trace ids in its meta, so `logpak log show --trace` finds a trace's batches without parsing the
  others' spans, and writes its spans one JSON line each: OTLP JSON and protobuf alike, attributes as
  plain JSON, ready for `jq`, DuckDB or pandas.

  Scenario: The spans of one trace come back as JSON lines, each with its prompt
    Given a logpak collector recording to a directory
    When an exporter posts 6 gzip-compressed JSON batches of spans to "/v1/traces"
    Then each record's meta lists the trace ids of its batch
    And `logpak log show --trace` of trace 1 gives its 6 spans in order, each with its prompt
    And `logpak log show --trace` in capitals finds the same spans

  Scenario: Protobuf exports over gRPC are decoded too
    Given a logpak collector recording to a directory
    When a gRPC exporter sends 4 protobuf trace exports of 2 traces each
    Then `logpak log show` gives 8 spans, each with its trace id, name, times and attributes
    And `logpak log show --trace` of trace 1 gives its 4 spans in order, each with its prompt

  Scenario: Records that are not OTLP come back whole
    Given 3 traces of 8 spans, each span carrying the whole prompt, as JSON lines
    When they are packed with `logpak log pack`
    Then `logpak log show` gives each line back as the body of its record

  Scenario: Only records received in the time asked for are shown
    Given a logpak collector recording to a directory
    When an exporter posts 6 gzip-compressed JSON batches of spans to "/v1/traces"
    Then `logpak log show --since 1h` gives 18 spans
    And `logpak log show --until 1h` gives none

  Scenario: A batch still in a coding logpak cannot decode is listed as unknown and shown whole
    Given a logpak collector recording to a directory
    When an exporter posts a batch of spans to "/v1/traces" in a coding logpak does not decode
    Then its record's meta lists its trace ids as unknown
    And `logpak log show` gives the record whole
