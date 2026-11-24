Feature: Exports too large for a backend are forwarded in parts
  Some backends refuse a request over a limit (New Relic at 1 MB, Datadog at 15 MiB), and one agent
  span can be megabytes. `--forward-max-body` splits an export into exports that fit before it is
  forwarded, keeping each span's resource and scope; the recording holds the whole export.

  Scenario: An export over the limit reaches the backend in parts that fit, each span once and in order
    Given an upstream API
    And a deltapack collector in front of it, recording to a directory, splitting what it forwards at 150000 bytes
    When an exporter posts 3 gzip-compressed JSON batches of spans to "/v1/traces"
    Then the upstream received each batch in parts of at most 150000 bytes, holding its spans in order
    And the recording holds every batch decoded, with its path and content type

  Scenario: A gRPC export over the limit reaches the backend as protobuf parts that fit
    Given an upstream API
    And a deltapack collector in front of it, recording to a directory, splitting what it forwards at 70000 bytes
    When a gRPC exporter sends 4 protobuf trace exports of 2 traces each
    Then the upstream received 8 protobuf parts at "/v1/traces", each of at most 70000 bytes

  Scenario: An export in a coding deltapack does not decode is forwarded as it came, unsplit
    Given an upstream API
    And a deltapack collector in front of it, recording to a directory, splitting what it forwards at 1000 bytes
    When an exporter posts a batch of spans to "/v1/traces" in a coding deltapack does not decode
    Then the upstream received the batch unchanged, still lz4-encoded

  Scenario: An export of exactly the limit is forwarded as it came
    Given an upstream API
    And a deltapack collector in front of it, recording to a directory, splitting what it forwards at the size of a batch decoded
    When an exporter posts 1 gzip-compressed JSON batches of spans to "/v1/traces"
    Then the upstream received the batch unchanged, still gzip-encoded
