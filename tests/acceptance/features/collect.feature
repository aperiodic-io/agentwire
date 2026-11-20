Feature: Logs and traces cross the network as deltas and are stored as deltas
  An OTLP exporter (or a gateway's HTTP log plugin) posts batches of spans, each carrying the whole
  prompt. `agentwire collect` stores every batch in AWL1, and forwards it to the real backend when
  given one (`--forward`). `agentwire client` in front of the exporter sends each batch as a delta
  against the previous one.

  Scenario: A collector without a backend stores gzip batches decoded and acknowledges each
    Given an agentwire collector recording to a directory
    When an exporter posts 24 gzip-compressed JSON batches of spans to "/v1/traces"
    Then every batch is acknowledged with 200 and "{}"
    And the recording holds every batch decoded, with its path and content type
    And the recording takes under 25% of the batches' decoded bytes

  Scenario: A protobuf batch is acknowledged in kind
    Given an agentwire collector recording to a directory
    When an exporter posts a protobuf batch to "/v1/traces"
    Then it is acknowledged with 200, an empty body and the protobuf content type

  Scenario: A collector with a backend forwards every batch exactly as it was sent
    Given an upstream API
    And an agentwire collector in front of it, recording to a directory
    When an exporter posts 6 gzip-compressed JSON batches of spans to "/v1/traces"
    Then the upstream received every batch byte for byte, still gzip-compressed
    And the recording holds every batch decoded, with its path and content type

  Scenario: Batches sent through agentwire client cross the network as deltas
    Given an agentwire collector recording to a directory
    And an agentwire client in front of that collector
    When an exporter posts 24 gzip-compressed JSON batches of spans to "/v1/traces" through the client
    Then the recording holds every batch decoded, with its path and content type
    And the batches after the first crossed the network in under 25% of their decoded size

  Scenario: A gzip body that inflates past the size limit is refused
    Given an agentwire collector recording to a directory
    When an exporter posts a gzip body that inflates past the collector's size limit
    Then it is answered 413 and nothing is recorded

  Scenario: A collector starts a new file at the size limit, and each file reads on its own
    Given an agentwire collector recording to a directory, starting a new file every 100000 bytes
    When an exporter posts 24 gzip-compressed JSON batches of spans to "/v1/traces"
    Then the recording spans several files, each readable on its own, holding every batch in order
    And the collector reports 24 records stored in under 25% of their bytes, and no failures
