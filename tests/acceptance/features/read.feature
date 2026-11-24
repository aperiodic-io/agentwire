Feature: A backend links a span to its full prompt
  A tracing backend keeps a span's attributes, often truncated; `--read-listen` serves a trace's
  spans, prompts whole, at /v1/traces/{trace id}, so a backend's span can link to them.

  Scenario: A trace's spans are served as JSON lines, each with its prompt
    Given a deltapack collector recording to a directory, serving reads
    When an exporter posts 6 gzip-compressed JSON batches of spans to "/v1/traces"
    Then GET /v1/traces/ of trace 1 answers 200 with its 6 spans in order, each with its prompt
    And GET /v1/traces/ of a trace never sent answers 404
    And GET /v1/traces/not-an-id answers 404

  Scenario: Reads need the token when one is set
    Given a deltapack collector recording to a directory, serving reads to holders of a token
    When an exporter posts 6 gzip-compressed JSON batches of spans to "/v1/traces"
    Then GET /v1/traces/ of trace 1 without the token answers 401
    And GET /v1/traces/ of trace 1 with the token answers 200 with its 6 spans in order, each with its prompt

  Scenario: With a directory per tenant, a read names its tenant
    Given a deltapack collector recording to a directory, a directory per tenant by "x-tenant", serving reads
    When an exporter posts 3 batches as tenant "acme", 2 as tenant "globex" and 1 as no tenant
    Then GET /v1/traces/ of trace 1 without a tenant answers 400
    And GET /v1/traces/ of trace 1 for tenant "acme" answers 200 with the 3 spans of it "acme" sent
    And GET /v1/traces/ of trace 1 for tenant "_none" answers 200 with the 1 spans of it "" sent
