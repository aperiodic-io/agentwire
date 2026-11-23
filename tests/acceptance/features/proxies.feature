Feature: Requests cross the network as deltas and reach the API byte for byte
  A tool posts to `deltapack client` on its machine, which sends each request to `deltapack server`
  as a delta against the previous one. The server rebuilds it and forwards it to the API.

  Background:
    Given an upstream API
    And a deltapack server in front of it
    And a deltapack client in front of that server

  Scenario: A conversation reaches the API unchanged, and only its first request is sent in full
    When a tool sends a conversation of 12 turns through the client
    Then the upstream received every request byte for byte
    And every response reached the tool unchanged
    And the requests after the first crossed the network in under 3% of their size
    And the server reports 11 deltas and no refusals

  Scenario: Interleaved conversations each find their own base
    When two tools send conversations of 6 turns through the client, turn by turn
    Then the upstream received every request byte for byte
    And the requests after the first of each conversation crossed the network in under 3% of their size

  Scenario: A restarted server forgets its bases, and the next request is resent in full
    When a tool sends a conversation of 4 turns through the client
    And the deltapack server restarts
    And the tool sends 4 more turns of that conversation
    Then the upstream received every request byte for byte
    And the client resent 1 request in full
    And the client reports sending under 35% of its requests' bytes

  Scenario: Bases are never shared across API keys
    When a tool sends a conversation of 3 turns through the client with the key "k1"
    And the tool sends 1 more turn of that conversation with the key "k2"
    Then the upstream received every request byte for byte
    And the client resent 1 request in full

  Scenario: A client without DPD1 is served as before
    When a plain client posts "hello" straight to the server
    Then the upstream received every request byte for byte
    And the response has no dpd1 headers

  Scenario: A corrupt delta is refused, and the API never sees it
    When a client keeps a base on the server, then posts a corrupt delta against it
    Then the server answers 400 with the dpd1-error "corrupt"
    And the upstream received only the base

  Scenario: A tool is told when the API cannot be reached
    When the upstream API is down
    And a tool posts a request through the client
    Then the tool is answered 502

  Scenario: A body in a content coding the client cannot decode reaches the API as it was sent
    When a tool posts an lz4-encoded body through the client
    Then the upstream received it unchanged, still lz4-encoded
