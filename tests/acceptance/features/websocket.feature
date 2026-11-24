Feature: WebSocket traffic is recorded as it is proxied
  Realtime and voice APIs speak WebSocket: a session's messages go both ways on one connection.
  `deltapack server --record-dir` proxies the upgrade to the upstream and stores every message as a
  record of its own, its direction in the meta.

  Scenario: Every message of a session is forwarded and stored, in order, both ways
    Given an upstream WebSocket API that answers each message
    And a deltapack server in front of the WebSocket API, recording to a directory
    When a tool opens a session through the server and sends 3 messages, waiting for each answer
    Then the tool received the 3 answers
    And the upstream saw the session's path and its API key
    And the recording holds the 3 messages and the 3 answers, in order, each with its direction

  Scenario: The upstream's answer to the handshake is the tool's: its refusal, and its subprotocol
    Given an upstream WebSocket API that answers each message
    And a deltapack server in front of the WebSocket API, recording to a directory
    Then a tool's session without its API key is refused with 401
    And a tool that offers the subprotocols "realtime, v2" gets "realtime", the upstream's choice

  Scenario: A session to an upstream that is down is refused with 502
    Given an upstream WebSocket API that answers each message
    And a deltapack server in front of the WebSocket API, recording to a directory
    When the upstream WebSocket API is down
    Then a tool's session through the server is refused with 502
