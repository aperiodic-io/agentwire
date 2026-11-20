Feature: Every request is stored as a delta, and read back byte for byte
  A gateway logs each request for audit and replay; a trace pipeline logs each span. Both re-send
  the whole prompt every call, so AWL1 stores each as a delta against an earlier record.

  Scenario: A recording server keeps every request it forwards, in a fraction of its size
    Given an upstream API
    And an agentwire server in front of it, recording to a directory
    And an agentwire client in front of that server
    When two tools send conversations of 6 turns through the client, turn by turn
    Then the upstream received every request byte for byte
    And the recording holds every request byte for byte, with its method and path
    And the recording takes under 25% of the requests' bytes

  Scenario: Spans exported as JSON lines pack and unpack byte for byte
    Given 3 traces of 8 spans, each span carrying the whole prompt, as JSON lines
    When they are packed with `agentwire log pack`
    Then `agentwire log unpack` gives back the same lines
    And `agentwire log get` gives back span 17 alone
    And the packed file takes under 25% of the lines' bytes
