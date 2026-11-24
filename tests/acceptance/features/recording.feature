Feature: Every request is stored as a delta, and read back byte for byte
  A gateway logs each request for audit and replay; a trace pipeline logs each span. Both re-send
  the whole prompt every call, so DPK1 stores each as a delta against an earlier record.

  Scenario: A recording server keeps every request it forwards, in a fraction of its size
    Given an upstream API
    And a deltapack server in front of it, recording to a directory
    And a deltapack client in front of that server
    When two tools send conversations of 6 turns through the client, turn by turn
    Then the upstream received every request byte for byte
    And the recording holds every request byte for byte, with its method and path
    And the recording takes under 25% of the requests' bytes

  Scenario: Spans exported as JSON lines pack and unpack byte for byte
    Given 3 traces of 8 spans, each span carrying the whole prompt, as JSON lines
    When they are packed with `deltapack log pack`
    Then `deltapack log unpack` gives back the same lines
    And `deltapack log get` gives back span 17 alone
    And the packed file takes under 25% of the lines' bytes
    And `deltapack log stats` reports its 24 records, the lines' bytes, the file's size and their ratio

  Scenario: `deltapack log stats` names a file it cannot read, and reports the others
    Given 3 traces of 8 spans, each span carrying the whole prompt, as JSON lines
    When they are packed with `deltapack log pack`
    Then `deltapack log stats` over it, a copy whose name has a space and an empty file reports both copies and their total, names the empty file and fails

  Scenario: `deltapack log estimate` says what packing would save, without writing a file
    Given 3 traces of 8 spans, each span carrying the whole prompt, as JSON lines
    When they are packed with `deltapack log pack`
    Then `deltapack log estimate --usd-per-gb 0.023` on the same lines reports their 24 records, their bytes, the packed file's size and the cost of each
