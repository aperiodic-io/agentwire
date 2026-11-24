Feature: Records sealed at rest
  A collector given a key seals every record's body with it: the files hold no prompt in the clear,
  and are read only with the key. With a key per tenant, deleting a tenant's key makes its records
  unreadable wherever they were copied.

  Scenario: A collector with a key stores no body in the clear, and reads back only with the key
    Given a key in a file
    And a deltapack collector recording to a directory, sealing with that key
    When an exporter posts 6 gzip-compressed JSON batches of spans to "/v1/traces"
    Then no batch's bytes are in the recording in the clear
    And `deltapack log ls` lists its 6 records without the key
    And `deltapack log show` without the key fails, saying the file is sealed
    And `deltapack log show --trace` of trace 1 with the key gives its 6 spans in order, each with its prompt

  Scenario: Each tenant's records are sealed with its own key, and a tenant without one is not recorded
    Given a directory of keys holding one for tenant "acme"
    And a deltapack collector recording to a directory, a directory per tenant by "x-tenant", each sealed with its key
    When an exporter posts 3 batches as tenant "acme", 0 as tenant "globex" and 0 as no tenant
    Then acme's batches read back with acme's key only
    And a batch as tenant "globex" is refused with 503, so that its exporter retries
    And nothing was recorded for "globex"
    And the collector reports 1 request it could not record

  Scenario: Lines packed with a key read back with the key only
    Given a key in a file
    And 3 traces of 8 spans, each span carrying the whole prompt, as JSON lines
    When they are packed with the key
    Then `deltapack log unpack` without the key fails, saying the file is sealed
    And `deltapack log unpack` with the key gives back the same lines

  Scenario: A file not sealed, read with a key, is read without it and said so
    Given a key in a file
    And 3 traces of 8 spans, each span carrying the whole prompt, as JSON lines
    When they are packed with `deltapack log pack`
    Then `deltapack log unpack` with the key gives back the same lines, saying the file is not sealed
