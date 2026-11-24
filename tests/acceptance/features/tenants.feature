Feature: Each tenant is recorded in a directory of its own
  A collector shared by tenants keeps each one's records apart, so that a tenant's files can be
  kept, shipped and deleted on their own: `--tenant-header` names the header that says whose a
  request is, and each tenant gets a directory in the record directory.

  Scenario: Each tenant's requests are recorded in its own directory
    Given a deltapack collector recording to a directory, a directory per tenant by "x-tenant"
    When an exporter posts 3 batches as tenant "acme", 2 as tenant "globex" and 1 as no tenant
    Then the directory "acme" holds acme's 3 batches in order
    And the directory "globex" holds globex's 2 batches in order
    And the directory "_none" holds the batch that named no tenant
    And the collector reports 6 records stored

  Scenario: A tenant header cannot reach outside the record directory
    Given a deltapack collector recording to a directory, a directory per tenant by "x-tenant"
    When an exporter posts a batch as each of the tenants "../evil", "a/b", ".hidden", "" and 65 letters
    Then the directory "_invalid" holds those 5 batches in order
    And the record directory holds only the directory "_invalid"

  Scenario: Past the most tenants allowed, the rest share one directory
    Given a deltapack collector recording to a directory, a directory per tenant by "x-tenant", for at most 2 tenants
    When an exporter posts a batch as each of the tenants "a", "b" and "c"
    Then the record directory holds the directories "_other", "a" and "b"
    And the directory "_other" holds tenant c's batch
