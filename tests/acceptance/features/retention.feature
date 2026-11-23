Feature: Old files are deleted or handed to a command
  A collector that runs for months fills its disk. `--keep-bytes` and `--keep-days` delete the
  oldest closed files; `--on-rotate` runs a command on each file once it is closed, such as one
  that moves it to object storage.

  Scenario: The oldest files are deleted to keep the directory under a size
    Given a deltapack collector recording to a directory, starting a new file every 100000 bytes and keeping 250000 bytes
    When an exporter posts 24 gzip-compressed JSON batches of spans to "/v1/traces"
    Then the closed files left take at most 250000 bytes, and the oldest are gone
    And the files left hold the newest batches, in order

  Scenario: Files last written before the age kept are deleted when the collector starts
    Given a record directory holding a file last written 3 days ago and one written now
    And a deltapack collector recording to it, keeping 2 days
    Then the file written 3 days ago is gone and the one written now is kept

  Scenario: Each closed file is handed to a command that moves it away
    Given a deltapack collector recording to a directory, starting a new file every 100000 bytes and moving each closed file elsewhere
    When an exporter posts 24 gzip-compressed JSON batches of spans to "/v1/traces"
    Then only the open file is left, and the files moved away and the open file hold every batch in order
    And the collector reports 0 failed rotation commands

  Scenario: Files from an earlier run are handed to the command when the collector starts
    Given a record directory holding a file last written 3 days ago and one written now
    And a deltapack collector recording to it, moving each closed file elsewhere
    Then both files are moved away

  Scenario: A command that fails is counted
    Given a deltapack collector recording to a directory, starting a new file every 100000 bytes and running a command that fails on each closed file
    When an exporter posts 24 gzip-compressed JSON batches of spans to "/v1/traces"
    Then the collector reports a failed rotation command for each closed file

  Scenario: A size limit smaller than one file deletes every closed file, never the open one
    Given a deltapack collector recording to a directory, starting a new file every 100000 bytes and keeping 1 bytes
    When an exporter posts 24 gzip-compressed JSON batches of spans to "/v1/traces"
    Then only the open file is left
    And the files left hold the newest batches, in order

  Scenario: A file is not deleted while its command runs
    Given a deltapack collector recording to a directory, starting a new file every 100000 bytes, keeping 1 bytes and moving each closed file elsewhere after a second
    When an exporter posts 24 gzip-compressed JSON batches of spans to "/v1/traces"
    Then only the open file is left, and the files moved away and the open file hold every batch in order

  Scenario: A file whose command failed is kept, whatever the limits say
    Given a deltapack collector recording to a directory, starting a new file every 100000 bytes, keeping 1 bytes and running a command that fails on each closed file
    When an exporter posts 24 gzip-compressed JSON batches of spans to "/v1/traces"
    Then the collector reports a failed rotation command for each closed file
    And the recording spans several files, each readable on its own, holding every batch in order

  Scenario: A second collector cannot record to a directory in use
    Given a deltapack collector recording to a directory
    Then a second collector recording to the same directory refuses to start
