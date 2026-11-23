# Security

Report a vulnerability privately through GitHub's
[private vulnerability reporting](https://github.com/aperiodic-io/deltapack/security/advisories/new)
for this repository. Do not open a public issue. We answer within 5 working days, and agree a
disclosure date with you, at most 90 days after the report.

Supported: the latest 0.x release.

In scope: anything that makes `deltapack` forward a body other than the one the client hashed;
lets one tenant use, detect or evict another tenant's bases beyond what SPEC.md §5 states; or makes
it allocate past its configured limits, panic, or hang on input from the network. The model these
are judged against is [SPEC.md §5](SPEC.md#5-scope-and-security).

Known and documented, not a vulnerability: the compression length side channel (SPEC.md §5), and one
tenant's bases being evicted by another's traffic under memory pressure (that tenant's client then
sends one request in full).
