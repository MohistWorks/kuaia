## Summary

Describe what changed and why.

## Scope

- [ ] Runtime behavior
- [ ] Connector or transform
- [ ] Public documentation
- [ ] Tests or CI
- [ ] Other

## Validation

Run the checks that match the change:

- [ ] `mvn -q test`
- [ ] `mvn -q package`
- [ ] `make public-mvp-smoke`
- [ ] `git diff --check`
- [ ] Other:

## Public Boundary

- [ ] This does not commit `dev/`, `.kuaia/`, `target/`, `.DS_Store`, or local state.
- [ ] This does not include API keys, bearer tokens, passwords, private URLs, or production data.
- [ ] Public documentation changes live in `README.md` or `docs/`.
- [ ] New capabilities are described as current behavior only if they are implemented and tested.
