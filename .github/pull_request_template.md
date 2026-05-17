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
- [ ] `docker compose config`
- [ ] `docker compose -f docker-compose.mysql.yml config`
- [ ] `git diff --check`
- [ ] Other:

## Public Boundary

- [ ] This does not include API keys, bearer tokens, passwords, private URLs, or production data.
- [ ] Public documentation changes live in `README.md` or `docs/`.
- [ ] New capabilities are described as current behavior only if they are implemented and tested.
