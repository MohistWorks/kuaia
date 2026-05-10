# Security Policy

Kuaia is currently an MVP runtime. Security reports are welcome, but the project
does not yet publish versioned production releases.

## Supported Versions

The `main` branch is the only supported development line. Historical commits,
local snapshots, and unreleased forks are not supported security release lines.

## Reporting A Vulnerability

Please report suspected vulnerabilities through GitHub Security Advisories for
this repository.

Do not open a public issue that includes:

- API keys, database passwords, bearer tokens, or private URLs,
- production data or proprietary schemas,
- exploit details that could put users at immediate risk.

Include enough detail to reproduce the issue in a local checkout when possible:

- affected command, connector, or YAML field,
- minimal pipeline config or input data,
- expected behavior and observed behavior,
- local Java and Maven versions.

## Current MVP Boundaries

Kuaia currently runs local batch pipelines and selected external service
examples. It does not yet provide a hosted service, authentication layer, RBAC,
multi-tenant control plane, or production deployment hardening.
