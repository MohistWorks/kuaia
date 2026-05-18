.PHONY: test run-local run-transform run-vector benchmark public-mvp-smoke e2e release-gate clean-state

KUAIA ?= ./bin/kuaia
CASE ?= all

test:
	mvn -q test

run-local:
	$(KUAIA) run -f examples/local-file-to-console.yaml

run-transform:
	$(KUAIA) run -f examples/local-file-transform-to-console.yaml

run-vector:
	$(KUAIA) run -f examples/local-file-to-vector.yaml

benchmark:
	$(KUAIA) benchmark

public-mvp-smoke:
	./scripts/public-mvp-smoke.sh

e2e:
	mvn -q package
	./scripts/connector-e2e-smoke.sh $(CASE)

release-gate:
	./scripts/release-gate.sh

clean-state:
	rm -rf .kuaia kuaia-engine/.kuaia
