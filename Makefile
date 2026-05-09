.PHONY: test run-local run-transform run-vector clean-state

KUAIA ?= ./bin/kuaia

test:
	mvn -q test

run-local:
	$(KUAIA) run -f examples/local-file-to-console.yaml

run-transform:
	$(KUAIA) run -f examples/local-file-transform-to-console.yaml

run-vector:
	$(KUAIA) run -f examples/local-file-to-vector.yaml

clean-state:
	rm -rf .kuaia kuaia-engine/.kuaia
