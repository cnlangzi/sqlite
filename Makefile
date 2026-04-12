.PHONY: test lint hooks-install

test:
	go test -race -v ./...

lint:
	golangci-lint run --config .golangci.yml

hooks-install:
	@echo "Installing pre-commit hook..."
	@mkdir -p .git/hooks
	@printf '#!/bin/sh\n# Pre-commit hook: runs test and lint before allowing commit\n\nset -e\n\necho "Running tests..."\nmake test\n\necho "Running linter..."\nmake lint\n\necho "All checks passed."\n' > .git/hooks/pre-commit
	@chmod +x .git/hooks/pre-commit
	@echo "Pre-commit hook installed. It will run 'make test' and 'make lint' before each commit."
