# Run the required CI/pre-commit checks, stopping on the first failure.
ci: lint-ci sansio no-monkeypatch typecheck test

# Apply type-checking and lint fixes, stopping on the first failure.
check: typecheck-ty lint

# Check formatting and lint without modifying files.
lint-ci:
    uv run --locked ruff format --check
    uv run --locked ruff check

# The sans-io modules must not mention trio at all. This also catches an import hidden in a function body, which the AST test in tests/test_sansio.py misses.
sansio:
    ! rg -n '\btrio\b' src/trio_http_proxy/protocol.py src/trio_http_proxy/timeouts.py src/trio_http_proxy/errors.py src/trio_http_proxy/http.py

# Check test ASTs without executing snippets or banning comments/strings.
no-monkeypatch:
    uv run --locked python scripts/check_no_monkeypatch.py

# Verify isolated mutants separately from the single-run CI suite.
mutation:
    uv run --locked python scripts/verify_mutations.py

# Fix lint violations where possible.
lint:
    uv run --locked ruff check --fix

# Check source types using the configured project environment.
typecheck:
    uv run --locked pyright

# Apply type-checking fixes with ty.
typecheck-ty:
    ty check --fix

# Run the suite once, requiring all runtime source lines and branches.
test:
    uv run --locked pytest --cov --cov-report=term-missing
