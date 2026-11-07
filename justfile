# Run the required CI/pre-commit checks, stopping on the first failure.
ci: lint-ci sansio typecheck test

# Apply type-checking and lint fixes, stopping on the first failure.
check: typecheck-ty lint

# Check lint without modifying files.
lint-ci:
    ruff check

# The sans-io modules must not mention trio at all. This also catches an import hidden in a function body, which the AST test in tests/test_sansio.py misses.
sansio:
    ! rg -n '\btrio\b' src/trio_http_proxy/protocol.py src/trio_http_proxy/timeouts.py src/trio_http_proxy/errors.py src/trio_http_proxy/http.py

# Fix lint violations where possible.
lint:
    ruff check --fix

# Check source types using the configured project environment.
typecheck:
    uv run --locked pyright

# Apply type-checking fixes with ty.
typecheck-ty:
    ty check --fix

# Run the full suite, including integration and Hypothesis tests.
test:
    uv run --locked pytest
