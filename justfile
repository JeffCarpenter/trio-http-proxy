# Run the required CI/pre-commit checks, stopping on the first failure.
ci: lint-ci typecheck test

# Apply type-checking and lint fixes, stopping on the first failure.
check: typecheck-ty lint

# Check lint without modifying files.
lint-ci:
    ruff check

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
