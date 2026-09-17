# Task Plan: Configuration Management with pydantic-settings

## Goal
Rigorously refactor `trio_http_proxy` configuration management to use `pydantic-settings`:
1. Design strongly-typed, validated Pydantic settings models cleanly structured into a new `config` package (following the codebase's architecture of single-responsibility classes in dedicated packages).
2. Support full backward compatibility with existing environment variable names (`PORT`, `BUFMAXSZ`, `MAXREQSZ`, `CONNECT_READ_TIMEOUT`, `DEST_CONNECT_TIMEOUT`, `PIPE_IDLE_TIMEOUT`, `PIPE_SEND_TIMEOUT`, `MAXCONNS`, `LOG_LEVEL`, `CACHE_URL`, `CACHE_ENABLED`, `CACHE_NAMESPACE`, `DNS_CACHE_TTL`, `CIRCUIT_BREAKER_ENABLED`, `CIRCUIT_BREAKER_ERRORS_RATE`, `CIRCUIT_BREAKER_PERIOD`, `CIRCUIT_BREAKER_TTL`, `CIRCUIT_BREAKER_MIN_CALLS`, `RATE_LIMIT_ENABLED`, `RATE_LIMIT`, `RATE_LIMIT_PERIOD`, `RATE_LIMIT_TTL`, `CACHE_FAILURE_POLICY`).
3. Support structured settings models:
   - `ProxySettings` (server/networking: port, buffer size, timeouts, concurrency, logging)
   - `CacheSettings` (caching: url, enabled, namespace, failure policy, probe interval, dns ttl)
   - `CircuitBreakerSettings` (breaker: enabled, errors rate, period, ttl, min calls, max destinations)
   - `RateLimitSettings` (rate limiting: enabled, limit, period, ttl)
   - Unified `Settings` model composing them with support for `.env` loading and custom env overrides.
4. Replace manual, ad-hoc `_int_env`, `_float_env`, `_bool_env`, `_failure_policy` parsing functions while preserving all validation constraints and exact error behavior where expected.
5. Provide top-level default configuration instances while preserving existing module exports and global access patterns in `proxy.py`, `proxy_cache.py`, `cache/__init__.py`, and `__init__.py`.
6. Maintain 100% backward compatibility for all existing public APIs, function signatures, default constants, and re-exports.
7. Use `mcpls` tools throughout to verify code intelligence, diagnostics, references, and hover types.
8. Verify 100% test pass (222+ tests), 0 ruff errors, 0 pyright errors, and 0 mcpls diagnostics.

## Tranches

- [x] **Tranche 1: Research & Discovery with mcpls**
  - Search and catalog all configuration constants, environment variables, validation rules, and references.
  - Document all settings, their types, validation bounds, and defaults.
  - Record findings in `findings.md`.

- [x] **Tranche 2: Settings Models Design & Implementation (`src/trio_http_proxy/config/`)**
  - Create `config/` package with single-responsibility modules:
    - `proxy_settings.py` -> `ProxySettings`
    - `cache_settings.py` -> `CacheSettings`
    - `circuit_breaker_settings.py` -> `CircuitBreakerSettings`
    - `rate_limit_settings.py` -> `RateLimitSettings`
    - `settings.py` -> Composite `Settings`
    - `config/__init__.py` -> Clean public re-exports
  - Support env loading from environment and optional `.env` file, case-insensitive, with aliases/precedence for existing env var names.
  - Comprehensive field validations matching existing invariants (bounds checking, duration validation, namespace regex, failure policy).

- [x] **Tranche 3: Integration into `proxy.py`, `proxy_cache.py`, and `circuit_breaker/`**
  - Integrate settings models into `ProxyCache`, `CircuitBreakerManager`, and `proxy.py`.
  - Allow passing explicit settings objects or kwargs to `ProxyCache` / server functions.
  - Preserve all existing module-level constants as defaults derived from the settings instance or re-exported for backward compatibility.

- [x] **Tranche 4: Comprehensive Test Suite for Settings & Configuration**
  - Add dedicated test suite for `pydantic-settings` configuration:
    - Default values
    - Environment variable loading and precedence
    - `.env` file loading
    - Validation failures (invalid port, negative buffer, invalid failure policy, invalid namespace, invalid rate limit)
    - Integration with `ProxyCache` and proxy server
  - Ensure all existing tests continue to pass with 0 regressions.

- [x] **Tranche 5: mcpls Rigorous Verification & Clean-up**
  - Eliminate ad-hoc `_int_env`, `_float_env`, `_bool_env`, `_failure_policy` helpers from modules outside `config`.
  - Strictly type `cache_failure_policy` as `Literal["fail-open", "fail-closed"]`.
  - Verify with `mcpls:get_diagnostics`, `pyright`, `ruff`, and `pytest`.
