# Findings: Configuration Management with pydantic-settings

## 1. Current Configuration Inventory & Validation Rules

### Proxy Server Settings (`src/trio_http_proxy/proxy.py`)
- `PORT` (int, default: 8080, min: 0, max: 65535)
- `BUFMAXSZ` (int, default: io.DEFAULT_BUFFER_SIZE, min: 1)
- `MAXREQSZ` (int, default: 16384, min: 1)
- `CONNECT_READ_TIMEOUT` (float, default: 10.0, min: > 0.0)
- `DEST_CONNECT_TIMEOUT` (float, default: 10.0, min: > 0.0)
- `PIPE_IDLE_TIMEOUT` (float, default: 300.0, min: > 0.0)
- `PIPE_SEND_TIMEOUT` (float, default: 30.0, min: > 0.0)
- `MAXCONNS` (int, default: 1000, min: 1)
- `LOG_LEVEL` (str, default: "INFO", uppercase standard logging levels)

### Cache Settings (`src/trio_http_proxy/proxy_cache.py`)
- `CACHE_URL` (str, default: "mem://")
- `CACHE_ENABLED` (bool, default: True, accepts 1/0, true/false, yes/no, on/off)
- `CACHE_NAMESPACE` (str, default: "trio_http_proxy", regex: `^[a-zA-Z0-9_-]+$`, stripped whitespace)
- `DNS_CACHE_TTL` (str/TTL, default: "0s", duration string)
- `CACHE_FAILURE_POLICY` (str, default: "fail-open", choices: "fail-open", "fail-closed")
- `PROBE_INTERVAL` (float, default: 30.0)

### Circuit Breaker Settings (`src/trio_http_proxy/proxy_cache.py` & `circuit_breaker/`)
- `CIRCUIT_BREAKER_ENABLED` (bool, default: True)
- `CIRCUIT_BREAKER_ERRORS_RATE` (int, default: 50, min: 1, max: 99)
- `CIRCUIT_BREAKER_PERIOD` (str/TTL, default: "60s", duration string, must be > 0s)
- `CIRCUIT_BREAKER_TTL` (str/TTL, default: "30s", duration string, must be > 0s)
- `CIRCUIT_BREAKER_MIN_CALLS` (int, default: 3, min: 1)
- `CIRCUIT_BREAKER_MAX_DESTINATIONS` (int, default: 10000, min: 1)

### Rate Limit Settings (`src/trio_http_proxy/proxy_cache.py`)
- `RATE_LIMIT_ENABLED` (bool, default: False)
- `RATE_LIMIT` (int, default: 0, min: 0)
- `RATE_LIMIT_PERIOD` (str/TTL, default: "60s", duration string, must be > 0s)
- `RATE_LIMIT_TTL` (str/TTL | None, default: None, duration string, must be > 0s if set)

## 2. Pydantic-Settings Capabilities & Integration Strategy

- Subclass `BaseSettings` with `SettingsConfigDict(env_file=".env", env_file_encoding="utf-8", extra="ignore", case_sensitive=False)`.
- Use field aliases or validation to support both flat environment variables (e.g., `PORT`, `CACHE_URL`, `RATE_LIMIT`) and prefixed/nested configurations.
- Custom validators:
  - Strict duration validation matching `cashews.ttl.ttl_to_seconds`: ensure non-zero for periods and ttls where required.
  - Failure policy validator: "fail-open" or "fail-closed".
  - Namespace regex validator: `^[a-zA-Z0-9_-]+$`.
  - Port, buffer size, max requests, timeouts, rate limits bound checks.
- Structure into dedicated classes:
  - `ProxySettings`
  - `CacheSettings`
  - `CircuitBreakerSettings`
  - `RateLimitSettings`
  - Composite `Settings` uniting all of them.

## 3. Backward Compatibility Requirements
- All module-level constants in `proxy.py` (`PORT`, `BUFMAXSZ`, `DEFAULT_PORT`, `MAXREQSZ`, etc.) and `proxy_cache.py` (`DEFAULT_CACHE_URL`, `DEFAULT_CACHE_ENABLED`, `CACHE_FAILURE_POLICY`, etc.) must remain accessible and importable.
- `ProxyCache.__init__` must continue to accept existing keyword arguments and also accept an optional `settings: Settings | CacheSettings` or configure from defaults.
- Any code importing from `trio_http_proxy.cache` or `trio_http_proxy.proxy` must continue to work without change.

## 4. Duplication Analysis & Deduplication Refactor (Tranche 6)
- **`dupe-cli` Scan Findings**:
  - 407 pairs within `settings.py`: 29 explicit 1-line property getters (`return self.group.field`). These are intentional false positives to retain static typing, LSP autocomplete, and pyright type safety.
  - 3 validator pairs: `_validate_circuit_breaker_period`, `_validate_circuit_breaker_ttl`, and `_validate_rate_limit_period` shared identical AST blocks for stripping, empty check, and error raising.
  - 6 duration property pairs: `*_seconds` properties returning `PrivateAttr` fields populated by `model_post_init` with `object.__setattr__`.
- **Refactoring Applied**:
  - Extracted canonical `validate_duration_string(value, *, field, allow_zero=False)` in `_base.py`.
  - Replaced manual stripping/checking across `CircuitBreakerSettings`, `RateLimitSettings`, and `CacheSettings` with `validate_duration_string`. In `CircuitBreakerSettings`, merged two separate validator methods into a single `@field_validator("circuit_breaker_period", "circuit_breaker_ttl")`.
  - Replaced `PrivateAttr` + `model_post_init` mutable assignments on frozen settings with `@cached_property` duration getters, evaluating `parse_duration_seconds()` lazily and caching in `__dict__`.
  - Verified with `uv run pytest` (260 passed), `uv run ruff check` (0 errors), and `uv run pyright src` (0 errors).

