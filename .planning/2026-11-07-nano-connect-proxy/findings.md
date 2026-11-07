# Findings

- mini already depends only on Trio and h11; no caching/rate limiting layer remains.
- Most optional complexity is in ProxyConfig, destination ACL/DNS checks, connection limiting and signal-aware draining.
- Existing parser preserves early tunneled bytes and enforces header limits independently of payload size.
- Existing streaming handles half-close and separate idle/send deadlines.
- Trio 0.34 installed docs confirm serve_tcp accepts host/task_status and open_tcp_stream handles address racing. aclose_forcefully provides cancellation-safe nonblocking cleanup.
- Removing ACLs means private network access is allowed; default loopback binding and clear documentation are required.
