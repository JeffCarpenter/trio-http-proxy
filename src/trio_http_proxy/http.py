
http_method_connect = b"CONNECT"
http_200_connection_established = b"HTTP/1.1 200 Connection established\r\n\r\n"

http_400_bad_request = (
    b"HTTP/1.1 400 Bad Request\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
)
http_408_request_timeout = (
    b"HTTP/1.1 408 Request Timeout\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
)
http_414_uri_too_long = (
    b"HTTP/1.1 414 URI Too Long\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
)
http_429_too_many_requests = (
    b"HTTP/1.1 429 Too Many Requests\r\nRetry-After: %d\r\n"
    b"Connection: close\r\nContent-Length: 0\r\n\r\n"
)
http_502_bad_gateway = (
    b"HTTP/1.1 502 Bad Gateway\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
)
http_503_service_unavailable = b"HTTP/1.1 503 Service Unavailable\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
http_504_gateway_timeout = (
    b"HTTP/1.1 504 Gateway Timeout\r\nConnection: close\r\nContent-Length: 0\r\n\r\n"
)


