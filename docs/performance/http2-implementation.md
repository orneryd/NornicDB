# HTTP/2 Support

**NornicDB HTTP/2 configuration and performance guide**

Last Updated: January 27, 2026

---

## Overview

NornicDB's HTTP server supports HTTP/2 for improved performance with concurrent workloads. HTTP/2 is **always enabled** and is fully backwards compatible — existing HTTP/1.1 clients continue to work without any changes.

### Benefits

1. **Multiplexing** — Multiple requests can be sent over a single TCP connection
2. **Header Compression** — Reduces overhead for repeated headers
3. **Binary Protocol** — More efficient than HTTP/1.1's text-based protocol

**Expected Performance Improvement:**
- **10-20% latency reduction** for concurrent requests
- **Reduced connection overhead** for high-concurrency workloads
- **Better resource utilization** with connection multiplexing

---

## Modes

### HTTPS Mode (TLS)
When TLS certificates are configured:
- HTTP/2 is enabled via ALPN (Application-Layer Protocol Negotiation)
- Clients automatically negotiate HTTP/2 during TLS handshake
- Falls back to HTTP/1.1 for clients that don't support HTTP/2

### HTTP Mode (Cleartext)
When running without TLS:
- Uses **h2c** (HTTP/2 Cleartext) protocol
- Automatically detects HTTP/2 vs HTTP/1.1 clients
- Falls back to HTTP/1.1 for older clients

### Backwards Compatibility

HTTP/2 is fully backwards compatible:
- HTTP/1.1 clients continue to work without any changes
- No client-side configuration required
- Automatic protocol negotiation
- No breaking changes to existing APIs

---

## Client Usage

### HTTP/1.1 Clients (No Changes Required)

Existing HTTP/1.1 clients work without modification:

```bash
# curl (HTTP/1.1 by default)
curl http://localhost:7474/health
```

### HTTP/2 Clients (Automatic Upgrade)

Modern HTTP clients automatically use HTTP/2:

```bash
# curl with HTTP/2
curl --http2 http://localhost:7474/health
```

---

## Performance Impact

### Benchmark Results

From profiling analysis:
- **Before HTTP/2:** 26,405 req/s, 0.57ms average latency
- **Expected with HTTP/2:** 10-20% latency reduction for concurrent requests
- **Connection overhead:** Reduced by multiplexing multiple requests per connection

### When HTTP/2 Helps Most

HTTP/2 provides the most benefit for:
1. **High concurrency workloads** — Multiple requests from same client
2. **Many small requests** — Header compression reduces overhead
3. **Latency-sensitive applications** — Reduced connection establishment overhead

### When HTTP/2 Has Minimal Impact

HTTP/2 has less impact for:
1. **Single request scenarios** — No multiplexing benefit
2. **Large payloads** — Header compression is less significant
3. **Low concurrency** — Connection overhead is already minimal

---

## Configuration

### MaxConcurrentStreams

Controls the maximum number of concurrent streams per HTTP/2 connection.

**Default Value: 250**

**Recommendations by Use Case:**

- **Default (most cases):** 250 streams
  - Good balance of performance and security
  - Adequate for typical API workloads

- **Lower memory usage:** 100 streams
  - Industry standard recommendation
  - Better security posture
  - Good for resource-constrained environments

- **High concurrency (50-200 clients):** 500-1000 streams
  - For scenarios with many concurrent clients
  - Each client can have multiple concurrent requests
  - Uses more memory per connection

- **Very high concurrency (200+ clients):** 1000+ streams
  - Only for specialized high-load scenarios
  - **Warning:** Values >1000 increase DoS attack risk
  - Monitor memory usage carefully

**Trade-offs:**
- **Higher values:** More concurrent requests per connection, but more memory usage and DoS risk
- **Lower values:** Less memory, better security, but may require more connections for high concurrency

**Memory Impact:**
Each stream requires memory for stream state tracking, flow control windows, and request/response buffers.

**Security Consideration:**
Malicious clients can open connections and create the maximum number of streams, potentially exhausting server memory. The default of 250 provides a good balance between functionality and security.

---

## Verifying HTTP/2

### Check Server Logs

On startup, look for:
```
🚀 HTTP/2 enabled (h2c cleartext mode, backwards compatible with HTTP/1.1)
```

Or for HTTPS:
```
🚀 HTTP/2 enabled (HTTPS mode)
```

### Test HTTP/2 Connection

```bash
# Test with curl (requires HTTP/2 support)
curl -v --http2 http://localhost:7474/health 2>&1 | grep -i "http/2"

# Expected output:
# < HTTP/2 200
```

---

## Troubleshooting

### HTTP/2 Not Working

1. **Check server logs** — Should show "HTTP/2 enabled" message
2. **Verify client support** — Some older clients don't support HTTP/2
3. **Check network** — Some proxies/firewalls may block HTTP/2

### Performance Not Improved

1. **Low concurrency** — HTTP/2 benefits are most visible with multiple concurrent requests
2. **Single connection** — HTTP/2 multiplexing requires multiple requests on same connection
3. **Large payloads** — Header compression has less impact on large responses

### Compatibility Issues

If you encounter issues with specific clients:

1. **HTTP/2 is backwards compatible** — Clients should fall back to HTTP/1.1
2. **Check client logs** — May show protocol negotiation details
3. **Test with HTTP/1.1 explicitly** — Some clients allow forcing HTTP/1.1

---

## References

- [HTTP/2 Specification (RFC 7540)](https://tools.ietf.org/html/rfc7540)
- [Go HTTP/2 Package Documentation](https://pkg.go.dev/golang.org/x/net/http2)
- [h2c (HTTP/2 Cleartext) Documentation](https://pkg.go.dev/golang.org/x/net/http2/h2c)
