# Hyperscrape Coordinator

## Proxy IP handling

`x-forwarded-for` is only used when the direct peer IP is listed in `server.trusted_proxies`.
Otherwise, the websocket peer IP is used.
