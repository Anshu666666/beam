package downloader

import (
	"context"
	"net"
	"net/http"
	"time"
)

// DefaultTransport provides a high-performance HTTP transport with IPv4 dial preference
// to avoid dead IPv6 routes on certain networks and ISPs when accessing global CDNs.
var DefaultTransport = &http.Transport{
	Proxy: http.ProxyFromEnvironment,
	DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
		dialer := &net.Dialer{
			Timeout:   15 * time.Second,
			KeepAlive: 30 * time.Second,
		}
		conn, err := dialer.DialContext(ctx, "tcp4", addr)
		if err != nil {
			return dialer.DialContext(ctx, network, addr)
		}
		return conn, nil
	},
	MaxIdleConns:        100,
	MaxIdleConnsPerHost: 20,
	IdleConnTimeout:     90 * time.Second,
	TLSHandshakeTimeout: 15 * time.Second,
}

// HTTPClient is the shared HTTP client used across probe and worker downloads.
var HTTPClient = &http.Client{
	Transport: DefaultTransport,
}