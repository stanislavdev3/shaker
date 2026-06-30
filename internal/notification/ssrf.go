package notification

import (
	"context"
	"errors"
	"net"
	"net/url"
)

var ErrUnsafeDestination = errors.New("unsafe webhook destination")

func ValidateURL(ctx context.Context, raw string, allowPrivate bool) (*url.URL, []net.IP, error) {
	u, err := url.Parse(raw)
	allowedScheme := u.Scheme == "https" || (allowPrivate && u.Scheme == "http")
	if err != nil || u.Hostname() == "" || u.User != nil || !allowedScheme {
		return nil, nil, ErrUnsafeDestination
	}
	ips, err := net.DefaultResolver.LookupIP(ctx, "ip", u.Hostname())
	if err != nil {
		return nil, nil, err
	}
	for _, ip := range ips {
		if !allowPrivate && unsafeIP(ip) {
			return nil, nil, ErrUnsafeDestination
		}
	}
	return u, ips, nil
}
func unsafeIP(ip net.IP) bool {
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsMulticast() || ip.IsUnspecified() {
		return true
	}
	// Cloud metadata services and IPv4-mapped equivalents.
	return ip.Equal(net.ParseIP("169.254.169.254")) || ip.Equal(net.ParseIP("100.100.100.200"))
}
