package mysql

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
)

// ErrNoServerCA is a configuration with no CA to verify the server with.
var ErrNoServerCA = errors.New("mysql: no server CA (mysql.ca_cert / WATCHER_MYSQL_CA_CERT) to verify the server with")

// serverTLS trusts only caPEM and requires the server certificate to name
// the host the watcher dials. There is no plaintext or unverified fallback:
// go-mysql refuses a server that does not offer TLS once this is set.
func serverTLS(caPEM, addr string) (*tls.Config, error) {
	roots := x509.NewCertPool()
	if caPEM == "" || !roots.AppendCertsFromPEM([]byte(caPEM)) {
		return nil, ErrNoServerCA
	}
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		host = addr
	}
	if host == "" {
		return nil, fmt.Errorf("mysql: no host name in %q to verify the server certificate against", addr)
	}
	return &tls.Config{RootCAs: roots, ServerName: host, MinVersion: tls.VersionTLS12}, nil
}
