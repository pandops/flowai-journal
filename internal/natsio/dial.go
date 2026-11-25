package natsio

import (
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/nats-io/jwt/v2"
	"github.com/nats-io/nats.go"

	"github.com/pandops/flowai-journal/internal/config"
)

// DialOptions builds the connection options for c: TLS with the stand CA,
// the user JWT credentials file and the inbox prefix. It fails when the
// credentials or CA cannot be read; journal never connects anonymously.
func DialOptions(c config.NATS) ([]nats.Option, error) {
	if !strings.HasPrefix(c.URL, "tls://") {
		return nil, errors.New("nats: url must use tls://")
	}
	creds, err := os.ReadFile(c.CredentialsFile)
	if err != nil {
		return nil, fmt.Errorf("nats: read credentials: %w", err)
	}
	token, err := jwt.ParseDecoratedJWT(creds)
	if err != nil {
		return nil, errors.New("nats: credentials file holds no user JWT")
	}
	if _, err := jwt.DecodeUserClaims(token); err != nil {
		return nil, errors.New("nats: credentials file holds no user JWT")
	}
	if kp, err := jwt.ParseDecoratedUserNKey(creds); err != nil || kp == nil {
		return nil, errors.New("nats: credentials file holds no user seed")
	}
	ca, err := os.ReadFile(c.CAFile)
	if err != nil {
		return nil, fmt.Errorf("nats: read CA: %w", err)
	}
	if !x509.NewCertPool().AppendCertsFromPEM(ca) {
		return nil, errors.New("nats: CA file holds no certificate")
	}
	prefix := c.InboxPrefix
	if prefix == "" {
		prefix = InboxPrefix
	}
	return []nats.Option{
		nats.Name("journal"),
		nats.UserCredentials(c.CredentialsFile),
		nats.RootCAs(c.CAFile),
		nats.CustomInboxPrefix(prefix),
		nats.MaxReconnects(-1),
		nats.ReconnectWait(time.Second),
	}, nil
}
