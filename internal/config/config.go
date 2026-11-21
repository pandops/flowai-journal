// Package config loads journal's YAML configuration
// (svc/journal/config.example.yaml lists every key).
package config

import (
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"net/url"
	"os"
	"regexp"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// DefaultSchema is the PostgreSQL schema used when db.schema is absent.
const DefaultSchema = "journal_schema"

// DB is the PostgreSQL connection.
type DB struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
	Name     string `yaml:"name"`
	Schema   string `yaml:"schema"`
	SSLMode  string `yaml:"sslmode"`
	CAFile   string `yaml:"ca_file"`
}

// NATS is the broker connection.
type NATS struct {
	URL             string        `yaml:"url"`
	CAFile          string        `yaml:"ca_file"`
	CredentialsFile string        `yaml:"credentials_file"`
	InboxPrefix     string        `yaml:"inbox_prefix"`
	RequestTimeout  time.Duration `yaml:"request_timeout"`
}

// HTTP is the one listener (probes and the stream edge).
type HTTP struct {
	Listen string `yaml:"listen"`
}

// CursorKey is one HMAC cursor key.
type CursorKey struct {
	ID     int64  `yaml:"id"`
	KeyHex string `yaml:"key_hex"`
}

// Config is journal's whole configuration.
type Config struct {
	HTTP         HTTP        `yaml:"http"`
	DB           DB          `yaml:"db"`
	NATS         NATS        `yaml:"nats"`
	GatewayToken string      `yaml:"gateway_token"`
	CursorKeys   []CursorKey `yaml:"cursor_keys"`
}

var schemaName = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,62}$`)

// Load reads and validates the file at path.
func Load(path string) (Config, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("config: %w", err)
	}
	return Parse(raw)
}

// Parse parses and validates raw YAML, applying defaults. It enforces the
// production trust rules: TLS NATS with CA and user credentials, and
// PostgreSQL with sslmode verify-full and a CA.
func Parse(raw []byte) (Config, error) {
	var c Config
	dec := yaml.NewDecoder(bytes.NewReader(raw))
	dec.KnownFields(true)
	if err := dec.Decode(&c); err != nil {
		return Config{}, fmt.Errorf("config: %w", err)
	}
	if c.HTTP.Listen == "" {
		c.HTTP.Listen = ":8080"
	}
	if c.DB.Port == 0 {
		c.DB.Port = 5432
	}
	if c.DB.Name == "" {
		c.DB.Name = "flowai"
	}
	if c.DB.Schema == "" {
		c.DB.Schema = DefaultSchema
	}
	if c.NATS.InboxPrefix == "" {
		c.NATS.InboxPrefix = "_INBOX.journal"
	}
	if c.NATS.RequestTimeout == 0 {
		c.NATS.RequestTimeout = 5 * time.Second
	}
	var errs []error
	need := func(ok bool, msg string) {
		if !ok {
			errs = append(errs, errors.New(msg))
		}
	}
	need(c.DB.Host != "", "db.host is required")
	need(c.DB.User != "", "db.user is required")
	need(c.DB.Password != "", "db.password is required")
	need(schemaName.MatchString(c.DB.Schema), "db.schema must be a lowercase PostgreSQL identifier")
	need(c.DB.SSLMode == "verify-full", "db.sslmode must be verify-full")
	need(c.DB.CAFile != "", "db.ca_file is required")
	u, err := url.Parse(c.NATS.URL)
	need(err == nil && u.Scheme == "tls" && u.Host != "", "nats.url must be a tls:// URL")
	need(c.NATS.CAFile != "", "nats.ca_file is required")
	need(c.NATS.CredentialsFile != "", "nats.credentials_file is required")
	need(strings.HasPrefix(c.NATS.InboxPrefix, "_INBOX."), "nats.inbox_prefix must start with _INBOX.")
	need(len(c.GatewayToken) >= 16, "gateway_token is required (16 bytes or more)")
	need(len(c.CursorKeys) > 0, "cursor_keys needs at least one key")
	for _, k := range c.CursorKeys {
		b, err := hex.DecodeString(k.KeyHex)
		need(k.ID > 0 && err == nil && len(b) >= 16, "cursor_keys: positive id and key_hex of 16 bytes or more")
	}
	if len(errs) > 0 {
		return Config{}, fmt.Errorf("config: %w", errors.Join(errs...))
	}
	return c, nil
}

// PostgresDSN is the pgx connection string for the configured database;
// search_path pins every unqualified name to the service schema.
func (c Config) PostgresDSN() string {
	q := func(s string) string {
		if s != "" && !strings.ContainsAny(s, " '\\\t\n") {
			return s
		}
		return "'" + strings.NewReplacer(`\`, `\\`, `'`, `\'`).Replace(s) + "'"
	}
	parts := []string{
		"host=" + q(c.DB.Host),
		fmt.Sprintf("port=%d", c.DB.Port),
		"user=" + q(c.DB.User),
		"password=" + q(c.DB.Password),
		"dbname=" + q(c.DB.Name),
		"sslmode=" + q(c.DB.SSLMode),
		"search_path=" + q(c.DB.Schema),
	}
	if c.DB.CAFile != "" {
		parts = append(parts, "sslrootcert="+q(c.DB.CAFile))
	}
	return strings.Join(parts, " ")
}
