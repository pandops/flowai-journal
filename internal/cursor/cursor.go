// Package cursor issues and verifies opaque HMAC-SHA256 pagination
// cursors bound to the operation, scope, filter, ordering and direction.
package cursor

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"sort"
	"strconv"
	"strings"
)

// ErrInvalid is every cursor rejection; callers answer
// 400 invalid_pagination.
var ErrInvalid = errors.New("invalid cursor")

// Key is one configured HMAC key.
type Key struct {
	ID     int64
	Secret []byte
}

// Binding is what a cursor is bound to.
type Binding struct {
	Operation  string
	TeamID     int64
	OperatorID int64
	Filter     map[string]int64
	Order      string
}

// Keyring signs with the highest key id and verifies with any key.
type Keyring struct {
	keys    map[int64][]byte
	current int64
}

// NewKeyring returns a keyring; it needs at least one key of 16 bytes or
// more with a positive, unique id.
func NewKeyring(keys []Key) (*Keyring, error) {
	if len(keys) == 0 {
		return nil, errors.New("cursor: no key")
	}
	k := &Keyring{keys: map[int64][]byte{}}
	for _, key := range keys {
		if key.ID < 1 || len(key.Secret) < 16 {
			return nil, errors.New("cursor: key id must be positive and the key at least 16 bytes")
		}
		if _, dup := k.keys[key.ID]; dup {
			return nil, errors.New("cursor: duplicate key id")
		}
		k.keys[key.ID] = append([]byte(nil), key.Secret...)
		if key.ID > k.current {
			k.current = key.ID
		}
	}
	return k, nil
}

func (b Binding) canonical() string {
	names := make([]string, 0, len(b.Filter))
	for n := range b.Filter {
		names = append(names, n)
	}
	sort.Strings(names)
	var sb strings.Builder
	sb.WriteString("journal-cursor-v1\x00" + b.Operation + "\x00" + strconv.FormatInt(b.TeamID, 10) + "\x00" + strconv.FormatInt(b.OperatorID, 10) + "\x00" + b.Order + "\x00forward")
	for _, n := range names {
		sb.WriteString("\x00" + n + "=" + strconv.FormatInt(b.Filter[n], 10))
	}
	return sb.String()
}

func mac(secret []byte, b Binding, keyID, last int64) []byte {
	m := hmac.New(sha256.New, secret)
	var buf [16]byte
	binary.BigEndian.PutUint64(buf[:8], uint64(keyID))
	binary.BigEndian.PutUint64(buf[8:], uint64(last))
	m.Write(buf[:])
	m.Write([]byte(b.canonical()))
	return m.Sum(nil)
}

// Sign issues a forward cursor positioned after last.
func (k *Keyring) Sign(b Binding, last int64) string {
	out := make([]byte, 16, 48)
	binary.BigEndian.PutUint64(out[:8], uint64(k.current))
	binary.BigEndian.PutUint64(out[8:], uint64(last))
	out = append(out, mac(k.keys[k.current], b, k.current, last)...)
	return base64.RawURLEncoding.EncodeToString(out)
}

// Verify returns the position of a cursor issued for b.
func (k *Keyring) Verify(token string, b Binding) (int64, error) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil || len(raw) != 48 {
		return 0, ErrInvalid
	}
	keyID := int64(binary.BigEndian.Uint64(raw[:8]))
	last := int64(binary.BigEndian.Uint64(raw[8:16]))
	secret, ok := k.keys[keyID]
	if !ok || last < 0 || !hmac.Equal(raw[16:], mac(secret, b, keyID, last)) {
		return 0, ErrInvalid
	}
	return last, nil
}
