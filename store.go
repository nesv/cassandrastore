package cassandrastore

import (
	"bytes"
	"encoding/base32"
	"encoding/gob"
	"fmt"
	"net/http"
	"strings"

	"github.com/gocql/gocql"
	"github.com/gorilla/securecookie"
	"github.com/gorilla/sessions"
)

// NewCassandraStore creates, and returns a new *CassandraStore. tableName lets
// you specify the name of the table which sessions should be stored in. If
// tableName is left as an empty string, it will default to "sessions".
//
// The only session options set by this function are sessions.Options.Path to
// "/", and sessions.Options.MaxAge to 2592000 (30 days).
//
// The connection to the database is not checked by this function.
func NewCassandraStore(config *gocql.ClusterConfig, tableName string, keypairs ...[]byte) (*CassandraStore, error) {
	if tableName == "" {
		tableName = "sessions"
	}

	return &CassandraStore{
		ClusterConfig: config,
		Codecs:        securecookie.CodecsFromPairs(keypairs...),
		Options: &sessions.Options{
			Path:   "/",
			MaxAge: 2592000,
		},
		TableName: tableName,
	}, nil
}

// CassandraStore stores sessions in a Cassandra database.
type CassandraStore struct {
	ClusterConfig *gocql.ClusterConfig
	Codecs        []securecookie.Codec
	Options       *sessions.Options
	TableName     string
}

func (c *CassandraStore) Get(r *http.Request, name string) (*sessions.Session, error) {
	return sessions.GetRegistry(r).Get(c, name)
}

func (c *CassandraStore) New(r *http.Request, name string) (*sessions.Session, error) {
	s := sessions.NewSession(c, name)
	opts := *c.Options
	s.Options = &opts
	s.IsNew = true

	if cookie, err := r.Cookie(name); err == nil {
		if e := securecookie.DecodeMulti(name, cookie.Value, &s.ID, c.Codecs...); e == nil {
			errLoad := c.load(s)
			if errLoad != nil && errLoad != gocql.ErrNotFound {
				s.IsNew = false
			}
		}
	}

	return s, nil
}

func (c *CassandraStore) Save(r *http.Request, w http.ResponseWriter, s *sessions.Session) error {
	if s.Options.MaxAge < 0 {
		// Don't worry about deleting the sessions from Cassandra, just blow
		// away the cookie.
		http.SetCookie(w, sessions.NewCookie(s.Name(), "", s.Options))
		return nil
	}

	if s.ID == "" {
		k := securecookie.GenerateRandomKey(32)
		s.ID = strings.TrimRight(base32.StdEncoding.EncodeToString(k), "=")
	}

	if err := c.save(s); err != nil {
		return fmt.Errorf("cassandrastore: %v", err.Error())
	}

	encoded, err := securecookie.EncodeMulti(s.Name(), s.ID, c.Codecs...)
	if err != nil {
		return fmt.Errorf("cassandrastore: %v", err.Error())
	}

	http.SetCookie(w, sessions.NewCookie(s.Name(), encoded, c.Options))
	return nil
}

// load reads in session information from the database.
//
// If the session exists in the database, this function will return true.
func (c *CassandraStore) load(s *sessions.Session) error {
	db, err := c.ClusterConfig.CreateSession()
	if err != nil {
		return fmt.Errorf("cassandrastore: %v", err.Error())
	}
	defer db.Close()

	var vals []byte
	q := `SELECT values FROM ` + c.TableName + ` WHERE id = ?`
	err = db.Query(q, s.ID).Scan(&vals)
	if err != nil {
		return fmt.Errorf("cassandrastore: %v", err.Error())
	}

	dec := gob.NewDecoder(bytes.NewBuffer(vals))
	return dec.Decode(&s.Values)
}

func (c *CassandraStore) save(s *sessions.Session) error {
	db, err := c.ClusterConfig.CreateSession()
	if err != nil {
		return fmt.Errorf("cassandrastore: %v", err.Error())
	}
	defer db.Close()

	buf := new(bytes.Buffer)
	enc := gob.NewEncoder(buf)
	if err = enc.Encode(s.Values); err != nil {
		return fmt.Errorf("cassandrastore: %v", err.Error())
	}

	q := `INSERT INTO ` + c.TableName + ` (id, values) VALUES (?, ?) USING TTL ?`
	return db.Query(q, s.ID, buf.Bytes(), s.Options.MaxAge).Exec()
}
