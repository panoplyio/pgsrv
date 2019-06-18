package pgsrv

import (
	"context"
	"database/sql/driver"
	nodes "github.com/lfittl/pg_query_go/nodes"
	"net"
)

// Queryer is a generic interface for objects capable of performing sql queries.
// The returned Rows object provides the API for reading the row data as well as
// metadata (like Columns, types, etc.)
type Queryer interface {
	Query(ctx context.Context, n nodes.Node) (driver.Rows, error)
}

// Execer is a generic interface for objects capable of executing sql write
// commands, like INSERT or CREATE TABLE. The returned Result object provides
// the API for reading the number of affected rows.
type Execer interface {
	Exec(ctx context.Context, n nodes.Node) (driver.Result, error)
}

// ResultTag can be implemented by driver.Result to provide the tag name to be
// used to notify the postgres client of the completed command. If left
// unimplemented, the default behavior follows the spec described in the link
// below. For all un-documented cases, "UPDATE N" will be used, where N is the
// number of affected rows.
// See CommandComplete: https://www.postgresql.org/docs/10/static/protocol-message-formats.html
type ResultTag interface {
	Tag() (string, error)
}

// Session represents a connected client session. It provides the API to set,
// get, delete and accessing all of the session variables. The session should
// added to the context of all queries executed via the "Session" key:
//
//      ctx.Value("Session").(Session)
//
type Session interface {
	Set(k string, v interface{})
	Get(k string) interface{}
	Del(k string)
	All() map[string]interface{}
}

// Server is an interface for objects capable for handling the postgres transport
// by serving client connections. Each connection is assigned a Session that's
// maintained in-memory until the connection is closed.
type Server interface {
	// Manually serve a connection
	Serve(net.Conn) error // blocks. Run in go-routine.
}

// general pgsrv constants to manage session and queries info
type ctxKey string

const (
	sessionCtxKey ctxKey = "Session"
	sqlCtxKey     ctxKey = "SQL"
	astCtxKey     ctxKey = "AST"
)
