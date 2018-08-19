package pgsrv

import (
	"fmt"
)

// fatalSeverity error terminates session
const fatalSeverity = "FATAL"

// Err is a postgres-compatible error object. It's not required to be used, as
// any other normal error object would be converted to a generic internal error,
// but it provides the API to generate user-friendly error messages. Note that
// all of the construction functions (prefixed with With*) are updating the same
// error, and does not create a new one. The same error is returned for
// chaining. See: https://www.postgresql.org/docs/9.3/static/protocol-error-fields.html
//
// Postgres has hundreds of different error codes, broken into categories. Use
// the constructors below (Invalid, Unsupported, etc.) to create errors with
// preset error codes. If you can't find the one you need, consider adding it
// here as a generic constructor. Otherwise, you can implement an object that
// adheres to this interface:
//
//      interface {
//          error
//          Code() string
//      }
//
// For the full list of error codes, see: https://www.postgresql.org/docs/10/static/errcodes-appendix.html
type Err error

type err struct {
	S string // Severity
	C string // Code
	M string // Message
	D string // Detail
	H string // Hint
	P int    // Position
}

func (e *err) Severity() string { return e.S }
func (e *err) Code() string     { return e.C }
func (e *err) Error() string    { return e.M }
func (e *err) Detail() string   { return e.D }
func (e *err) Hint() string     { return e.H }
func (e *err) Position() int    { return e.P }

// WithSeverity decorates an error object to also include an optional severity
func WithSeverity(err error, severity string) Err {
	if err == nil {
		return nil
	}

	e := fromErr(err)
	e.S = severity
	return e
}

// WithDetail decorates an error object to also include  an optional secondary
// error message carrying more detail about the problem. Might run to multiple
// lines
func WithDetail(err error, detail string, args ...interface{}) Err {
	if err == nil {
		return nil
	}

	e := fromErr(err)
	e.D = fmt.Sprintf(detail, args...)
	return e
}

// WithHint decorates an error object to also include an optional suggestion what
// to do about the problem. This is intended to differ from Detail in that it
// offers advice (potentially inappropriate) rather than hard facts. Might run
// to multiple lines
func WithHint(err error, hint string, args ...interface{}) Err {
	if err == nil {
		return nil
	}

	e := fromErr(err)
	e.H = fmt.Sprintf(hint, args...)
	return e
}

// WithPosition decorates an error object to also include the cursor position
// (location) for the error in the original query string. Positions are measured
// in characters not bytes. This is useful to provide the client a specific
// marker of where the error occurred in his SQL
func WithPosition(err error, position int) Err {
	if err == nil {
		return nil
	}

	e := fromErr(err)

	// keep the bottom-most position, which is the origin of the error,
	// by prevent top-level errors to override actual origin position
	if e.P < 0 {
		e.P = position
	}
	return e
}

// Unrecognized indicates that a certain entity (function, column, etc.) is not
// registered or available for use.
func Unrecognized(msg string, args ...interface{}) Err {
	msg = fmt.Sprintf("unrecognized "+msg, args...)
	return &err{M: msg, C: "42000", P: -1}
}

// Invalid indicates that the user request is invalid or otherwise incorrect.
// It's very much similar to a syntax error, except that the invalidity is
// logical within the request rather than syntactic. For example, using a non-
// boolean expression in WHERE, or when a requested data type, table, or
// function is undefined.
func Invalid(msg string, args ...interface{}) Err {
	msg = fmt.Sprintf("invalid "+msg, args...)
	return &err{M: msg, C: "42000", P: -1}
}

// Disallowed indicates a permissions, authorization or permanently disallowed
// operation - access to table data, alerting users, etc.
func Disallowed(msg string, args ...interface{}) Err {
	msg = fmt.Sprintf("disallowed "+msg, args...)
	return &err{M: msg, C: "42000", P: -1}
}

// Unsupported indicates that a certain feature is not supported. Unlike
// Undefined - this error is not for cases where a user-space entity is not
// recognized but when the recognized entity cannot perform some of its
// functionality
func Unsupported(msg string, args ...interface{}) Err {
	msg = fmt.Sprintf("unsupported "+msg, args...)
	return &err{M: msg, C: "0A000", P: -1}
}

func fromErr(e error) *err {
	err1, ok := e.(*err)
	if ok {
		return err1
	}

	severitier, ok := e.(interface {
		Severity() string
	})
	s := ""
	if ok {
		s = severitier.Severity()
	}

	coder, ok := e.(interface {
		Code() string
	})
	c := ""
	if ok {
		c = coder.Code()
	}

	m := e.Error()

	detailer, ok := e.(interface {
		Detail() string
	})
	d := ""
	if ok {
		d = detailer.Detail()
	}

	hinter, ok := e.(interface {
		Hint() string
	})
	h := ""
	if ok {
		h = hinter.Hint()
	}

	positioner, ok := e.(interface {
		Position() int
	})
	p := -1
	if ok {
		p = positioner.Position()
	}

	return &err{S: s, C: c, M: m, D: d, H: h, P: p}
}
