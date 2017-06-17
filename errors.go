package pgsrv

import (
    "fmt"
)

// Err is a postgres-compatible error object. It's not required to be used, as
// any other normal error object would be converted to a generic internal error,
// but it provides the API to generate user-friendly error messages. Note that
// all of the construction functions (prefixed with With*) are updating the same
// error, and does not create a new one. The same error is returned for
// chaining. See:
//
//      https://www.postgresql.org/docs/9.3/static/protocol-error-fields.html
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
// For the full list of error codes, see:
//
//      See: https://www.postgresql.org/docs/10/static/errcodes-appendix.html
type Err error

type err struct {
    M string // Message
    H string // Hint
    C string // Code
    L int    // Location
}

func (e *err) Error() string { return e.M }
func (e *err) Hint() string { return e.H }
func (e *err) Code() string { return e.C }
func (e *err) Loc() int { return e.L }

// WithLoc decorates an error object to also include the cursor position
// location) for the error in the original query text. This is useful to provide
// the client with a specific marker of where the error occured in his SQL
func WithLoc(err error, loc int) Err {
    return nil
}

// WithHint decorates an error object to also include a suggestion what to do
// about the problem. This is intended to differ from Detail in that it offers
// advice (potentially inappropriate) rather than hard facts. Might run to
// multiple lines
func WithHint(err error, hint string, args ...interface{}) Err {
    return nil
}

// Undefined indicates that a certain entity (function, column, etc.) is not
// registered or available for use.
func Undefined(msg string, args ...interface{}) Err {
    msg = fmt.Sprintf("Undefined " + msg, args...)
    return &err{M: msg, C: "42703"}
}

// Invalid indicates that the user request is invalid or otherwise incorrect.
// It's very much similar to a syntax error, except that the invalidity is
// logical within the request rather than syntactic. For example, using a non-
// boolean expression in WHERE
func Invalid(msg string, args ...interface{}) Err {
    msg = fmt.Sprintf("Invalid " + msg, args...)
    return &err{M: msg, C: "22000"}
}

// Unsupported indicates that a certain feature is not supported. Unlike
// Undefined - this error is not for cases where a user-space entity is not
// recognized but when the recognized entity cannot perform some of its
// functionality
func Unsupported(msg string, args ...interface{}) Err {
    msg = fmt.Sprintf("Unsupported " + msg, args...)
    return &err{M: msg, C: "0A000"}
}
