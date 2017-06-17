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
type Err interface {
    error

    // WithHints sets an optional suggestion what to do about the problem. This
    // is intended to differ from Detail in that it offers advice (potentially
    // inappropriate) rather than hard facts. Might run to multiple lines
    WithHint(hint string, args ...interface{}) Err

    // WithCode sets a the SQLSTATE code for the error. Not localizable.
    // You can also use the genric Error constructors (Undefined, Invalid, etc.)
    // to generate errors with preset error codes
    // See: https://www.postgresql.org/docs/10/static/errcodes-appendix.html
    WithCode(code string) Err

    // WithLoc sets the cursor position (location) for the error in the original
    // query text. This is useful to provide the client with a specific marker
    // of where the error occured in his SQL
    WithLoc(loc int) Err
}

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
func (e *err) WithCode(code string) Err { e.C = code; return e }
func (e *err) WithLoc(loc int) Err { e.L = loc; return e }
func (e *err) WithHint(hint string, args ...interface{}) Err {
    e.H = fmt.Sprintf(hint, args...)
    return e
}

// Undefined indicates that a certain entity (function, column, etc.) is not
// registered or available for use.
func Undefined(msg string, args ...interface{}) Err {
    msg = fmt.Sprintf("Undefined " + msg, args...)
    return (&err{M: msg}).WithCode("42703")
}

// Invalid indicates that the user request is invalid or otherwise incorrect.
// It's very much similar to a syntax error, except that the invalidity is
// logical within the request rather than syntactic. For example, using a non-
// boolean expression in WHERE
func Invalid(msg string, args ...interface{}) Err {
    msg = fmt.Sprintf("Invalid " + msg, args...)
    return (&err{M: msg}).WithCode("22000")
}

// Unsupported indicates that a certain feature is not supported. Unlike
// Undefined - this error is not for cases where a user-space entity is not
// recognized but when the recognized entity cannot perform some of its
// functionality
func Unsupported(msg string, args ...interface{}) Err {
    msg = fmt.Sprintf("Unsupported " + msg, args...)
    return (&err{M: msg}).WithCode("0A000")
}
