package pgsrv

import (
    "fmt"
)

type Err interface {
    error

    WithHint(hint string, args ...interface{}) Err

    // https://www.postgresql.org/docs/10/static/errcodes-appendix.html
    WithCode(code string) Err
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
    return &err{M: msg}
}

// Invalid indicates that the user request is invalid or otherwise incorrect.
// It's very much similar to a syntax error, except that the invalidity is
// logical within the request rather than syntactic. For example, using a non-
// boolean expression in WHERE
func Invalid(msg string, args ...interface{}) Err {
    msg = fmt.Sprintf("Invalid " + msg, args...)
    return &err{M: msg}
}

// Unsupported indicates that a certain feature is not supported. Unlike
// Undefined - this error is not for cases where a user-space entity is not
// recognized but when the recognized entity cannot perform some of its
// functionality
func Unsupported(msg string, args ...interface{}) Err {
    msg = fmt.Sprintf("Unsupported " + msg, args...)
    return &err{M: msg}
}
