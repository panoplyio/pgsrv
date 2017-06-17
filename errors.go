package pgsrv

import (
    "fmt"
)

type Err interface {
    error
    WithHint(hint string) ErrHinter
    WithCode(code string) ErrCoder
    WithLoc(loc int) ErrLocer
}

// Error object that includes a hint text
type ErrHinter interface {
    Hint() string
}

// Error object that includes an error code
// See list of available error codes here:
//      https://www.postgresql.org/docs/10/static/errcodes-appendix.html
type ErrCoder interface {
    Code() string
}

type Locer interface {
    Loc() int
}

// Undefined indicates that a certain entity (function, column, etc.) is not
// registered or available for use.
func Undefined(msg string, args ...interface{}) Err {
    return fmt.Errorf("Undefined " + msg, args...)
}

// Invalid indicates that the user request is invalid or otherwise incorrect.
// It's very much similar to a syntax error, except that the invalidity is
// logical within the request rather than syntactic. For example, using a non-
// boolean expression in WHERE
func Invalid(msg string, args ...interface{}) Err {
    return fmt.Errorf("Invalid " + msg, args...)
}

// Unsupported indicates that a certain feature is not supported. Unlike
// Undefined - this error is not for cases where a user-space entity is not
// recognized but when the recognized entity cannot perform some of its
// functionality
func Unsupported(msg string, args ...interface{}) Err {
    return fmt.Errorf("Unsupported " + msg, args...)
}
