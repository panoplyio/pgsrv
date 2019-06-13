package protocol

import (
	"encoding/binary"
	"fmt"
)

// TypesOid maps between a type name to its corresponding OID
var TypesOid = map[string]int{
	"BOOL":       16,
	"BYTEA":      17,
	"CHAR":       18,
	"INT8":       20,
	"INT2":       21,
	"INT4":       23,
	"TEXT":       25,
	"JSON":       114,
	"XML":        142,
	"FLOAT4":     700,
	"FLOAT8":     701,
	"VARCHAR":    1043,
	"DATE":       1082,
	"TIME":       1083,
	"TIMESTAMP":  1114,
	"TIMESTAMPZ": 1184,
	"INTERVAL":   1186,
	"NUMERIC":    1700,
	"JSONB":      3802,
	"ANY":        2276,
}

// ReadyForQuery is sent whenever the backend is ready for a new query cycle.
var ReadyForQuery = []byte{'Z', 0, 0, 0, 5, 'I'}

// QueryText returns the SQL query string from a Query or Parse message
func (m Message) QueryText() (string, error) {
	if m.Type() != Query {
		return "", fmt.Errorf("not a query message: %q", m.Type())
	}

	return string(m[5:]), nil
}

// RowDescription is a message indicating that DataRow messages are about to
// be transmitted and delivers their schema (column names/types)
func RowDescription(cols, types []string) Message {
	msg := []byte{'T' /* LEN = */, 0, 0, 0, 0 /* NUM FIELDS = */, 0, 0}
	binary.BigEndian.PutUint16(msg[5:], uint16(len(cols)))

	for i, c := range cols {
		msg = append(msg, []byte(c)...)
		msg = append(msg, 0) // NULL TERMINATED

		msg = append(msg, 0, 0, 0, 0) // object ID of the table; otherwise zero
		msg = append(msg, 0, 0)       // attribute number of the column; otherwise zero

		// object ID of the field's data type
		typeOid := TypesOid[types[i]]
		if typeOid == 0 {
			typeOid = TypesOid["TEXT"]
		}

		oid := []byte{0, 0, 0, 0}
		binary.BigEndian.PutUint32(oid, uint32(typeOid))
		msg = append(msg, oid...)
		msg = append(msg, 0, 0)       // data type size
		msg = append(msg, 0, 0, 0, 0) // type modifier
		msg = append(msg, 0, 0)       // format code (text = 0, binary = 1)
	}

	// write the length
	binary.BigEndian.PutUint32(msg[1:5], uint32(len(msg)-1))
	return msg
}

// DataRow is sent for every row of resulted row set
func DataRow(vals []string) Message {
	msg := []byte{'D' /* LEN = */, 0, 0, 0, 0 /* NUM VALS = */, 0, 0}
	binary.BigEndian.PutUint16(msg[5:], uint16(len(vals)))

	for _, v := range vals {
		b := append(make([]byte, 4), []byte(v)...)
		binary.BigEndian.PutUint32(b[0:4], uint32(len(b)-4))
		msg = append(msg, b...)
	}

	// write the length
	binary.BigEndian.PutUint32(msg[1:5], uint32(len(msg)-1))
	return msg
}

// CommandComplete is sent when query was fully executed and cursor reached the end of the row set
func CommandComplete(tag string) Message {
	msg := []byte{'C', 0, 0, 0, 0}
	msg = append(msg, []byte(tag)...)
	msg = append(msg, 0) // NULL TERMINATED

	// write the length
	binary.BigEndian.PutUint32(msg[1:5], uint32(len(msg)-1))
	return msg
}

// ErrorResponse is sent whenever error has occurred
func ErrorResponse(err error) Message {
	msg := []byte{'E', 0, 0, 0, 0}

	// https://www.postgresql.org/docs/9.3/static/protocol-error-fields.html
	fields := map[string]string{
		"S": "ERROR", // Severity
		"C": "XX000",
		"M": err.Error(),
	}

	// severity
	errSeverity, ok := err.(interface {
		Severity() string
	})
	if ok && errSeverity.Severity() != "" {
		fields["S"] = errSeverity.Severity()
	}

	// error code
	errCode, ok := err.(interface {
		Code() string
	})
	if ok && errCode.Code() != "" {
		fields["C"] = errCode.Code()
	}

	// detail
	errDetail, ok := err.(interface {
		Detail() string
	})
	if ok && errDetail.Detail() != "" {
		fields["D"] = errDetail.Detail()
	}

	// hint
	errHint, ok := err.(interface {
		Hint() string
	})
	if ok && errHint.Hint() != "" {
		fields["H"] = errHint.Hint()
	}

	// cursor position
	errPosition, ok := err.(interface {
		Position() int
	})
	if ok && errPosition.Position() >= 0 {
		fields["P"] = fmt.Sprintf("%d", errPosition.Position())
	}

	for k, v := range fields {
		msg = append(msg, byte(k[0]))
		msg = append(msg, []byte(v)...)
		msg = append(msg, 0) // NULL TERMINATED
	}

	msg = append(msg, 0) // NULL TERMINATED

	// write the length
	binary.BigEndian.PutUint32(msg[1:5], uint32(len(msg)-1))
	return msg
}
