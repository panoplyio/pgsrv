package postgressrv

import (
    "io"
    "encoding/binary"
)

// Read reads and returns a single message from the connection.
//
// The Postgres protocol supports two types of messages: (1) untyped messages
// are only mostly present during the initial startup process and starts with
// the length of the message, followed by the content. (2) typed messages are
// similar to the untyped messages except that they're prefixed with a
// single-byte type character used to distinguish between the different message
// types (query, prepare, etc.), and are the normal messages used for most
// client-server communications.
//
// This method abstracts away this differentiation, returning the next available
// message whether it's typed or not.
func (s *Session) Read() (Msg, error) {
    typechar := make([]byte, 1)
    if s.initialized {

        // we've already started up, so all future messages are MUST start with
        // a single-byte type identifier.
        _, err := s.Conn.Read(typechar)
        if err != nil {
            return nil, err
        }
    }

    // read the actual body of the message
    msg, err := s.readBody()
    if err != nil {
        return nil, err
    }

    if typechar[0] != 0 {

        // we have a typed-message, prepend it to the message body by first
        // creating a new message that's 1-byte longer than the body in order to
        // make room in memory for the type byte
        body := msg
        msg = make([]byte, len(body) + 1)

        // fixing the type byte at the beginning (position 0) of the new message
        msg[0] = typechar[0]

        // finally append the body to the new message, starting from position 1
        copy(msg[1:], body)
    }

    return NewMsg(msg), nil
}

// ReadMsgBody reads the body of the next message in the connection. The body is
// comprised of an Int32 body-length (N), inclusive of the length itself
// followed by N-bytes of the actual body.
func (s *Session) readBody() ([]byte, error) {

    // messages starts with an Int32 Length of message contents in bytes,
    // including self.
    lenbytes := make([]byte, 4)
    _, err := io.ReadFull(s.Conn, lenbytes)
    if err != nil {
        return nil, err
    }

    // convert the 4-bytes to int
    length := int(binary.BigEndian.Uint32(lenbytes))

    // read the remaining bytes in the message
    msg := make([]byte, length)
    _, err = io.ReadFull(s.Conn, msg[4:]) // keep 4 bytes for the length
    if err != nil {
        return nil, err
    }

    // append the message content to the length bytes in order to rebuild the
    // original message in its entirety
    copy(msg[:4], lenbytes)
    return msg, nil
}

// WriteMsg writes the provided msg to the client connection
func (s *Session) Write(m Msg) error {
    _, err := s.Conn.Write(m)
    return err
}
