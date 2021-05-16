package shared

import (
	"bytes"
	crypto_rand "crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	math_rand "math/rand"
)

// Serialization

func EncodeStringToBuffer(s string, buffer *bytes.Buffer) error {
	sb := []byte(s)
	err := binary.Write(buffer, binary.BigEndian, uint16(len(sb)))
	if err != nil {
		return err
	}
	n, err := buffer.Write(sb)
	if n != len(sb) {
		return errors.New(fmt.Sprint("Expected to write", len(sb), "wrote", n))
	}
	if err != nil {
		return err
	}
	return nil
}

func NewRand() *math_rand.Rand {
	var b [8]byte
	_, err := crypto_rand.Read(b[:])
	if err != nil {
		panic("cannot seed math/rand package with cryptographically secure random number generator")
	}
	return math_rand.New(math_rand.NewSource(int64(binary.LittleEndian.Uint64(b[:]))))
}

func DecodeStringFromBuffer(buffer *bytes.Buffer) (string, error) {
	var sLen uint16
	err := binary.Read(buffer, binary.BigEndian, &sLen)
	if err != nil {
		return "", err
	}

	sb := make([]byte, sLen)
	n, err := buffer.Read(sb)
	if n != int(sLen) {
		return "", errors.New(fmt.Sprint("Expected to read", sLen, "read", n))
	}

	if err != nil {
		return "", err
	}

	return string(sb), nil
}

func EncodeNumberToBuffer(n interface{}, buffer *bytes.Buffer) error {
	return binary.Write(buffer, binary.BigEndian, n)
}

func DecodeNumberFromBuffer(nPointer interface{}, buffer *bytes.Buffer) error {
	return binary.Read(buffer, binary.BigEndian, nPointer)
}
