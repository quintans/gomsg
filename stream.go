package gomsg

import (
	"bytes"
	"encoding/binary"
	"io"

	"github.com/quintans/toolkit/faults"
)

const STRING_END byte = 0

type InputStream struct {
	reader io.Reader
}

func NewInputStream(reader io.Reader) *InputStream {
	this := &InputStream{reader}
	return this
}

func (this *InputStream) Read(p []byte) (n int, err error) {
	return this.reader.Read(p)
}

func (this *InputStream) ReadNBytes(size int) ([]byte, error) {
	var data = make([]byte, size)
	if _, err := io.ReadFull(this.reader, data); err != nil {
		return nil, faults.Wrap(err)
	}
	return data, nil
}

func (this *InputStream) ReadUI8() (uint8, error) {
	buf, err := this.ReadNBytes(1)
	if err != nil {
		return 0, err
	}
	return buf[0], err
}

func (this *InputStream) ReadUI16() (uint16, error) {
	buf, err := this.ReadNBytes(2)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint16(buf), nil
}

func (this *InputStream) ReadUI32() (uint32, error) {
	buf, err := this.ReadNBytes(4)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(buf), nil
}

func (this *InputStream) ReadUI64() (uint64, error) {
	buf, err := this.ReadNBytes(8)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(buf), nil
}

func (this *InputStream) ReadString() (string, error) {
	data := []byte{0}
	var s bytes.Buffer
	for {
		if _, err := io.ReadFull(this.reader, data); err != nil {
			return "", faults.Wrap(err)
		}
		if data[0] == STRING_END {
			break
		} else {
			s.WriteByte(data[0])
		}
	}
	return string(s.Bytes()), nil
}

func (this *InputStream) ReadBytes() ([]byte, error) {
	// reads byte array size
	size, err := this.ReadUI16()
	if err != nil {
		return nil, faults.Wrap(err)
	}

	if size > 0 {
		return this.ReadNBytes(int(size))
	}
	return nil, nil
}

// =============

type OutputStream struct {
	writer io.Writer
}

func NewOutputStream(writer io.Writer) *OutputStream {
	this := &OutputStream{writer}
	return this
}

func (this *OutputStream) Write(p []byte) (n int, err error) {
	return this.writer.Write(p)
}

func (this *OutputStream) WriteUI8(data uint8) error {
	var buf8 = make([]byte, 1)
	buf8[0] = data
	_, err := this.writer.Write(buf8)
	return faults.Wrap(err)
}

func (this *OutputStream) WriteUI16(data uint16) error {
	var buf16 = make([]byte, 2)
	binary.LittleEndian.PutUint16(buf16, data)
	_, err := this.writer.Write(buf16)
	return faults.Wrap(err)
}

func (this *OutputStream) WriteUI32(data uint32) error {
	var buf32 = make([]byte, 4)
	binary.LittleEndian.PutUint32(buf32, data)
	_, err := this.writer.Write(buf32)
	return faults.Wrap(err)
}

func (this *OutputStream) WriteUI64(data uint64) error {
	var buf64 = make([]byte, 8)
	binary.LittleEndian.PutUint64(buf64, data)
	_, err := this.writer.Write(buf64)
	return faults.Wrap(err)
}

func (this *OutputStream) WriteString(s string) error {
	data := append([]byte(s), STRING_END)
	_, err := this.writer.Write([]byte(data))
	return faults.Wrap(err)
}

func (this *OutputStream) WriteBytes(data []byte) error {
	// array size
	size := uint16(len(data))
	err := this.WriteUI16(size)
	if err != nil {
		return faults.Wrap(err)
	}
	// string data
	if size > 0 {
		_, err = this.writer.Write(data)
		if err != nil {
			return faults.Wrap(err)
		}
	}
	return nil
}
