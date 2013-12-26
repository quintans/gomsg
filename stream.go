package gobus

import (
	"bytes"
	"encoding/binary"
	"io"
)

type CustomReader struct {
	Reader io.Reader
	buf8   []byte
	buf16  []byte
	buf32  []byte
	buf64  []byte
}

func NewCustomReader(reader io.Reader) *CustomReader {
	this := &CustomReader{Reader: reader}
	return this
}

func (this *CustomReader) ReadUI8() (uint8, error) {
	if this.buf8 == nil {
		this.buf8 = make([]byte, 1)
	}
	_, err := this.Reader.Read(this.buf8)
	if err != nil {
		return 0, err
	} else {
		return this.buf8[0], err
	}
}

func (this *CustomReader) ReadUI16() (uint16, error) {
	if this.buf16 == nil {
		this.buf16 = make([]byte, 2)
	}
	_, err := io.ReadFull(this.Reader, this.buf16)
	if err != nil {
		return 0, err
	}
	data := binary.LittleEndian.Uint16(this.buf16)
	return data, nil
}

func (this *CustomReader) ReadUI32() (uint32, error) {
	if this.buf32 == nil {
		this.buf32 = make([]byte, 4)
	}
	_, err := io.ReadFull(this.Reader, this.buf32)
	if err != nil {
		return 0, err
	}
	data := binary.LittleEndian.Uint32(this.buf32)
	return data, nil
}

func (this *CustomReader) ReadUI64() (uint64, error) {
	if this.buf64 == nil {
		this.buf64 = make([]byte, 8)
	}
	_, err := io.ReadFull(this.Reader, this.buf64)
	if err != nil {
		return 0, err
	}
	data := binary.LittleEndian.Uint64(this.buf64)
	return data, nil
}

func (this *CustomReader) ReadString() (string, error) {
	// reads message name size
	size, err := this.ReadUI16()
	if err != nil {
		return "", err
	}
	if size > 0 {
		in := bytes.NewBuffer(make([]byte, 0))
		_, err = io.CopyN(in, this.Reader, int64(size))
		if err != nil {
			return "", err
		}
		return string(in.Bytes()), nil
	}
	return "", nil
}

func (this *CustomReader) ReadBytes() ([]byte, error) {
	// reads message name size
	size, err := this.ReadUI32()
	if err != nil {
		return nil, err
	}
	if size > 0 {
		in := bytes.NewBuffer(make([]byte, 0))
		_, err = io.CopyN(in, this.Reader, int64(size))
		if err != nil {
			return nil, err
		}
		return in.Bytes(), nil
	}
	return nil, nil
}

// =============

type CustomWriter struct {
	Writer io.Writer
	buf8   []byte
	buf16  []byte
	buf32  []byte
	buf64  []byte
}

func NewCustomWriter(writer io.Writer) *CustomWriter {
	this := &CustomWriter{Writer: writer}
	return this
}

func (this *CustomWriter) WriteUI8(data uint8) error {
	if this.buf8 == nil {
		this.buf8 = make([]byte, 1)
	}
	this.buf8[0] = data
	_, err := this.Writer.Write(this.buf8)
	return err
}

func (this *CustomWriter) WriteUI16(data uint16) error {
	if this.buf16 == nil {
		this.buf16 = make([]byte, 2)
	}
	binary.LittleEndian.PutUint16(this.buf16, data)
	_, err := this.Writer.Write(this.buf16)
	return err
}

func (this *CustomWriter) WriteUI32(data uint32) error {
	if this.buf32 == nil {
		this.buf32 = make([]byte, 4)
	}
	binary.LittleEndian.PutUint32(this.buf32, data)
	_, err := this.Writer.Write(this.buf32)
	return err
}

func (this *CustomWriter) WriteUI64(data uint64) error {
	if this.buf64 == nil {
		this.buf64 = make([]byte, 8)
	}
	binary.LittleEndian.PutUint64(this.buf64, data)
	_, err := this.Writer.Write(this.buf64)
	return err
}

func (this *CustomWriter) WriteString(data string) error {
	// string size
	size := uint16(len(data))
	err := this.WriteUI16(size)
	if err != nil {
		return err
	}
	// string data
	if size > 0 {
		_, err = this.Writer.Write([]byte(data))
		if err != nil {
			return err
		}
	}
	return nil
}

func (this *CustomWriter) WriteBytes(data []byte) error {
	// string size
	size := uint32(len(data))
	err := this.WriteUI32(size)
	if err != nil {
		return err
	}
	// string data
	if size > 0 {
		_, err = this.Writer.Write([]byte(data))
		if err != nil {
			return err
		}
	}
	return nil
}
