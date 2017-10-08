package gomsg

import (
	"bufio"
	"encoding/binary"
	"io"

	"github.com/quintans/toolkit/faults"
)

type StreamFactory interface {
	Input(io.Reader) InputStream
	Output(io.Writer) OutputStream
}

type BinStreamFactory struct {
}

var _ StreamFactory = BinStreamFactory{}

func (this BinStreamFactory) Input(r io.Reader) InputStream {
	return NewInputBinStream(r)
}

func (this BinStreamFactory) Output(w io.Writer) OutputStream {
	return NewOutputBinStream(w)
}

const BIN_STRING_END byte = 0

type InputStream interface {
	Read([]byte) (int, error)
	ReadNBytes(int) ([]byte, error)
	ReadUI8() (uint8, error)
	ReadUI16() (uint16, error)
	ReadUI32() (uint32, error)
	ReadUI64() (uint64, error)
	ReadString() (string, error)
	ReadBytes() ([]byte, error)
}

var _ InputStream = new(InputBinStream)

type InputBinStream struct {
	reader *bufio.Reader
	//reader io.Reader
}

func NewInputBinStream(reader io.Reader) *InputBinStream {
	this := &InputBinStream{bufio.NewReader(reader)}
	return this
}

func (this *InputBinStream) Read(p []byte) (n int, err error) {
	return this.reader.Read(p)
}

func (this *InputBinStream) ReadNBytes(size int) ([]byte, error) {
	var data = make([]byte, size)
	if _, err := io.ReadFull(this.reader, data); err != nil {
		return nil, faults.Wrap(err)
	}
	return data, nil
}

func (this *InputBinStream) ReadUI8() (uint8, error) {
	buf, err := this.ReadNBytes(1)
	if err != nil {
		return 0, err
	}
	return buf[0], err
}

func (this *InputBinStream) ReadUI16() (uint16, error) {
	buf, err := this.ReadNBytes(2)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint16(buf), nil
}

func (this *InputBinStream) ReadUI32() (uint32, error) {
	buf, err := this.ReadNBytes(4)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(buf), nil
}

func (this *InputBinStream) ReadUI64() (uint64, error) {
	buf, err := this.ReadNBytes(8)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(buf), nil
}

func (this *InputBinStream) ReadString() (string, error) {
	bytes, err := this.reader.ReadBytes(BIN_STRING_END)
	if err != nil {
		return "", faults.Wrap(err)
	}
	return string(bytes[:len(bytes)-1]), nil
}

func (this *InputBinStream) ReadBytes() ([]byte, error) {
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

type OutputStream interface {
	Write([]byte) (n int, err error)
	WriteUI8(uint8) error
	WriteUI16(uint16) error
	WriteUI32(uint32) error
	WriteUI64(uint64) error
	WriteString(string) error
	WriteBytes([]byte) error
}

var _ OutputStream = new(OutputBinStream)

type OutputBinStream struct {
	writer io.Writer
}

func NewOutputBinStream(writer io.Writer) *OutputBinStream {
	this := &OutputBinStream{writer}
	return this
}

func (this *OutputBinStream) Write(p []byte) (n int, err error) {
	return this.writer.Write(p)
}

func (this *OutputBinStream) WriteUI8(data uint8) error {
	var buf8 = make([]byte, 1)
	buf8[0] = data
	_, err := this.writer.Write(buf8)
	return faults.Wrap(err)
}

func (this *OutputBinStream) WriteUI16(data uint16) error {
	var buf16 = make([]byte, 2)
	binary.LittleEndian.PutUint16(buf16, data)
	_, err := this.writer.Write(buf16)
	return faults.Wrap(err)
}

func (this *OutputBinStream) WriteUI32(data uint32) error {
	var buf32 = make([]byte, 4)
	binary.LittleEndian.PutUint32(buf32, data)
	_, err := this.writer.Write(buf32)
	return faults.Wrap(err)
}

func (this *OutputBinStream) WriteUI64(data uint64) error {
	var buf64 = make([]byte, 8)
	binary.LittleEndian.PutUint64(buf64, data)
	_, err := this.writer.Write(buf64)
	return faults.Wrap(err)
}

func (this *OutputBinStream) WriteString(s string) error {
	_, err := this.writer.Write([]byte(s))
	if err != nil {
		return faults.Wrap(err)
	}
	_, err = this.writer.Write([]byte{BIN_STRING_END})
	if err != nil {
		return faults.Wrap(err)
	}
	return nil
}

func (this *OutputBinStream) WriteBytes(data []byte) error {
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
