package gomsg

import (
	"bufio"
	"encoding/base64"
	"encoding/binary"
	"io"

	"github.com/quintans/toolkit/faults"
)

const TXT_STRING_END byte = '\n'

type TxtStreamFactory struct {
}

var _ StreamFactory = TxtStreamFactory{}

func (this TxtStreamFactory) Input(r io.Reader) InputStream {
	return NewInputTxtStream(r)
}

func (this TxtStreamFactory) Output(w io.Writer) OutputStream {
	return NewOutputTxtStream(w)
}

var _ InputStream = new(InputTxtStream)

// InputTxtStream will read data one item per line
type InputTxtStream struct {
	reader *bufio.Reader
}

func NewInputTxtStream(r io.Reader) *InputTxtStream {
	this := &InputTxtStream{bufio.NewReader(r)}
	return this
}

func (this *InputTxtStream) Read(p []byte) (n int, err error) {
	var line string
	line, err = this.reader.ReadString('\n')
	if err != nil {
		return
	}
	var data []byte
	data, err = base64.StdEncoding.DecodeString(line)
	if err != nil {
		return
	}
	n = len(data)
	if n > len(p) {
		n = len(p)
	}
	copy(p, data[:n])

	return this.reader.Read(p)
}

func (this *InputTxtStream) ReadNBytes(size int) ([]byte, error) {
	var data = make([]byte, size)
	var _, err = this.Read(data)
	if err != nil {
		return nil, faults.Wrap(err)
	}
	return data, nil
}

func (this *InputTxtStream) ReadUI8() (uint8, error) {
	buf, err := this.ReadNBytes(1)
	if err != nil {
		return 0, err
	}
	return buf[0], err
}

func (this *InputTxtStream) ReadUI16() (uint16, error) {
	buf, err := this.ReadNBytes(2)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint16(buf), nil
}

func (this *InputTxtStream) ReadUI32() (uint32, error) {
	buf, err := this.ReadNBytes(4)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint32(buf), nil
}

func (this *InputTxtStream) ReadUI64() (uint64, error) {
	buf, err := this.ReadNBytes(8)
	if err != nil {
		return 0, err
	}
	return binary.LittleEndian.Uint64(buf), nil
}

func (this *InputTxtStream) ReadString() (string, error) {
	bytes, err := this.reader.ReadBytes(TXT_STRING_END)
	if err != nil {
		return "", err
	}
	return string(bytes[:len(bytes)-1]), nil
}

func (this *InputTxtStream) ReadBytes() ([]byte, error) {
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

var _ OutputStream = new(OutputTxtStream)

type OutputTxtStream struct {
	writer io.Writer
}

func NewOutputTxtStream(writer io.Writer) *OutputTxtStream {
	this := &OutputTxtStream{writer}
	return this
}

func (this *OutputTxtStream) Write(p []byte) (n int, err error) {
	var s = base64.StdEncoding.EncodeToString(p)
	n, err = this.writer.Write([]byte(s))
	if err != nil {
		return
	}
	// new line
	return this.writer.Write([]byte("\n"))
}

func (this *OutputTxtStream) WriteUI8(data uint8) error {
	var buf8 = make([]byte, 1)
	buf8[0] = data
	_, err := this.Write(buf8)
	return faults.Wrap(err)
}

func (this *OutputTxtStream) WriteUI16(data uint16) error {
	var buf16 = make([]byte, 2)
	binary.LittleEndian.PutUint16(buf16, data)
	_, err := this.Write(buf16)
	return faults.Wrap(err)
}

func (this *OutputTxtStream) WriteUI32(data uint32) error {
	var buf32 = make([]byte, 4)
	binary.LittleEndian.PutUint32(buf32, data)
	_, err := this.Write(buf32)
	return faults.Wrap(err)
}

func (this *OutputTxtStream) WriteUI64(data uint64) error {
	var buf64 = make([]byte, 8)
	binary.LittleEndian.PutUint64(buf64, data)
	_, err := this.Write(buf64)
	return faults.Wrap(err)
}

func (this *OutputTxtStream) WriteString(s string) error {
	_, err := this.writer.Write([]byte(s))
	if err != nil {
		return faults.Wrap(err)
	}
	_, err = this.writer.Write([]byte{TXT_STRING_END})
	if err != nil {
		return faults.Wrap(err)
	}
	return nil
}

func (this *OutputTxtStream) WriteBytes(data []byte) error {
	// array size
	size := uint16(len(data))
	err := this.WriteUI16(size)
	if err != nil {
		return faults.Wrap(err)
	}
	// string data
	if size > 0 {
		_, err = this.Write(data)
		if err != nil {
			return faults.Wrap(err)
		}
	}
	return nil
}
