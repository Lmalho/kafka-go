package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

const (
	UNSUPPORTED_VERSION int16 = 35
)

const (
	API_VERSIONS = 18
)

type Response struct {
	size          int32
	correlationId int32
	errorCode     int16
}

// Response structs
type Request struct {
	size          int32
	apiKey        int16
	apiVersion    int16
	correlationId int32
}

func main() {
	// You can use print statements as follows for debugging, they'll be visible when running tests.
	fmt.Println("Logs from your program will appear here!")

	// Uncomment this block to pass the first stage
	//
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	c, err := l.Accept()
	if err != nil {
		fmt.Println("Error accepting connection: ", err.Error())
		os.Exit(1)
	}

	defer c.Close()

	reqBuf := make([]byte, 1024)
	n, err := c.Read(reqBuf)
	if err != nil {
		if err != io.EOF {
			fmt.Println("Error reading from the connection")
		}
	}
	fmt.Printf("Received %d bytes", n)

	req := DecodeRequest(reqBuf)

	res := responseService(req)

	c.Write(res.Marshal())
}

// mesSize  apiKey  apiVersion  corrId
// [0000]   [00]    [00]        [0000]  [...]

func DecodeRequest(request []byte) Request {
	req := Request{
		size:          int32(binary.BigEndian.Uint32(request[0:4])),
		apiKey:        int16(binary.BigEndian.Uint16(request[4:6])),
		apiVersion:    int16(binary.BigEndian.Uint16(request[6:8])),
		correlationId: int32(binary.BigEndian.Uint32(request[8:12])),
	}
	return req
}

func responseService(request Request) Response {
	var errorCode int16 = 0

	if request.apiVersion < 0 || request.apiVersion > 4 {
		errorCode = UNSUPPORTED_VERSION
	}

	fmt.Println(request.apiVersion)
	fmt.Println(errorCode)

	return Response{
		size:          0,
		correlationId: request.correlationId,
		errorCode:     errorCode,
	}
}

func (res *Response) Marshal() []byte {
	r := make([]byte, 10)
	binary.BigEndian.PutUint32(r[0:4], uint32(res.size))
	binary.BigEndian.PutUint32(r[4:8], uint32(res.correlationId))
	binary.BigEndian.PutUint16(r[8:10], uint16(res.errorCode))
	return r
}
