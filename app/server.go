package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
)

const (
	UNSUPPORTED_VERSION int16 = 35
)

const (
	API_VERSIONS int32 = 18
)

type ApiVersionsResponse struct {
	correlationId int32

	errorCode int16

	apiKeysLen uint8
	apiKeys    []ApiKeys

	throttleTimeMs int32
}

// Response struct
type Request struct {
	size          int32
	apiKey        int16
	apiVersion    int16
	correlationId int32
}

type ApiKeys struct {
	apiKey     int16
	minVersion int16
	maxVersion int16
}

type Server struct {
	net.Listener
}

func main() {
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}

	fmt.Println("Listening on port 9092")
	s := &Server{Listener: l}
	s.Serve()
}

func (s *Server) Serve() {
	for {
		c, err := s.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		go s.Handle(c)
	}
}

func (s *Server) Handle(conn net.Conn) {
	defer conn.Close()
	for {
		reqBuf := make([]byte, 1024)
		n, err := conn.Read(reqBuf)
		if err != nil {
			if err != io.EOF {
				fmt.Println("Error reading from the connection")
				os.Exit(1)
			}
		}
		fmt.Printf("Received %d bytes \n", n)

		req := decodeRequest(reqBuf)

		res := responseService(req)

		Send(conn, res.Marshal())
	}
}

func Send(c net.Conn, resp []byte) {
	binary.Write(c, binary.BigEndian, int32(len(resp)))
	binary.Write(c, binary.BigEndian, resp)
}

// mesSize  apiKey  apiVersion  corrId
// [0000]   [00]    [00]        [0000]  [...]

func decodeRequest(request []byte) Request {
	req := Request{
		size:          int32(binary.BigEndian.Uint32(request[0:4])),
		apiKey:        int16(binary.BigEndian.Uint16(request[4:6])),
		apiVersion:    int16(binary.BigEndian.Uint16(request[6:8])),
		correlationId: int32(binary.BigEndian.Uint32(request[8:12])),
	}
	return req
}

func responseService(request Request) ApiVersionsResponse {
	var errorCode int16 = 0

	if request.apiVersion < 0 || request.apiVersion > 4 {
		errorCode = UNSUPPORTED_VERSION
	}

	return ApiVersionsResponse{
		correlationId: request.correlationId,
		errorCode:     errorCode,

		apiKeysLen: 2, // Kafka Compact length array, where 0 is null array, 1 is empty array
		apiKeys: []ApiKeys{
			{
				apiKey:     18,
				minVersion: 3,
				maxVersion: 4,
			},
		},
		throttleTimeMs: 0,
	}
}

func (res *ApiVersionsResponse) Marshal() []byte {
	r := make([]byte, 19)
	binary.BigEndian.PutUint32(r, uint32(res.correlationId))
	binary.BigEndian.PutUint16(r[4:6], uint16(res.errorCode))
	r[6] = res.apiKeysLen

	binary.BigEndian.PutUint16(r[7:9], uint16(res.apiKeys[0].apiKey))
	binary.BigEndian.PutUint16(r[9:11], uint16(res.apiKeys[0].minVersion))
	binary.BigEndian.PutUint16(r[11:13], uint16(res.apiKeys[0].maxVersion))

	r[13] = 0 // TAG_BUFFER
	binary.BigEndian.PutUint32(r[14:18], uint32(res.throttleTimeMs))
	r[18] = 0 // TAG_BUFFER

	return r
}
