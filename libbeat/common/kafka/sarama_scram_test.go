// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

package kafka

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type wireSCRAMClient struct {
	done      bool
	stepError bool
}

func (c *wireSCRAMClient) Begin(_, _, _ string) error { return nil }

func (c *wireSCRAMClient) Step(challenge string) (string, error) {
	if c.stepError && challenge != "" {
		return "", errors.New("client step failed")
	}
	switch challenge {
	case "":
		return "client-first", nil
	case "server-final":
		c.done = true
		return "", nil
	default:
		return "", fmt.Errorf("unexpected challenge %q", challenge)
	}
}

func (c *wireSCRAMClient) Done() bool { return c.done }

type wireBrokerScenario struct {
	mechanism             sarama.SASLMechanism
	handshakeError        int16
	authError             int16
	stepError             bool
	correlationIDMismatch bool
}

func TestSCRAMBrokerWirePath(t *testing.T) {
	tests := []struct {
		name     string
		scenario wireBrokerScenario
		wantErr  bool
	}{
		{name: "SCRAM-SHA-256 success", scenario: wireBrokerScenario{mechanism: sarama.SASLTypeSCRAMSHA256}},
		{name: "SCRAM-SHA-512 success", scenario: wireBrokerScenario{mechanism: sarama.SASLTypeSCRAMSHA512}},
		{name: "handshake failure", scenario: wireBrokerScenario{mechanism: sarama.SASLTypeSCRAMSHA512, handshakeError: 33}, wantErr: true},
		{name: "authentication failure", scenario: wireBrokerScenario{mechanism: sarama.SASLTypeSCRAMSHA512, authError: 58}, wantErr: true},
		{name: "client step failure", scenario: wireBrokerScenario{mechanism: sarama.SASLTypeSCRAMSHA512, stepError: true}, wantErr: true},
		{name: "correlation ID mismatch", scenario: wireBrokerScenario{mechanism: sarama.SASLTypeSCRAMSHA512, correlationIDMismatch: true}, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			address, brokerErr, closeBroker := startWireBroker(t, test.scenario)
			defer closeBroker()

			config := sarama.NewConfig()
			config.Version = sarama.V1_0_0_0
			config.Net.DialTimeout = time.Second
			config.Net.ReadTimeout = time.Second
			config.Net.WriteTimeout = time.Second
			config.Net.SASL.Enable = true
			config.Net.SASL.User = "user"
			config.Net.SASL.Password = "password"
			config.Net.SASL.Mechanism = test.scenario.mechanism
			config.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
				return &wireSCRAMClient{stepError: test.scenario.stepError}
			}

			broker := sarama.NewBroker(address)
			require.NoError(t, broker.Open(config))
			connected, err := broker.Connected()
			if test.wantErr {
				assert.Error(t, err)
				assert.False(t, connected)
			} else {
				require.NoError(t, err)
				assert.True(t, connected)
				require.NoError(t, broker.Close())
			}
			assert.NoError(t, <-brokerErr)
		})
	}
}

func startWireBroker(t *testing.T, scenario wireBrokerScenario) (string, <-chan error, func()) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	errCh := make(chan error, 1)
	var once sync.Once
	closeFn := func() { once.Do(func() { _ = listener.Close() }) }

	go func() {
		conn, err := listener.Accept()
		if err != nil {
			errCh <- nil
			return
		}
		defer conn.Close()

		apiKey, version, correlationID, mechanism, err := readHandshakeRequest(conn)
		if err != nil {
			errCh <- err
			return
		}
		if apiKey != 17 || version != 1 || mechanism != string(scenario.mechanism) {
			errCh <- fmt.Errorf("unexpected handshake: api=%d version=%d mechanism=%q", apiKey, version, mechanism)
			return
		}
		if err := writeHandshakeResponse(conn, correlationID, scenario.handshakeError); err != nil {
			errCh <- err
			return
		}
		if scenario.handshakeError != 0 {
			errCh <- nil
			return
		}

		apiKey, version, correlationID, authBytes, err := readAuthenticateRequest(conn)
		if err != nil {
			errCh <- err
			return
		}
		if apiKey != 36 || version != 0 || string(authBytes) != "client-first" {
			errCh <- fmt.Errorf("unexpected authenticate request: api=%d version=%d auth=%q", apiKey, version, authBytes)
			return
		}
		if scenario.correlationIDMismatch {
			correlationID++
		}
		challenge := []byte("server-final")
		if scenario.authError != 0 {
			challenge = nil
		}
		if err := writeAuthenticateResponse(conn, correlationID, scenario.authError, challenge); err != nil {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	return listener.Addr().String(), errCh, closeFn
}

func readHandshakeRequest(conn net.Conn) (int16, int16, int32, string, error) {
	apiKey, version, correlationID, body, err := readKafkaRequest(conn)
	if err != nil {
		return 0, 0, 0, "", err
	}
	mechanism, _, err := readKafkaString(body)
	return apiKey, version, correlationID, mechanism, err
}

func readAuthenticateRequest(conn net.Conn) (int16, int16, int32, []byte, error) {
	apiKey, version, correlationID, body, err := readKafkaRequest(conn)
	if err != nil {
		return 0, 0, 0, nil, err
	}
	if len(body) < 4 {
		return 0, 0, 0, nil, io.ErrUnexpectedEOF
	}
	length := int(binary.BigEndian.Uint32(body[:4]))
	if length < 0 || len(body) < 4+length {
		return 0, 0, 0, nil, io.ErrUnexpectedEOF
	}
	return apiKey, version, correlationID, body[4 : 4+length], nil
}

func readKafkaRequest(conn net.Conn) (int16, int16, int32, []byte, error) {
	packet, err := readKafkaPacket(conn)
	if err != nil {
		return 0, 0, 0, nil, err
	}
	if len(packet) < 10 {
		return 0, 0, 0, nil, io.ErrUnexpectedEOF
	}
	apiKey := int16(binary.BigEndian.Uint16(packet[0:2]))
	version := int16(binary.BigEndian.Uint16(packet[2:4]))
	correlationID := int32(binary.BigEndian.Uint32(packet[4:8]))
	clientID, rest, err := readKafkaString(packet[8:])
	if err != nil {
		return 0, 0, 0, nil, err
	}
	if clientID == "" {
		return 0, 0, 0, nil, errors.New("missing client ID")
	}
	return apiKey, version, correlationID, rest, nil
}

func readKafkaPacket(conn net.Conn) ([]byte, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(conn, header); err != nil {
		return nil, err
	}
	length := int(binary.BigEndian.Uint32(header))
	packet := make([]byte, length)
	_, err := io.ReadFull(conn, packet)
	return packet, err
}

func readKafkaString(data []byte) (string, []byte, error) {
	if len(data) < 2 {
		return "", nil, io.ErrUnexpectedEOF
	}
	length := int(binary.BigEndian.Uint16(data[:2]))
	if len(data) < 2+length {
		return "", nil, io.ErrUnexpectedEOF
	}
	return string(data[2 : 2+length]), data[2+length:], nil
}

func writeHandshakeResponse(conn net.Conn, correlationID int32, errorCode int16) error {
	mechanism256 := string(sarama.SASLTypeSCRAMSHA256)
	mechanism512 := string(sarama.SASLTypeSCRAMSHA512)
	body := make([]byte, 2+4+2+len(mechanism256)+2+len(mechanism512))
	binary.BigEndian.PutUint16(body[0:2], uint16(errorCode))
	binary.BigEndian.PutUint32(body[2:6], 2)
	offset := 6
	offset = putKafkaString(body, offset, mechanism256)
	putKafkaString(body, offset, mechanism512)
	return writeKafkaResponse(conn, correlationID, body)
}

func writeAuthenticateResponse(conn net.Conn, correlationID int32, errorCode int16, authBytes []byte) error {
	errorMessage := []byte(nil)
	if errorCode != 0 {
		errorMessage = []byte("authentication failed")
	}
	body := make([]byte, 2+2+len(errorMessage)+4+len(authBytes))
	binary.BigEndian.PutUint16(body[0:2], uint16(errorCode))
	offset := 2
	if errorMessage == nil {
		binary.BigEndian.PutUint16(body[offset:offset+2], uint16(0xffff))
		offset += 2
	} else {
		offset = putKafkaString(body, offset, string(errorMessage))
	}
	binary.BigEndian.PutUint32(body[offset:offset+4], uint32(len(authBytes)))
	copy(body[offset+4:], authBytes)
	return writeKafkaResponse(conn, correlationID, body)
}

func writeKafkaResponse(conn net.Conn, correlationID int32, body []byte) error {
	packet := make([]byte, 8+len(body))
	binary.BigEndian.PutUint32(packet[0:4], uint32(4+len(body)))
	binary.BigEndian.PutUint32(packet[4:8], uint32(correlationID))
	copy(packet[8:], body)
	_, err := conn.Write(packet)
	return err
}

func putKafkaString(target []byte, offset int, value string) int {
	binary.BigEndian.PutUint16(target[offset:offset+2], uint16(len(value)))
	copy(target[offset+2:], value)
	return offset + 2 + len(value)
}
