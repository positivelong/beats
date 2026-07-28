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
	"strings"
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/xdg/scram"
)

func TestNormalizeSASLMechanism(t *testing.T) {
	tests := []struct {
		name      string
		mechanism string
		expected  sarama.SASLMechanism
		wantErr   bool
	}{
		{name: "empty means sarama default", mechanism: "", expected: ""},
		{name: "plain", mechanism: "PLAIN", expected: sarama.SASLTypePlaintext},
		{name: "plain lowercase", mechanism: "plain", expected: sarama.SASLTypePlaintext},
		{name: "scram sha 256", mechanism: "SCRAM-SHA-256", expected: sarama.SASLTypeSCRAMSHA256},
		{name: "scram sha 512", mechanism: "SCRAM-SHA-512", expected: sarama.SASLTypeSCRAMSHA512},
		{name: "scram sha 512 mixed case", mechanism: "scram-sha-512", expected: sarama.SASLTypeSCRAMSHA512},
		{name: "unsupported gssapi", mechanism: "GSSAPI", wantErr: true},
		{name: "unsupported oauthbearer", mechanism: "OAUTHBEARER", wantErr: true},
		{name: "unsupported garbage", mechanism: "foo", wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			actual, err := NormalizeSASLMechanism(test.mechanism)
			if test.wantErr {
				assert.Error(t, err)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, test.expected, actual)
		})
	}
}

func TestConfigureSASL(t *testing.T) {
	t.Run("no username keeps sasl disabled", func(t *testing.T) {
		k := sarama.NewConfig()
		require.NoError(t, ConfigureSASL(k, "", "", "SCRAM-SHA-512"))
		assert.False(t, k.Net.SASL.Enable)
	})

	t.Run("empty mechanism keeps sarama plain default", func(t *testing.T) {
		k := sarama.NewConfig()
		require.NoError(t, ConfigureSASL(k, "user", "pass", ""))
		assert.True(t, k.Net.SASL.Enable)
		assert.Equal(t, "user", k.Net.SASL.User)
		assert.Equal(t, "pass", k.Net.SASL.Password)
		assert.Equal(t, sarama.SASLMechanism(""), k.Net.SASL.Mechanism)
		assert.Nil(t, k.Net.SASL.SCRAMClientGeneratorFunc)
		require.NoError(t, k.Validate())
	})

	t.Run("plain mechanism explicit", func(t *testing.T) {
		k := sarama.NewConfig()
		require.NoError(t, ConfigureSASL(k, "user", "pass", "PLAIN"))
		require.NoError(t, k.Validate())
		assert.Nil(t, k.Net.SASL.SCRAMClientGeneratorFunc)
	})

	t.Run("scram sha 512 installs generator", func(t *testing.T) {
		k := sarama.NewConfig()
		require.NoError(t, ConfigureSASL(k, "user", "pass", "SCRAM-SHA-512"))
		require.NoError(t, k.Validate())
		assert.Equal(t, sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA512), k.Net.SASL.Mechanism)
		require.NotNil(t, k.Net.SASL.SCRAMClientGeneratorFunc)
		require.NotNil(t, k.Net.SASL.SCRAMClientGeneratorFunc())
	})

	t.Run("scram sha 256 installs generator", func(t *testing.T) {
		k := sarama.NewConfig()
		require.NoError(t, ConfigureSASL(k, "user", "pass", "SCRAM-SHA-256"))
		require.NoError(t, k.Validate())
		assert.Equal(t, sarama.SASLMechanism(sarama.SASLTypeSCRAMSHA256), k.Net.SASL.Mechanism)
		require.NotNil(t, k.Net.SASL.SCRAMClientGeneratorFunc)
	})

	t.Run("unsupported mechanism fails", func(t *testing.T) {
		k := sarama.NewConfig()
		assert.Error(t, ConfigureSASL(k, "user", "pass", "GSSAPI"))
	})
}

func TestXDGSCRAMClientConversation(t *testing.T) {
	client := &XDGSCRAMClient{HashGeneratorFcn: SHA512}
	require.NoError(t, client.Begin("user", "pencil", ""))

	// client-first-message, e.g. "n,,n=user,r=abc"
	clientFirst, err := client.Step("")
	require.NoError(t, err)
	require.True(t, strings.HasPrefix(clientFirst, "n,,n=user,r="))
	assert.False(t, client.Done())

	clientNonce := strings.TrimPrefix(clientFirst, "n,,n=user,r=")

	// server-first-message with the client nonce echoed back plus server nonce
	serverFirst := "r=" + clientNonce + "%hvYDpWUa2RaTCAfuxFIlj)hNlF$k0,s=W22ZaJ0SNY7soEsUEjb6gQ==,i=4096"
	clientFinal, err := client.Step(serverFirst)
	require.NoError(t, err)
	assert.True(t, strings.HasPrefix(clientFinal, "c=biws,r="+clientNonce))
	assert.Contains(t, clientFinal, ",p=")
	assert.False(t, client.Done())

	// server-final-message: a wrong server signature must be rejected
	_, err = client.Step("v=6rriTRBi23WpRR/wtup+mMhUZUn/dB5nLTJRsjl95G4=")
	assert.Error(t, err)
}

// newTestSCRAMServer builds a real xdg-go/scram server with stored credentials
// derived from the given password.
func newTestSCRAMServer(t *testing.T, password string) *scram.ServerConversation {
	credClient, err := SHA512.NewClient("", password, "")
	require.NoError(t, err)
	storedCreds := credClient.GetStoredCredentials(scram.KeyFactors{
		Salt:  "W22ZaJ0SNY7soEsUEjb6gQ==",
		Iters: 4096,
	})

	server, err := SHA512.NewServer(func(username string) (scram.StoredCredentials, error) {
		return storedCreds, nil
	})
	require.NoError(t, err)
	return server.NewConversation()
}

func TestXDGSCRAMClientAgainstRealServer(t *testing.T) {
	// Drive a full SCRAM-SHA-512 exchange between the sarama-facing client and
	// a real xdg-go/scram server, verifying both sides complete successfully.
	serverConv := newTestSCRAMServer(t, "pencil")

	client := &XDGSCRAMClient{HashGeneratorFcn: SHA512}
	require.NoError(t, client.Begin("user", "pencil", ""))

	clientFirst, err := client.Step("")
	require.NoError(t, err)

	serverFirst, err := serverConv.Step(clientFirst)
	require.NoError(t, err)

	clientFinal, err := client.Step(serverFirst)
	require.NoError(t, err)

	serverFinal, err := serverConv.Step(clientFinal)
	require.NoError(t, err)
	assert.True(t, serverConv.Done())

	_, err = client.Step(serverFinal)
	require.NoError(t, err)
	assert.True(t, client.Done())
}

func TestXDGSCRAMClientWrongPassword(t *testing.T) {
	serverConv := newTestSCRAMServer(t, "pencil")

	client := &XDGSCRAMClient{HashGeneratorFcn: SHA512}
	require.NoError(t, client.Begin("user", "wrong-password", ""))

	clientFirst, err := client.Step("")
	require.NoError(t, err)

	serverFirst, err := serverConv.Step(clientFirst)
	require.NoError(t, err)

	clientFinal, err := client.Step(serverFirst)
	require.NoError(t, err)

	// the server must reject the proof derived from the wrong password
	_, err = serverConv.Step(clientFinal)
	assert.Error(t, err)
}
