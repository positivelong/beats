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
	"crypto/sha256"
	"crypto/sha512"
	"fmt"
	"strings"

	"github.com/Shopify/sarama"
	"github.com/xdg/scram"
)

var (
	// SHA256 is the hash generator for the SCRAM-SHA-256 mechanism.
	SHA256 scram.HashGeneratorFcn = sha256.New
	// SHA512 is the hash generator for the SCRAM-SHA-512 mechanism.
	SHA512 scram.HashGeneratorFcn = sha512.New
)

// XDGSCRAMClient implements sarama.SCRAMClient on top of xdg-go/scram,
// adapted from the official sarama SASL/SCRAM example.
type XDGSCRAMClient struct {
	*scram.Client
	*scram.ClientConversation
	scram.HashGeneratorFcn
}

// Begin prepares the SCRAM exchange with the given credentials.
func (x *XDGSCRAMClient) Begin(userName, password, authzID string) (err error) {
	x.Client, err = x.HashGeneratorFcn.NewClient(userName, password, authzID)
	if err != nil {
		return err
	}
	x.ClientConversation = x.Client.NewConversation()
	return nil
}

// Step advances the SCRAM exchange with a server challenge.
func (x *XDGSCRAMClient) Step(challenge string) (response string, err error) {
	response, err = x.ClientConversation.Step(challenge)
	return
}

// Done reports whether the SCRAM conversation has completed.
func (x *XDGSCRAMClient) Done() bool {
	return x.ClientConversation.Done()
}

// saslMechanisms maps the canonical SASL mechanism names to sarama constants.
var saslMechanisms = map[string]sarama.SASLMechanism{
	"PLAIN":         sarama.SASLTypePlaintext,
	"SCRAM-SHA-256": sarama.SASLTypeSCRAMSHA256,
	"SCRAM-SHA-512": sarama.SASLTypeSCRAMSHA512,
}

// NormalizeSASLMechanism validates the mechanism name (case-insensitive) and
// returns the canonical sarama value. An empty input means the sarama default
// (PLAIN) and is returned as-is.
func NormalizeSASLMechanism(mechanism string) (sarama.SASLMechanism, error) {
	if mechanism == "" {
		return "", nil
	}
	m, ok := saslMechanisms[strings.ToUpper(mechanism)]
	if !ok {
		return "", fmt.Errorf("unsupported sasl mechanism %q, available mechanisms are: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512", mechanism)
	}
	return m, nil
}

// ConfigureSASL applies username/password SASL authentication with the given
// mechanism to the sarama config. When username is empty, SASL stays disabled
// and the config is left untouched. PLAIN (or empty mechanism) keeps the
// sarama defaults; SCRAM mechanisms additionally install the matching
// SCRAMClientGeneratorFunc.
func ConfigureSASL(k *sarama.Config, username, password, mechanism string) error {
	if username == "" {
		return nil
	}

	m, err := NormalizeSASLMechanism(mechanism)
	if err != nil {
		return err
	}

	k.Net.SASL.Enable = true
	k.Net.SASL.User = username
	k.Net.SASL.Password = password

	switch m {
	case sarama.SASLTypeSCRAMSHA256:
		k.Net.SASL.Mechanism = m
		k.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
			return &XDGSCRAMClient{HashGeneratorFcn: SHA256}
		}
	case sarama.SASLTypeSCRAMSHA512:
		k.Net.SASL.Mechanism = m
		k.Net.SASL.SCRAMClientGeneratorFunc = func() sarama.SCRAMClient {
			return &XDGSCRAMClient{HashGeneratorFcn: SHA512}
		}
	}
	return nil
}
