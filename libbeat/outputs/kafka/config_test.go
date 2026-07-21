// Licensed to Elasticsearch B.V. under one or more contributor
// license agreements. See the NOTICE file distributed with
// this work for additional information regarding copyright
// ownership. Elasticsearch B.V. licenses this file to you under
// the Apache License, Version 2.0 (the "License"); you may
// not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package kafka

import (
	"testing"

	"github.com/Shopify/sarama"

	"github.com/elastic/beats/libbeat/common"
)

func TestConfigAcceptValid(t *testing.T) {
	tests := map[string]common.MapStr{
		"default config is valid": common.MapStr{},
		"lz4 with 0.11": common.MapStr{
			"compression": "lz4",
			"version":     "0.11",
		},
		"lz4 with 1.0": common.MapStr{
			"compression": "lz4",
			"version":     "1.0.0",
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			c := common.MustNewConfigFrom(test)
			c.SetString("hosts", 0, "localhost")
			cfg, err := readConfig(c)
			if err != nil {
				t.Fatalf("Can not create test configuration: %v", err)
			}
			if _, err := newSaramaConfig(cfg); err != nil {
				t.Fatalf("Failure creating sarama config: %v", err)
			}
		})
	}
}

func TestSASLMechanismConfig(t *testing.T) {
	tests := map[string]struct {
		config    common.MapStr
		mechanism sarama.SASLMechanism
		scram     bool
		wantErr   bool
	}{
		"no sasl config": {
			config:    common.MapStr{},
			mechanism: "",
		},
		"plain by default when username set": {
			config: common.MapStr{
				"username": "user",
				"password": "pass",
			},
			mechanism: sarama.SASLTypePlaintext,
		},
		"scram sha 512": {
			config: common.MapStr{
				"username":       "user",
				"password":       "pass",
				"sasl_mechanism": "SCRAM-SHA-512",
			},
			mechanism: sarama.SASLTypeSCRAMSHA512,
			scram:     true,
		},
		"scram sha 256 lowercase": {
			config: common.MapStr{
				"username":       "user",
				"password":       "pass",
				"sasl_mechanism": "scram-sha-256",
			},
			mechanism: sarama.SASLTypeSCRAMSHA256,
			scram:     true,
		},
		"unsupported mechanism": {
			config: common.MapStr{
				"username":       "user",
				"password":       "pass",
				"sasl_mechanism": "GSSAPI",
			},
			wantErr: true,
		},
	}

	for name, test := range tests {
		test := test
		t.Run(name, func(t *testing.T) {
			c := common.MustNewConfigFrom(test.config)
			c.SetString("hosts", 0, "localhost")
			cfg, err := readConfig(c)
			if test.wantErr {
				if err == nil {
					_, err = newSaramaConfig(cfg)
				}
				if err == nil {
					t.Fatal("expected error for unsupported sasl mechanism, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("Can not create test configuration: %v", err)
			}

			k, err := newSaramaConfig(cfg)
			if err != nil {
				t.Fatalf("Failure creating sarama config: %v", err)
			}
			if k.Net.SASL.Mechanism != test.mechanism {
				t.Fatalf("unexpected sasl mechanism: got %q, want %q", k.Net.SASL.Mechanism, test.mechanism)
			}
			if scram := k.Net.SASL.SCRAMClientGeneratorFunc != nil; scram != test.scram {
				t.Fatalf("unexpected scram generator presence: got %v, want %v", scram, test.scram)
			}
		})
	}
}
