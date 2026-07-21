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
	"testing"

	"github.com/Shopify/sarama"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/elastic/beats/libbeat/common"
)

func TestSASLMechanismConfig(t *testing.T) {
	base := common.MapStr{
		"hosts":    []string{"localhost:9092"},
		"topics":   []string{"test"},
		"group_id": "test-group",
	}
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
		"explicit plain": {
			config: common.MapStr{
				"username":       "user",
				"password":       "pass",
				"sasl_mechanism": "PLAIN",
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
		t.Run(name, func(t *testing.T) {
			cfgMap := common.MapStr{}
			for k, v := range base {
				cfgMap[k] = v
			}
			for k, v := range test.config {
				cfgMap[k] = v
			}

			config := defaultConfig()
			err := common.MustNewConfigFrom(cfgMap).Unpack(&config)
			if test.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			k, err := newSaramaConfig(config)
			require.NoError(t, err)
			assert.Equal(t, test.mechanism, k.Net.SASL.Mechanism)
			if test.scram {
				assert.NotNil(t, k.Net.SASL.SCRAMClientGeneratorFunc)
			} else {
				assert.Nil(t, k.Net.SASL.SCRAMClientGeneratorFunc)
			}
		})
	}
}
