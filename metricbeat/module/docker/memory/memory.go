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

package memory

import (
	"github.com/docker/docker/client"
	"github.com/pkg/errors"

	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/metricbeat/mb"
	"github.com/elastic/beats/metricbeat/module/docker"
)

var logger = logp.NewLogger("docker.memory")

func init() {
	mb.Registry.MustAddMetricSet("docker", "memory", New,
		mb.WithHostParser(docker.HostParser),
		mb.DefaultMetricSet(),
	)
}

// MetricSet type defines all fields of the MetricSet
type MetricSet struct {
	mb.BaseMetricSet
	memoryService *MemoryService
	dockerClient  *client.Client
	dedot         bool
}

// New creates a new instance of the docker memory MetricSet.
func New(base mb.BaseMetricSet) (mb.MetricSet, error) {
	config := docker.DefaultConfig()
	if err := base.Module().UnpackConfig(&config); err != nil {
		return nil, err
	}

	client, err := docker.NewDockerClient(base.HostData().URI, config)
	if err != nil {
		return nil, err
	}

	return &MetricSet{
		BaseMetricSet: base,
		memoryService: &MemoryService{},
		dockerClient:  client,
		dedot:         config.DeDot,
	}, nil
}

// Fetch creates a list of memory events for each container.
func (m *MetricSet) Fetch(r mb.ReporterV2) {
	stats, err := docker.FetchStats(m.dockerClient, m.Module().Config().Timeout)
	if err != nil {
		err = errors.Wrap(err, "failed to get docker stats")
		logger.Error(err)
		r.Error(err)
		return
	}

	memoryStats := m.memoryService.getMemoryStatsList(stats, m.dedot)
	eventsMapping(r, memoryStats)
}

//Close stops the metricset
func (m *MetricSet) Close() error {

	return m.dockerClient.Close()
}
