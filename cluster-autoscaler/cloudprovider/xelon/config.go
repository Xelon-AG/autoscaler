/*
Copyright 2026 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package xelon

import (
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"strings"
)

// Config contains only XKS connection settings. Node group bounds and the
// worker pool identifier are supplied through CA's --nodes flag.
type Config struct {
	BaseURL   string `json:"base_url"`
	Token     string `json:"token"`
	ClientID  string `json:"client_id"`
	ClusterID string `json:"cluster_id"`
}

func readConfig(reader io.Reader) (*Config, error) {
	if reader == nil {
		return nil, fmt.Errorf("Xelon cloud config is required")
	}

	decoder := json.NewDecoder(reader)
	decoder.DisallowUnknownFields()
	config := new(Config)
	if err := decoder.Decode(config); err != nil {
		return nil, fmt.Errorf("decode Xelon cloud config: %w", err)
	}
	if err := ensureJSONEOF(decoder); err != nil {
		return nil, err
	}
	if err := config.validate(); err != nil {
		return nil, err
	}
	return config, nil
}

func ensureJSONEOF(decoder *json.Decoder) error {
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("Xelon cloud config contains multiple JSON values")
		}
		return fmt.Errorf("decode trailing Xelon cloud config data: %w", err)
	}
	return nil
}

func (config *Config) validate() error {
	if strings.TrimSpace(config.Token) == "" {
		return fmt.Errorf("Xelon token is required")
	}
	if strings.TrimSpace(config.ClientID) == "" {
		return fmt.Errorf("Xelon client ID is required")
	}
	if strings.TrimSpace(config.ClusterID) == "" {
		return fmt.Errorf("Xelon Kubernetes cluster ID is required")
	}
	if config.BaseURL == "" {
		return nil
	}
	parsed, err := url.Parse(config.BaseURL)
	if err != nil {
		return fmt.Errorf("parse Xelon base URL: %w", err)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("Xelon base URL must use http or https")
	}
	if parsed.Host == "" || parsed.RawQuery != "" || parsed.Fragment != "" {
		return fmt.Errorf("Xelon base URL must contain a host and no query or fragment")
	}
	if !strings.HasSuffix(parsed.Path, "/") {
		return fmt.Errorf("Xelon base URL must end with a slash")
	}
	return nil
}
