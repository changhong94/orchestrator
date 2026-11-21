package config

import (
	"encoding/json"
	"gopkg.in/yaml.v3"
	"testing"
)

func TestPrepareConcurrencyDecode(t *testing.T) {
	for _, raw := range []string{`{"unit":"runner@.service","size":64,"prepare_concurrency":8}`, `{"unit":"runner@.service","size":64,"prepare_concurrency":0}`} {
		var c RunPoolConfig
		if err := json.Unmarshal([]byte(raw), &c); err != nil {
			t.Fatal(err)
		}
	}
	for _, decode := range []func([]byte, any) error{json.Unmarshal, yaml.Unmarshal} {
		var c RunPoolConfig
		if err := decode([]byte(`{"unit":"runner@.service","size":64,"prepare_concurrency":-1}`), &c); err == nil {
			t.Fatal("negative preparation limit accepted")
		}
	}
	var c RunPoolConfig
	if err := yaml.Unmarshal([]byte("unit: runner@.service\nsize: 64\nprepare_concurrency: 8\n"), &c); err != nil || c.PrepareConcurrency != 8 {
		t.Fatalf("config=%+v err=%v", c, err)
	}
}
