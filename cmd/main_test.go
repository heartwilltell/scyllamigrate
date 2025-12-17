package main

import (
	"testing"

	td "github.com/maxatome/go-testdeep/td"
)

func TestParseNetworkTopology(t *testing.T) {
	type tcase struct {
		input       string
		expected    map[string]int
		expectError bool
		errorMsg    string
	}

	tests := map[string]tcase{
		"single datacenter": {
			input:    "dc1:3",
			expected: map[string]int{"dc1": 3},
		},
		"two datacenters": {
			input:    "dc1:3,dc2:2",
			expected: map[string]int{"dc1": 3, "dc2": 2},
		},
		"three datacenters": {
			input:    "us-east:3,us-west:2,eu-west:2",
			expected: map[string]int{"us-east": 3, "us-west": 2, "eu-west": 2},
		},
		"with spaces": {
			input:    "dc1: 3, dc2 :2 , dc3 : 1",
			expected: map[string]int{"dc1": 3, "dc2": 2, "dc3": 1},
		},
		"trailing comma": {
			input:    "dc1:3,dc2:2,",
			expected: map[string]int{"dc1": 3, "dc2": 2},
		},
		"leading comma ignored": {
			input:    ",dc1:3",
			expected: map[string]int{"dc1": 3},
		},
		"empty string": {
			input:       "",
			expectError: true,
			errorMsg:    "network topology must specify at least one datacenter",
		},
		"only spaces": {
			input:       "   ",
			expectError: true,
			errorMsg:    "network topology must specify at least one datacenter",
		},
		"missing colon": {
			input:       "dc1",
			expectError: true,
			errorMsg:    "invalid network topology format",
		},
		"missing replication factor": {
			input:       "dc1:",
			expectError: true,
			errorMsg:    "invalid replication factor",
		},
		"invalid replication factor - not a number": {
			input:       "dc1:abc",
			expectError: true,
			errorMsg:    "invalid replication factor",
		},
		"zero replication factor": {
			input:       "dc1:0",
			expectError: true,
			errorMsg:    "replication factor must be at least 1",
		},
		"negative replication factor": {
			input:       "dc1:-1",
			expectError: true,
			errorMsg:    "replication factor must be at least 1",
		},
		"empty datacenter name": {
			input:       ":3",
			expectError: true,
			errorMsg:    "empty datacenter name",
		},
		"one valid one invalid": {
			input:       "dc1:3,dc2",
			expectError: true,
			errorMsg:    "invalid network topology format",
		},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			result, err := parseNetworkTopology(tc.input)

			if tc.expectError {
				td.CmpError(t, err)
				if tc.errorMsg != "" {
					td.Cmp(t, err.Error(), td.Contains(tc.errorMsg))
				}
				return
			}

			td.CmpNoError(t, err)
			td.Cmp(t, result, tc.expected)
		})
	}
}

func TestParseConsistency(t *testing.T) {
	type tcase struct {
		input    string
		expected string
	}

	tests := map[string]tcase{
		"any lowercase":          {input: "any", expected: "ANY"},
		"any uppercase":          {input: "ANY", expected: "ANY"},
		"one lowercase":          {input: "one", expected: "ONE"},
		"one uppercase":          {input: "ONE", expected: "ONE"},
		"two":                    {input: "two", expected: "TWO"},
		"three":                  {input: "three", expected: "THREE"},
		"quorum lowercase":       {input: "quorum", expected: "QUORUM"},
		"quorum uppercase":       {input: "QUORUM", expected: "QUORUM"},
		"all":                    {input: "all", expected: "ALL"},
		"local_quorum":           {input: "local_quorum", expected: "LOCAL_QUORUM"},
		"localquorum":            {input: "localquorum", expected: "LOCAL_QUORUM"},
		"LOCALQUORUM":            {input: "LOCALQUORUM", expected: "LOCAL_QUORUM"},
		"each_quorum":            {input: "each_quorum", expected: "EACH_QUORUM"},
		"eachquorum":             {input: "eachquorum", expected: "EACH_QUORUM"},
		"local_one":              {input: "local_one", expected: "LOCAL_ONE"},
		"localone":               {input: "localone", expected: "LOCAL_ONE"},
		"unknown defaults":       {input: "unknown", expected: "QUORUM"},
		"empty string defaults":  {input: "", expected: "QUORUM"},
	}

	for name, tc := range tests {
		t.Run(name, func(t *testing.T) {
			result := parseConsistency(tc.input)
			td.Cmp(t, result.String(), tc.expected)
		})
	}
}
