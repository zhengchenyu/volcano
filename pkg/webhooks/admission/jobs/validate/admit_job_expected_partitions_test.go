/*
Copyright 2025 The Volcano Authors.

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

package validate

import (
	"testing"

	v1alpha1 "volcano.sh/apis/pkg/apis/batch/v1alpha1"
)

func TestValidateExpectedPartitions(t *testing.T) {
	tests := []struct {
		name          string
		taskSpec      v1alpha1.TaskSpec
		expectError   bool
		errorContains string
	}{
		{
			name: "Valid expected partitions",
			taskSpec: v1alpha1.TaskSpec{
				Name:     "test-task",
				Replicas: 8,
				PartitionPolicy: &v1alpha1.PartitionPolicySpec{
					TotalPartitions:    4,
					PartitionSize:      2,
					MinPartitions:      1,
					ExpectedPartitions: []int32{1, 2, 4},
				},
			},
			expectError: false,
		},
		{
			name: "First element not equal to MinPartitions",
			taskSpec: v1alpha1.TaskSpec{
				Name:     "test-task",
				Replicas: 8,
				PartitionPolicy: &v1alpha1.PartitionPolicySpec{
					TotalPartitions:    4,
					PartitionSize:      2,
					MinPartitions:      1,
					ExpectedPartitions: []int32{2, 4}, // Should start with 1
				},
			},
			expectError:   true,
			errorContains: "ExpectedPartitions[0]",
		},
		{
			name: "Last element not equal to TotalPartitions",
			taskSpec: v1alpha1.TaskSpec{
				Name:     "test-task",
				Replicas: 8,
				PartitionPolicy: &v1alpha1.PartitionPolicySpec{
					TotalPartitions:    4,
					PartitionSize:      2,
					MinPartitions:      1,
					ExpectedPartitions: []int32{1, 2, 3}, // Should end with 4
				},
			},
			expectError:   true,
			errorContains: "ExpectedPartitions[last]",
		},
		{
			name: "Not monotonically increasing",
			taskSpec: v1alpha1.TaskSpec{
				Name:     "test-task",
				Replicas: 8,
				PartitionPolicy: &v1alpha1.PartitionPolicySpec{
					TotalPartitions:    4,
					PartitionSize:      2,
					MinPartitions:      1,
					ExpectedPartitions: []int32{1, 3, 2, 4}, // Not monotonic
				},
			},
			expectError:   true,
			errorContains: "monotonically increasing",
		},
		{
			name: "Valid with all partitions",
			taskSpec: v1alpha1.TaskSpec{
				Name:     "test-task",
				Replicas: 8,
				PartitionPolicy: &v1alpha1.PartitionPolicySpec{
					TotalPartitions:    4,
					PartitionSize:      2,
					MinPartitions:      1,
					ExpectedPartitions: []int32{1, 2, 3, 4},
				},
			},
			expectError: false,
		},
		{
			name: "Valid with only min and max",
			taskSpec: v1alpha1.TaskSpec{
				Name:     "test-task",
				Replicas: 8,
				PartitionPolicy: &v1alpha1.PartitionPolicySpec{
					TotalPartitions:    4,
					PartitionSize:      2,
					MinPartitions:      1,
					ExpectedPartitions: []int32{1, 4},
				},
			},
			expectError: false,
		},
		{
			name: "No expected partitions (should pass)",
			taskSpec: v1alpha1.TaskSpec{
				Name:     "test-task",
				Replicas: 8,
				PartitionPolicy: &v1alpha1.PartitionPolicySpec{
					TotalPartitions: 4,
					PartitionSize:   2,
					MinPartitions:   1,
				},
			},
			expectError: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			job := &v1alpha1.Job{
				Spec: v1alpha1.JobSpec{
					Tasks: []v1alpha1.TaskSpec{tt.taskSpec},
				},
			}
			job.Name = "test-job"

			msg := validatePartitionPolicy(tt.taskSpec, job)

			if tt.expectError {
				if msg == "" {
					t.Errorf("Expected error but got none")
				} else if tt.errorContains != "" {
					if !contains(msg, tt.errorContains) {
						t.Errorf("Expected error message to contain '%s', but got: %s", tt.errorContains, msg)
					}
				}
			} else {
				if msg != "" {
					t.Errorf("Expected no error but got: %s", msg)
				}
			}
		})
	}
}

func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && findSubstring(s, substr))
}

func findSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
