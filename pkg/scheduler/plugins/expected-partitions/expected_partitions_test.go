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

package expectedpartitions

import (
	"reflect"
	"testing"
	"unsafe"

	"volcano.sh/apis/pkg/apis/scheduling"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/conf"
	"volcano.sh/volcano/pkg/scheduler/framework"
)

// TestPluginName verifies the plugin name
func TestPluginName(t *testing.T) {
	plugin := New(nil)
	if plugin.Name() != PluginName {
		t.Errorf("Expected plugin name %s, got %s", PluginName, plugin.Name())
	}
}

// TaskOpConfig defines a task operation configuration for creating test statements
type TaskOpConfig struct {
	TaskID string
	OpType framework.Operation // Allocate or Pipeline
}

// createStatements creates statements from task operations with flexible configuration
// - If onePerTask is true: creates one statement per task (for independent statement testing)
// - If onePerTask is false: creates one statement containing all tasks (for batch operation testing)
func createStatements(job *api.JobInfo, taskOps []TaskOpConfig, onePerTask bool) []*framework.Statement {
	if len(taskOps) == 0 {
		return []*framework.Statement{}
	}

	// Create a minimal mock session for the statements
	mockSession := &framework.Session{
		UID:  "test-session",
		Jobs: make(map[api.JobID]*api.JobInfo),
	}
	if job != nil {
		mockSession.Jobs[job.UID] = job
	}

	if onePerTask {
		// Create one statement per task
		stmts := make([]*framework.Statement, 0, len(taskOps))
		for _, taskOp := range taskOps {
			task := &api.TaskInfo{
				UID:    api.TaskID(taskOp.TaskID),
				Job:    job.UID,
				Resreq: api.EmptyResource(),
			}
			stmt := framework.NewStatement(mockSession)
			stmt.AddOperationForTest(taskOp.OpType, task, "")
			stmts = append(stmts, stmt)
		}
		return stmts
	}

	// Create one statement with all tasks
	stmt := framework.NewStatement(mockSession)
	for _, taskOp := range taskOps {
		task := &api.TaskInfo{
			UID:    api.TaskID(taskOp.TaskID),
			Job:    job.UID,
			Resreq: api.EmptyResource(),
		}
		stmt.AddOperationForTest(taskOp.OpType, task, "")
	}
	return []*framework.Statement{stmt}
}

// TestExpectedJobReady tests the expectedJobReady functionality with various scenarios
func TestExpectedJobReady(t *testing.T) {
	testCases := []struct {
		name                     string
		gidHasExpectedPartitions map[api.SubJobGID]bool
		gidAllocated             map[api.SubJobGID]int
		gidNextExpected          map[api.SubJobGID]int
		job                      *api.JobInfo
		taskOps                  []TaskOpConfig
		expectedResult           bool
	}{
		{
			name:                     "no expected partitions configured",
			gidHasExpectedPartitions: make(map[api.SubJobGID]bool),
			gidAllocated:             make(map[api.SubJobGID]int),
			gidNextExpected:          make(map[api.SubJobGID]int),
			job: &api.JobInfo{
				UID:       "job1",
				Name:      "test-job",
				Namespace: "default",
				SubJobs:   map[api.SubJobID]*api.SubJobInfo{},
			},
			taskOps:        []TaskOpConfig{},
			expectedResult: true,
		},
		{
			name: "expected partitions not met",
			gidHasExpectedPartitions: map[api.SubJobGID]bool{
				"gid1": true,
			},
			gidAllocated: map[api.SubJobGID]int{
				"gid1": 1,
			},
			gidNextExpected: map[api.SubJobGID]int{
				"gid1": 2,
			},
			job: &api.JobInfo{
				UID:       "job1",
				Name:      "test-job",
				Namespace: "default",
				SubJobs: map[api.SubJobID]*api.SubJobInfo{
					"subjob-1": {
						GID: "gid1",
					},
				},
			},
			taskOps:        []TaskOpConfig{},
			expectedResult: false,
		},
		{
			name: "expected partitions met",
			gidHasExpectedPartitions: map[api.SubJobGID]bool{
				"gid1": true,
			},
			gidAllocated: map[api.SubJobGID]int{
				"gid1": 2,
			},
			gidNextExpected: map[api.SubJobGID]int{
				"gid1": 2,
			},
			job: &api.JobInfo{
				UID:       "job1",
				Name:      "test-job",
				Namespace: "default",
				SubJobs: map[api.SubJobID]*api.SubJobInfo{
					"subjob-1": {
						GID: "gid1",
					},
				},
			},
			taskOps:        []TaskOpConfig{},
			expectedResult: true,
		},
		{
			name: "allocated + pending meets expected",
			gidHasExpectedPartitions: map[api.SubJobGID]bool{
				"gid1": true,
			},
			gidAllocated: map[api.SubJobGID]int{
				"gid1": 1,
			},
			gidNextExpected: map[api.SubJobGID]int{
				"gid1": 2,
			},
			job: &api.JobInfo{
				UID:             "job1",
				Name:            "test-job",
				Namespace:       "default",
				Tasks:           make(map[api.TaskID]*api.TaskInfo),
				TaskStatusIndex: make(map[api.TaskStatus]api.TasksMap),
				TaskToSubJob: map[api.TaskID]api.SubJobID{
					"task-1": "subjob-1",
				},
				SubJobs: map[api.SubJobID]*api.SubJobInfo{
					"subjob-1": {
						GID: "gid1",
					},
				},
			},
			taskOps: []TaskOpConfig{
				{TaskID: "task-1", OpType: framework.Allocate},
			},
			expectedResult: true,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			plugin := &expectedPartitionsPlugin{
				gidHasExpectedPartitions: tc.gidHasExpectedPartitions,
				gidAllocated:             tc.gidAllocated,
				gidNextExpected:          tc.gidNextExpected,
			}

			stmtList := createStatements(tc.job, tc.taskOps, false)
			result := plugin.expectedJobReady(tc.job, stmtList)

			if result != tc.expectedResult {
				t.Errorf("expected %v, got %v", tc.expectedResult, result)
			}
		})
	}
}

// TestDiscardStatements tests the discardStatements functionality with various scenarios
func TestDiscardStatements(t *testing.T) {
	testCases := []struct {
		name                     string
		gidHasExpectedPartitions map[api.SubJobGID]bool
		gidAllocated             map[api.SubJobGID]int
		gidNextExpected          map[api.SubJobGID]int
		job                      *api.JobInfo
		taskOps                  []TaskOpConfig
		expectedKeptStatements   int
	}{
		{
			name:                     "no expected partitions configured",
			gidHasExpectedPartitions: make(map[api.SubJobGID]bool),
			gidAllocated:             make(map[api.SubJobGID]int),
			gidNextExpected:          make(map[api.SubJobGID]int),
			job: &api.JobInfo{
				UID:       "job1",
				Name:      "test-job",
				Namespace: "default",
				SubJobs:   map[api.SubJobID]*api.SubJobInfo{},
			},
			taskOps: []TaskOpConfig{
				{TaskID: "task-1", OpType: framework.Allocate},
				{TaskID: "task-2", OpType: framework.Allocate},
				{TaskID: "task-3", OpType: framework.Allocate},
			},
			expectedKeptStatements: 3,
		},
		{
			name: "expected is met - statements kept",
			gidHasExpectedPartitions: map[api.SubJobGID]bool{
				"gid1": true,
			},
			gidAllocated: map[api.SubJobGID]int{
				"gid1": 1,
			},
			gidNextExpected: map[api.SubJobGID]int{
				"gid1": 2,
			},
			job: &api.JobInfo{
				UID:             "job1",
				Name:            "test-job",
				Namespace:       "default",
				Tasks:           make(map[api.TaskID]*api.TaskInfo),
				TaskStatusIndex: make(map[api.TaskStatus]api.TasksMap),
				TaskToSubJob: map[api.TaskID]api.SubJobID{
					"task-1": "subjob-1",
				},
				SubJobs: map[api.SubJobID]*api.SubJobInfo{
					"subjob-1": {
						GID: "gid1",
					},
				},
			},
			taskOps: []TaskOpConfig{
				{TaskID: "task-1", OpType: framework.Allocate},
			},
			expectedKeptStatements: 1,
		},
		{
			name: "multiple GIDs - one meets expected, one does not",
			gidHasExpectedPartitions: map[api.SubJobGID]bool{
				"gid1": true,
				"gid2": true,
			},
			gidAllocated: map[api.SubJobGID]int{
				"gid1": 1, // Already has 1, expects 2 - will meet with 1 more statement
				"gid2": 1, // Already has 1, expects 3 - will NOT meet with 1 more statement (only reaches 2)
			},
			gidNextExpected: map[api.SubJobGID]int{
				"gid1": 2, // Needs 2 total, has 1, adding 1 more = 2 (meets expected)
				"gid2": 3, // Needs 3 total, has 1, adding 1 more = 2 (does NOT meet expected)
			},
			job: &api.JobInfo{
				UID:             "job1",
				Name:            "test-job",
				Namespace:       "default",
				Tasks:           make(map[api.TaskID]*api.TaskInfo),
				TaskStatusIndex: make(map[api.TaskStatus]api.TasksMap),
				TotalRequest:    api.EmptyResource(),
				Allocated:       api.EmptyResource(),
				TaskToSubJob: map[api.TaskID]api.SubJobID{
					"task-1": "subjob-1", // gid1
					"task-2": "subjob-2", // gid2
				},
				SubJobs: map[api.SubJobID]*api.SubJobInfo{
					"subjob-1": {
						GID: "gid1",
					},
					"subjob-2": {
						GID: "gid2",
					},
				},
			},
			taskOps: []TaskOpConfig{
				{TaskID: "task-1", OpType: framework.Allocate},
				{TaskID: "task-2", OpType: framework.Allocate},
			},
			// Only gid1's statement should be kept (gid2's should be discarded)
			expectedKeptStatements: 1,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			plugin := &expectedPartitionsPlugin{
				gidHasExpectedPartitions: tc.gidHasExpectedPartitions,
				gidAllocated:             tc.gidAllocated,
				gidNextExpected:          tc.gidNextExpected,
			}

			// Create independent statements for each task
			stmtList := createStatements(tc.job, tc.taskOps, true)
			result := plugin.discardStatements(tc.job, stmtList)
			resultStmts, ok := result.([]*framework.Statement)
			if !ok {
				t.Fatalf("discardStatements returned invalid type")
			}

			if len(resultStmts) != tc.expectedKeptStatements {
				t.Errorf("expected %d statements to be kept, got %d", tc.expectedKeptStatements, len(resultStmts))
			}
		})
	}
}

// TestGetPendingFromStmtList tests the getPendingFromStmtList functionality
func TestGetPendingFromStmtList(t *testing.T) {
	testCases := []struct {
		name           string
		job            *api.JobInfo
		taskOps        []TaskOpConfig // Tasks and their operation types to create in statement
		expectedCounts map[api.SubJobGID]int
	}{
		{
			name: "empty statement list",
			job: &api.JobInfo{
				UID:          "job1",
				Name:         "test-job",
				Namespace:    "default",
				TaskToSubJob: map[api.TaskID]api.SubJobID{},
				SubJobs:      map[api.SubJobID]*api.SubJobInfo{},
			},
			taskOps:        []TaskOpConfig{},
			expectedCounts: map[api.SubJobGID]int{},
		},
		{
			name: "single GID with one task",
			job: &api.JobInfo{
				UID:       "job1",
				Name:      "test-job",
				Namespace: "default",
				TaskToSubJob: map[api.TaskID]api.SubJobID{
					"task-1": "subjob-1",
				},
				SubJobs: map[api.SubJobID]*api.SubJobInfo{
					"subjob-1": {
						UID: "subjob-1",
						GID: "gid-1",
					},
				},
			},
			taskOps: []TaskOpConfig{
				{TaskID: "task-1", OpType: framework.Allocate},
			},
			expectedCounts: map[api.SubJobGID]int{
				"gid-1": 1,
			},
		},
		{
			name: "single GID with multiple tasks from same SubJob",
			job: &api.JobInfo{
				UID:       "job1",
				Name:      "test-job",
				Namespace: "default",
				TaskToSubJob: map[api.TaskID]api.SubJobID{
					"task-1": "subjob-1",
					"task-2": "subjob-1", // Same SubJob
				},
				SubJobs: map[api.SubJobID]*api.SubJobInfo{
					"subjob-1": {
						UID: "subjob-1",
						GID: "gid-1",
					},
				},
			},
			taskOps: []TaskOpConfig{
				{TaskID: "task-1", OpType: framework.Allocate},
				{TaskID: "task-2", OpType: framework.Allocate},
			},
			expectedCounts: map[api.SubJobGID]int{
				"gid-1": 1, // Should count SubJob, not tasks
			},
		},
		{
			name: "single GID with multiple SubJobs",
			job: &api.JobInfo{
				UID:       "job1",
				Name:      "test-job",
				Namespace: "default",
				TaskToSubJob: map[api.TaskID]api.SubJobID{
					"task-1": "subjob-1",
					"task-2": "subjob-2",
					"task-3": "subjob-3",
				},
				SubJobs: map[api.SubJobID]*api.SubJobInfo{
					"subjob-1": {
						UID: "subjob-1",
						GID: "gid-1",
					},
					"subjob-2": {
						UID: "subjob-2",
						GID: "gid-1",
					},
					"subjob-3": {
						UID: "subjob-3",
						GID: "gid-1",
					},
				},
			},
			taskOps: []TaskOpConfig{
				{TaskID: "task-1", OpType: framework.Allocate},
				{TaskID: "task-2", OpType: framework.Allocate},
				{TaskID: "task-3", OpType: framework.Allocate},
			},
			expectedCounts: map[api.SubJobGID]int{
				"gid-1": 3, // 3 different SubJobs
			},
		},
		{
			name: "multiple GIDs",
			job: &api.JobInfo{
				UID:       "job1",
				Name:      "test-job",
				Namespace: "default",
				TaskToSubJob: map[api.TaskID]api.SubJobID{
					"task-1": "subjob-1",
					"task-2": "subjob-2",
					"task-3": "subjob-3",
					"task-4": "subjob-4",
				},
				SubJobs: map[api.SubJobID]*api.SubJobInfo{
					"subjob-1": {
						UID: "subjob-1",
						GID: "gid-1",
					},
					"subjob-2": {
						UID: "subjob-2",
						GID: "gid-1",
					},
					"subjob-3": {
						UID: "subjob-3",
						GID: "gid-2",
					},
					"subjob-4": {
						UID: "subjob-4",
						GID: "gid-2",
					},
				},
			},
			taskOps: []TaskOpConfig{
				{TaskID: "task-1", OpType: framework.Allocate},
				{TaskID: "task-2", OpType: framework.Allocate},
				{TaskID: "task-3", OpType: framework.Allocate},
				{TaskID: "task-4", OpType: framework.Allocate},
			},
			expectedCounts: map[api.SubJobGID]int{
				"gid-1": 2,
				"gid-2": 2,
			},
		},
		{
			name: "mixed Allocate and Pipeline operations - only count Allocate",
			job: &api.JobInfo{
				UID:       "job1",
				Name:      "test-job",
				Namespace: "default",
				TaskToSubJob: map[api.TaskID]api.SubJobID{
					"task-1": "subjob-1",
					"task-2": "subjob-2",
					"task-3": "subjob-3",
				},
				SubJobs: map[api.SubJobID]*api.SubJobInfo{
					"subjob-1": {
						UID: "subjob-1",
						GID: "gid-1",
					},
					"subjob-2": {
						UID: "subjob-2",
						GID: "gid-1",
					},
					"subjob-3": {
						UID: "subjob-3",
						GID: "gid-1",
					},
				},
			},
			taskOps: []TaskOpConfig{
				{TaskID: "task-1", OpType: framework.Allocate},
				{TaskID: "task-2", OpType: framework.Pipeline},
				{TaskID: "task-3", OpType: framework.Allocate},
			},
			// Only count task-1 and task-3 (Allocate), not task-2 (Pipeline)
			expectedCounts: map[api.SubJobGID]int{
				"gid-1": 2,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			plugin := &expectedPartitionsPlugin{}

			stmtList := createStatements(tc.job, tc.taskOps, false)
			result := plugin.getPendingFromStmtList(stmtList, tc.job)

			// Verify the result
			if len(result) != len(tc.expectedCounts) {
				t.Errorf("expected %d GIDs, got %d", len(tc.expectedCounts), len(result))
			}

			for gid, expectedCount := range tc.expectedCounts {
				actualCount, exists := result[gid]
				if !exists {
					t.Errorf("expected GID %s to exist in result", gid)
					continue
				}
				if actualCount != expectedCount {
					t.Errorf("GID %s: expected count %d, got %d", gid, expectedCount, actualCount)
				}
			}

			// Check for unexpected GIDs
			for gid := range result {
				if _, expected := tc.expectedCounts[gid]; !expected {
					t.Errorf("unexpected GID %s in result", gid)
				}
			}
		})
	}
}

// TestGetExpectedSubJobsReady tests the getExpectedSubJobsReady functionality
func TestGetExpectedSubJobsReady(t *testing.T) {
	testCases := []struct {
		name            string
		gidNextExpected map[api.SubJobGID]int
		gidAllocated    map[api.SubJobGID]int
		job             *api.JobInfo
		taskOps         []TaskOpConfig
		expectedReady   map[api.SubJobGID]bool
	}{
		{
			name: "no pending statements - mixed ready status",
			gidNextExpected: map[api.SubJobGID]int{
				"gid-1": 2,
				"gid-2": 1,
			},
			gidAllocated: map[api.SubJobGID]int{
				"gid-1": 1, // 1 < 2, not ready
				"gid-2": 1, // 1 >= 1, ready
			},
			job: &api.JobInfo{
				UID:       "job1",
				Name:      "test-job",
				Namespace: "default",
				SubJobs: map[api.SubJobID]*api.SubJobInfo{
					"subjob-1": {
						GID: "gid-1",
					},
					"subjob-2": {
						GID: "gid-2",
					},
				},
			},
			taskOps: []TaskOpConfig{}, // Empty stmtList
			expectedReady: map[api.SubJobGID]bool{
				"gid-1": false,
				"gid-2": true,
			},
		},
		{
			name: "with pending statements - one GID becomes ready",
			gidNextExpected: map[api.SubJobGID]int{
				"gid-1": 3,
				"gid-2": 2,
			},
			gidAllocated: map[api.SubJobGID]int{
				"gid-1": 1, // Has 1, needs 3
				"gid-2": 1, // Has 1, needs 2
			},
			job: &api.JobInfo{
				UID:       "job1",
				Name:      "test-job",
				Namespace: "default",
				TaskToSubJob: map[api.TaskID]api.SubJobID{
					"task-1": "subjob-1",
					"task-2": "subjob-2",
				},
				SubJobs: map[api.SubJobID]*api.SubJobInfo{
					"subjob-1": {
						GID: "gid-1",
					},
					"subjob-2": {
						GID: "gid-2",
					},
				},
			},
			taskOps: []TaskOpConfig{
				{TaskID: "task-2", OpType: framework.Allocate}, // One task for gid-2
			},
			expectedReady: map[api.SubJobGID]bool{
				"gid-1": false, // 1 + 0 = 1 < 3, not ready
				"gid-2": true,  // 1 + 1 = 2 >= 2, ready
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			plugin := &expectedPartitionsPlugin{
				gidNextExpected: tc.gidNextExpected,
				gidAllocated:    tc.gidAllocated,
			}

			stmtList := createStatements(tc.job, tc.taskOps, false)
			result := plugin.getExpectedSubJobsReady(tc.job, stmtList)

			// Verify all expected GIDs
			if len(result) != len(tc.expectedReady) {
				t.Errorf("Expected %d GIDs in result, got %d", len(tc.expectedReady), len(result))
			}

			for gid, expectedReady := range tc.expectedReady {
				actualReady, exists := result[gid]
				if !exists {
					t.Errorf("GID %s: expected to exist in result but not found", gid)
					continue
				}
				if actualReady != expectedReady {
					t.Errorf("GID %s: expected ready=%v, got ready=%v", gid, expectedReady, actualReady)
				}
			}
		})
	}
}

// TestBuildAllocationContext tests the buildAllocationContext functionality with various scenarios
func TestBuildAllocationContext(t *testing.T) {
	testCases := []struct {
		name                 string
		jobs                 map[api.JobID]*api.JobInfo
		expectedHasExpected  map[api.SubJobGID]bool
		expectedAllocated    map[api.SubJobGID]int
		expectedNextExpected map[api.SubJobGID]int
		subJobReadyOverrides map[api.SubJobID]bool // Override SubJobReady results for testing
	}{
		{
			name:                 "no jobs in session",
			jobs:                 map[api.JobID]*api.JobInfo{},
			expectedHasExpected:  map[api.SubJobGID]bool{},
			expectedAllocated:    map[api.SubJobGID]int{},
			expectedNextExpected: map[api.SubJobGID]int{},
		},
		{
			name: "job with no PodGroup",
			jobs: map[api.JobID]*api.JobInfo{
				"job1": {
					UID:       "job1",
					Name:      "test-job",
					Namespace: "default",
					PodGroup:  nil,
					SubJobs:   map[api.SubJobID]*api.SubJobInfo{},
				},
			},
			expectedHasExpected:  map[api.SubJobGID]bool{},
			expectedAllocated:    map[api.SubJobGID]int{},
			expectedNextExpected: map[api.SubJobGID]int{},
		},
		{
			name: "job with PodGroup but no SubGroupPolicy",
			jobs: map[api.JobID]*api.JobInfo{
				"job1": {
					UID:       "job1",
					Name:      "test-job",
					Namespace: "default",
					PodGroup: &api.PodGroup{
						PodGroup: scheduling.PodGroup{
							Spec: scheduling.PodGroupSpec{
								SubGroupPolicy: []scheduling.SubGroupPolicySpec{},
							},
						},
					},
					SubJobs: map[api.SubJobID]*api.SubJobInfo{},
				},
			},
			expectedHasExpected:  map[api.SubJobGID]bool{},
			expectedAllocated:    map[api.SubJobGID]int{},
			expectedNextExpected: map[api.SubJobGID]int{},
		},
		{
			name: "job with SubGroupPolicy but no ExpectedSubGroups",
			jobs: map[api.JobID]*api.JobInfo{
				"job1": {
					UID:       "job1",
					Name:      "test-job",
					Namespace: "default",
					PodGroup: &api.PodGroup{
						PodGroup: scheduling.PodGroup{
							Spec: scheduling.PodGroupSpec{
								SubGroupPolicy: []scheduling.SubGroupPolicySpec{
									{
										Name:              "worker",
										ExpectedSubGroups: []int32{}, // No expected
									},
								},
							},
						},
					},
					SubJobs: map[api.SubJobID]*api.SubJobInfo{},
				},
			},
			expectedHasExpected: map[api.SubJobGID]bool{
				"job1/worker": false,
			},
			expectedAllocated:    map[api.SubJobGID]int{},
			expectedNextExpected: map[api.SubJobGID]int{},
		},
		{
			name: "job with ExpectedSubGroups and no ready SubJobs",
			jobs: map[api.JobID]*api.JobInfo{
				"job1": {
					UID:       "job1",
					Name:      "test-job",
					Namespace: "default",
					PodGroup: &api.PodGroup{
						PodGroup: scheduling.PodGroup{
							Spec: scheduling.PodGroupSpec{
								SubGroupPolicy: []scheduling.SubGroupPolicySpec{
									{
										Name:              "worker",
										ExpectedSubGroups: []int32{1, 2, 4},
									},
								},
							},
						},
					},
					SubJobs: map[api.SubJobID]*api.SubJobInfo{
						"subjob-1": {
							UID: "subjob-1",
							GID: "job1/worker",
						},
						"subjob-2": {
							UID: "subjob-2",
							GID: "job1/worker",
						},
					},
				},
			},
			expectedHasExpected: map[api.SubJobGID]bool{
				"job1/worker": true,
			},
			expectedAllocated: map[api.SubJobGID]int{
				"job1/worker": 0, // No ready SubJobs
			},
			expectedNextExpected: map[api.SubJobGID]int{
				"job1/worker": 1, // First value in ExpectedSubGroups
			},
			subJobReadyOverrides: map[api.SubJobID]bool{
				"subjob-1": false,
				"subjob-2": false,
			},
		},
		{
			name: "job with ExpectedSubGroups and some ready SubJobs",
			jobs: map[api.JobID]*api.JobInfo{
				"job1": {
					UID:       "job1",
					Name:      "test-job",
					Namespace: "default",
					PodGroup: &api.PodGroup{
						PodGroup: scheduling.PodGroup{
							Spec: scheduling.PodGroupSpec{
								SubGroupPolicy: []scheduling.SubGroupPolicySpec{
									{
										Name:              "worker",
										ExpectedSubGroups: []int32{1, 2, 4},
									},
								},
							},
						},
					},
					SubJobs: map[api.SubJobID]*api.SubJobInfo{
						"subjob-1": {
							UID: "subjob-1",
							GID: "job1/worker",
						},
						"subjob-2": {
							UID: "subjob-2",
							GID: "job1/worker",
						},
						"subjob-3": {
							UID: "subjob-3",
							GID: "job1/worker",
						},
					},
				},
			},
			expectedHasExpected: map[api.SubJobGID]bool{
				"job1/worker": true,
			},
			expectedAllocated: map[api.SubJobGID]int{
				"job1/worker": 1, // 1 ready SubJob
			},
			expectedNextExpected: map[api.SubJobGID]int{
				"job1/worker": 2, // Next value after 1 in ExpectedSubGroups
			},
			subJobReadyOverrides: map[api.SubJobID]bool{
				"subjob-1": true,  // Ready
				"subjob-2": false, // Not ready
				"subjob-3": false, // Not ready
			},
		},
		{
			name: "job with ExpectedSubGroups - current equals intermediate expected",
			jobs: map[api.JobID]*api.JobInfo{
				"job1": {
					UID:       "job1",
					Name:      "test-job",
					Namespace: "default",
					PodGroup: &api.PodGroup{
						PodGroup: scheduling.PodGroup{
							Spec: scheduling.PodGroupSpec{
								SubGroupPolicy: []scheduling.SubGroupPolicySpec{
									{
										Name:              "worker",
										ExpectedSubGroups: []int32{1, 2, 4},
									},
								},
							},
						},
					},
					SubJobs: map[api.SubJobID]*api.SubJobInfo{
						"subjob-1": {
							UID: "subjob-1",
							GID: "job1/worker",
						},
						"subjob-2": {
							UID: "subjob-2",
							GID: "job1/worker",
						},
					},
				},
			},
			expectedHasExpected: map[api.SubJobGID]bool{
				"job1/worker": true,
			},
			expectedAllocated: map[api.SubJobGID]int{
				"job1/worker": 2, // 2 ready SubJobs
			},
			expectedNextExpected: map[api.SubJobGID]int{
				"job1/worker": 4, // Next value after 2 in ExpectedSubGroups
			},
			subJobReadyOverrides: map[api.SubJobID]bool{
				"subjob-1": true, // Ready
				"subjob-2": true, // Ready
			},
		},
		{
			name: "job with ExpectedSubGroups - all ready",
			jobs: map[api.JobID]*api.JobInfo{
				"job1": {
					UID:       "job1",
					Name:      "test-job",
					Namespace: "default",
					PodGroup: &api.PodGroup{
						PodGroup: scheduling.PodGroup{
							Spec: scheduling.PodGroupSpec{
								SubGroupPolicy: []scheduling.SubGroupPolicySpec{
									{
										Name:              "worker",
										ExpectedSubGroups: []int32{1, 2, 4},
									},
								},
							},
						},
					},
					SubJobs: map[api.SubJobID]*api.SubJobInfo{
						"subjob-1": {
							UID: "subjob-1",
							GID: "job1/worker",
						},
						"subjob-2": {
							UID: "subjob-2",
							GID: "job1/worker",
						},
						"subjob-3": {
							UID: "subjob-3",
							GID: "job1/worker",
						},
						"subjob-4": {
							UID: "subjob-4",
							GID: "job1/worker",
						},
					},
				},
			},
			expectedHasExpected: map[api.SubJobGID]bool{
				"job1/worker": true,
			},
			expectedAllocated: map[api.SubJobGID]int{
				"job1/worker": 4, // All 4 ready SubJobs
			},
			expectedNextExpected: map[api.SubJobGID]int{
				"job1/worker": 4, // Max value (last) in ExpectedSubGroups
			},
			subJobReadyOverrides: map[api.SubJobID]bool{
				"subjob-1": true,
				"subjob-2": true,
				"subjob-3": true,
				"subjob-4": true,
			},
		},
		{
			name: "multiple GIDs with different configurations",
			jobs: map[api.JobID]*api.JobInfo{
				"job1": {
					UID:       "job1",
					Name:      "test-job1",
					Namespace: "default",
					PodGroup: &api.PodGroup{
						PodGroup: scheduling.PodGroup{
							Spec: scheduling.PodGroupSpec{
								SubGroupPolicy: []scheduling.SubGroupPolicySpec{
									{
										Name:              "worker",
										ExpectedSubGroups: []int32{1, 2, 4},
									},
									{
										Name:              "ps",
										ExpectedSubGroups: []int32{2, 4},
									},
								},
							},
						},
					},
					SubJobs: map[api.SubJobID]*api.SubJobInfo{
						"subjob-worker-1": {
							UID: "subjob-worker-1",
							GID: "job1/worker",
						},
						"subjob-worker-2": {
							UID: "subjob-worker-2",
							GID: "job1/worker",
						},
						"subjob-ps-1": {
							UID: "subjob-ps-1",
							GID: "job1/ps",
						},
					},
				},
			},
			expectedHasExpected: map[api.SubJobGID]bool{
				"job1/worker": true,
				"job1/ps":     true,
			},
			expectedAllocated: map[api.SubJobGID]int{
				"job1/worker": 1, // 1 ready worker
				"job1/ps":     1, // 1 ready ps
			},
			expectedNextExpected: map[api.SubJobGID]int{
				"job1/worker": 2, // Next after 1
				"job1/ps":     2, // First value (1 < 2)
			},
			subJobReadyOverrides: map[api.SubJobID]bool{
				"subjob-worker-1": true,
				"subjob-worker-2": false,
				"subjob-ps-1":     true,
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Create session with Tiers configuration to enable SubJobReady plugin
			enabled := true
			mockSession := &framework.Session{
				UID:  "test-session",
				Jobs: tc.jobs,
				Tiers: []conf.Tier{
					{
						Plugins: []conf.PluginOption{
							{
								Name:               "test-subjob-ready",
								EnabledSubJobReady: &enabled,
							},
						},
					},
				},
			}

			// Initialize private subJobReadyFns map using unsafe reflection
			sessionValue := reflect.ValueOf(mockSession).Elem()
			subJobReadyFnsField := sessionValue.FieldByName("subJobReadyFns")
			if subJobReadyFnsField.IsValid() {
				// Use unsafe to set the private field
				subJobReadyFnsMap := reflect.NewAt(subJobReadyFnsField.Type(), unsafe.Pointer(subJobReadyFnsField.UnsafeAddr())).Elem()
				subJobReadyFnsMap.Set(reflect.MakeMap(subJobReadyFnsField.Type()))
			}

			// Register custom SubJobReady function
			mockSession.AddSubJobReadyFn("test-subjob-ready", func(obj interface{}) bool {
				subJob, ok := obj.(*api.SubJobInfo)
				if !ok {
					return true
				}
				if tc.subJobReadyOverrides == nil {
					return true
				}
				ready, exists := tc.subJobReadyOverrides[subJob.UID]
				if !exists {
					return true
				}
				return ready
			})

			// Create plugin instance
			plugin := &expectedPartitionsPlugin{
				session: mockSession,
			}

			// Call the real buildAllocationContext function
			plugin.buildAllocationContext(mockSession)

			// Verify gidHasExpectedPartitions
			if len(plugin.gidHasExpectedPartitions) != len(tc.expectedHasExpected) {
				t.Errorf("gidHasExpectedPartitions: expected %d entries, got %d", len(tc.expectedHasExpected), len(plugin.gidHasExpectedPartitions))
			}
			for gid, expected := range tc.expectedHasExpected {
				if actual, exists := plugin.gidHasExpectedPartitions[gid]; !exists || actual != expected {
					t.Errorf("gidHasExpectedPartitions[%s]: expected %v, got %v (exists=%v)", gid, expected, actual, exists)
				}
			}

			// Verify gidAllocated
			if len(plugin.gidAllocated) != len(tc.expectedAllocated) {
				t.Errorf("gidAllocated: expected %d entries, got %d", len(tc.expectedAllocated), len(plugin.gidAllocated))
			}
			for gid, expected := range tc.expectedAllocated {
				if actual, exists := plugin.gidAllocated[gid]; !exists || actual != expected {
					t.Errorf("gidAllocated[%s]: expected %d, got %d (exists=%v)", gid, expected, actual, exists)
				}
			}

			// Verify gidNextExpected
			if len(plugin.gidNextExpected) != len(tc.expectedNextExpected) {
				t.Errorf("gidNextExpected: expected %d entries, got %d", len(tc.expectedNextExpected), len(plugin.gidNextExpected))
			}
			for gid, expected := range tc.expectedNextExpected {
				if actual, exists := plugin.gidNextExpected[gid]; !exists || actual != expected {
					t.Errorf("gidNextExpected[%s]: expected %d, got %d (exists=%v)", gid, expected, actual, exists)
				}
			}
		})
	}
}

// TestSubJobOrderFn tests the SubJob ordering function
func TestSubJobOrderFn(t *testing.T) {
	testCases := []struct {
		name                     string
		gidHasExpectedPartitions map[api.SubJobGID]bool
		gidNextExpected          map[api.SubJobGID]int
		leftSubJob               *api.SubJobInfo
		rightSubJob              *api.SubJobInfo
		expectedResult           int
	}{
		{
			name: "left has expected, right does not - left has higher priority",
			gidHasExpectedPartitions: map[api.SubJobGID]bool{
				"gid1": true,
				"gid2": false,
			},
			gidNextExpected: map[api.SubJobGID]int{
				"gid1": 2,
			},
			leftSubJob: &api.SubJobInfo{
				UID: "subjob-1",
				GID: "gid1",
			},
			rightSubJob: &api.SubJobInfo{
				UID: "subjob-2",
				GID: "gid2",
			},
			expectedResult: -1,
		},
		{
			name: "left does not have expected, right has - right has higher priority",
			gidHasExpectedPartitions: map[api.SubJobGID]bool{
				"gid1": false,
				"gid2": true,
			},
			gidNextExpected: map[api.SubJobGID]int{
				"gid2": 2,
			},
			leftSubJob: &api.SubJobInfo{
				UID: "subjob-1",
				GID: "gid1",
			},
			rightSubJob: &api.SubJobInfo{
				UID: "subjob-2",
				GID: "gid2",
			},
			expectedResult: 1,
		},
		{
			name: "both have expected - left should schedule, right should not",
			gidHasExpectedPartitions: map[api.SubJobGID]bool{
				"gid1": true,
				"gid2": true,
			},
			gidNextExpected: map[api.SubJobGID]int{
				"gid1": 3,
				"gid2": 2,
			},
			leftSubJob: &api.SubJobInfo{
				UID:        "subjob-1",
				GID:        "gid1",
				MatchIndex: 2, // 2 < 3, should schedule
			},
			rightSubJob: &api.SubJobInfo{
				UID:        "subjob-2",
				GID:        "gid2",
				MatchIndex: 3, // 3 >= 2, should not schedule
			},
			expectedResult: -1,
		},
		{
			name: "both have expected - left should not schedule, right should",
			gidHasExpectedPartitions: map[api.SubJobGID]bool{
				"gid1": true,
				"gid2": true,
			},
			gidNextExpected: map[api.SubJobGID]int{
				"gid1": 2,
				"gid2": 3,
			},
			leftSubJob: &api.SubJobInfo{
				UID:        "subjob-1",
				GID:        "gid1",
				MatchIndex: 2, // 2 >= 2, should not schedule
			},
			rightSubJob: &api.SubJobInfo{
				UID:        "subjob-2",
				GID:        "gid2",
				MatchIndex: 1, // 1 < 3, should schedule
			},
			expectedResult: 1,
		},
		{
			name: "both have expected - both should schedule",
			gidHasExpectedPartitions: map[api.SubJobGID]bool{
				"gid1": true,
				"gid2": true,
			},
			gidNextExpected: map[api.SubJobGID]int{
				"gid1": 3,
				"gid2": 4,
			},
			leftSubJob: &api.SubJobInfo{
				UID:        "subjob-1",
				GID:        "gid1",
				MatchIndex: 1, // 1 < 3, should schedule
			},
			rightSubJob: &api.SubJobInfo{
				UID:        "subjob-2",
				GID:        "gid2",
				MatchIndex: 2, // 2 < 4, should schedule
			},
			expectedResult: 0,
		},
		{
			name: "both have expected - both should not schedule",
			gidHasExpectedPartitions: map[api.SubJobGID]bool{
				"gid1": true,
				"gid2": true,
			},
			gidNextExpected: map[api.SubJobGID]int{
				"gid1": 2,
				"gid2": 3,
			},
			leftSubJob: &api.SubJobInfo{
				UID:        "subjob-1",
				GID:        "gid1",
				MatchIndex: 5, // 5 >= 2, should not schedule
			},
			rightSubJob: &api.SubJobInfo{
				UID:        "subjob-2",
				GID:        "gid2",
				MatchIndex: 4, // 4 >= 3, should not schedule
			},
			expectedResult: 0,
		},
		{
			name: "neither has expected - equal priority",
			gidHasExpectedPartitions: map[api.SubJobGID]bool{
				"gid1": false,
				"gid2": false,
			},
			gidNextExpected: map[api.SubJobGID]int{},
			leftSubJob: &api.SubJobInfo{
				UID: "subjob-1",
				GID: "gid1",
			},
			rightSubJob: &api.SubJobInfo{
				UID: "subjob-2",
				GID: "gid2",
			},
			expectedResult: 0,
		},
		{
			name: "both have expected - MatchIndex equals nextExpected",
			gidHasExpectedPartitions: map[api.SubJobGID]bool{
				"gid1": true,
				"gid2": true,
			},
			gidNextExpected: map[api.SubJobGID]int{
				"gid1": 2,
				"gid2": 3,
			},
			leftSubJob: &api.SubJobInfo{
				UID:        "subjob-1",
				GID:        "gid1",
				MatchIndex: 2, // 2 >= 2, should not schedule (boundary case)
			},
			rightSubJob: &api.SubJobInfo{
				UID:        "subjob-2",
				GID:        "gid2",
				MatchIndex: 3, // 3 >= 3, should not schedule (boundary case)
			},
			expectedResult: 0,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			plugin := &expectedPartitionsPlugin{
				gidHasExpectedPartitions: tc.gidHasExpectedPartitions,
				gidNextExpected:          tc.gidNextExpected,
			}

			// Create the subJobOrderFn (same logic as in OnSessionOpen)
			subJobOrderFn := func(l, r interface{}) int {
				lv := l.(*api.SubJobInfo)
				rv := r.(*api.SubJobInfo)

				lHasExpected := plugin.gidHasExpectedPartitions[lv.GID]
				rHasExpected := plugin.gidHasExpectedPartitions[rv.GID]

				// SubJob with ExpectedPartitions should have higher priority
				if lHasExpected != rHasExpected {
					if lHasExpected {
						return -1
					}
					return 1
				}

				// If both have expected partitions, check if they should be scheduled in this round
				if lHasExpected && rHasExpected {
					lNextExpected := plugin.gidNextExpected[lv.GID]
					rNextExpected := plugin.gidNextExpected[rv.GID]

					// Check if SubJob's MatchIndex is within the expected range
					// MatchIndex < nextExpected means this partition should be scheduled in this round
					lShouldSchedule := lv.MatchIndex < lNextExpected
					rShouldSchedule := rv.MatchIndex < rNextExpected

					if lShouldSchedule != rShouldSchedule {
						if lShouldSchedule {
							return -1 // l should be scheduled first
						}
						return 1 // r should be scheduled first
					}
				}

				return 0
			}

			result := subJobOrderFn(tc.leftSubJob, tc.rightSubJob)

			if result != tc.expectedResult {
				t.Errorf("expected %d, got %d", tc.expectedResult, result)
			}
		})
	}
}
