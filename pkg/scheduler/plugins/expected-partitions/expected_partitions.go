/*
Copyright 2018 The Kubernetes Authors.
Copyright 2018-2025 The Volcano Authors.

Modifications made by Volcano authors:
- Added comprehensive operation management with save/recover capabilities
- Enhanced with Allocate/UnAllocate and UnPipeline operations
- Added improved error handling and rollback support

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
	"k8s.io/klog/v2"
	"volcano.sh/volcano/pkg/scheduler/api"
	"volcano.sh/volcano/pkg/scheduler/framework"
)

// PluginName indicates name of volcano scheduler plugin.
const PluginName = "expected-partitions"

type expectedPartitionsPlugin struct {
	pluginArguments framework.Arguments
	session         *framework.Session
	// Allocation context for current session
	gidHasExpectedPartitions map[api.SubJobGID]bool
	gidAllocated             map[api.SubJobGID]int // Current number of ready SubJobs for each GID
	gidNextExpected          map[api.SubJobGID]int // Next expected target for each GID
}

// New return expected-partitions plugin
func New(arguments framework.Arguments) framework.Plugin {
	return &expectedPartitionsPlugin{
		pluginArguments:          arguments,
		gidHasExpectedPartitions: make(map[api.SubJobGID]bool),
	}
}

func (ep *expectedPartitionsPlugin) Name() string {
	return PluginName
}

func (ep *expectedPartitionsPlugin) OnSessionOpen(ssn *framework.Session) {
	ep.session = ssn

	// Build allocation context and ExpectedPartitions cache in one pass
	ep.buildAllocationContext(ssn)

	// SubJob with ExpectedPartitions should have higher priority to ensure
	// the expected-partitions feature takes effect.
	subJobOrderFn := func(l, r interface{}) int {
		lv := l.(*api.SubJobInfo)
		rv := r.(*api.SubJobInfo)

		lHasExpected := ep.gidHasExpectedPartitions[lv.GID]
		rHasExpected := ep.gidHasExpectedPartitions[rv.GID]

		klog.V(4).Infof("ExpectedPartitions SubJobOrderFn: <%v> hasExpected: %t, <%v> hasExpected: %t",
			lv.UID, lHasExpected, rv.UID, rHasExpected)

		// SubJob with ExpectedPartitions should have higher priority
		if lHasExpected != rHasExpected {
			if lHasExpected {
				return -1
			}
			return 1
		}

		// If both have or both don't have ExpectedPartitions, treat them equally
		// Let other plugins' SubJobOrderFn determine the priority
		return 0
	}

	ssn.AddSubJobOrderFn(ep.Name(), subJobOrderFn)
	ssn.AddExpectedJobReadyFn(ep.Name(), ep.expectedJobReady)
	ssn.AddDiscardStatementsFn(ep.Name(), ep.discardStatements)
	ssn.AddJobAllocatedFn(ep.Name(), ep.jobAllocated)
}

func (ep *expectedPartitionsPlugin) OnSessionClose(ssn *framework.Session) {
	// Clean up caches and session reference
	ep.gidHasExpectedPartitions = nil
	ep.gidAllocated = nil
	ep.gidNextExpected = nil
	ep.session = nil
}

// buildAllocationContext builds the initial allocation context and ExpectedPartitions cache for the current session.
// It calculates:
// - gidHasExpectedPartitions: whether each GID has ExpectedPartitions configured (remains constant)
// - gidAllocated: initial allocated count for each GID (updated dynamically via jobAllocated hook)
// - gidNextExpected: initial next expected target for each GID (updated dynamically via jobAllocated hook)
func (ep *expectedPartitionsPlugin) buildAllocationContext(ssn *framework.Session) {
	ep.gidHasExpectedPartitions = make(map[api.SubJobGID]bool)
	ep.gidAllocated = make(map[api.SubJobGID]int)
	ep.gidNextExpected = make(map[api.SubJobGID]int)

	for _, job := range ssn.Jobs {
		if job.PodGroup == nil {
			continue
		}

		for _, policy := range job.PodGroup.Spec.SubGroupPolicy {
			gid := api.GetSubJobGID(job.UID, policy.Name)
			hasExpected := len(policy.ExpectedSubGroups) > 0
			ep.gidHasExpectedPartitions[gid] = hasExpected

			if !hasExpected {
				continue
			}

			// Count SubJobs that are already ready (have met their minAvailable)
			current := 0
			for _, subJob := range job.SubJobs {
				if subJob.GID != gid {
					continue
				}

				if ep.session.SubJobReady(job, subJob) {
					current++
				}
			}
			ep.gidAllocated[gid] = current

			// Calculate next expected target
			target := policy.ExpectedSubGroups[len(policy.ExpectedSubGroups)-1] // Default to max
			for _, e := range policy.ExpectedSubGroups {
				if int(e) > current {
					target = e
					break
				}
			}
			ep.gidNextExpected[gid] = int(target)

			klog.V(3).Infof("Job %s SubGroupPolicy %s (GID: %s) has ExpectedPartitions: %v, allocated=%d, nextExpected=%d",
				job.UID, policy.Name, gid, policy.ExpectedSubGroups, current, target)
		}
	}

	klog.V(3).Infof("Built allocation context: %d total GIDs, %d with ExpectedPartitions",
		len(ep.gidHasExpectedPartitions), len(ep.gidAllocated))
}

// getPendingFromStmtList calculates the number of partitions that may be allocated
// if the given statement list is committed. This is used during the allocation attempt to track progress.
func (ep *expectedPartitionsPlugin) getPendingFromStmtList(stmtList []*framework.Statement, job *api.JobInfo) map[api.SubJobGID]int {
	pendingByGid := make(map[api.SubJobGID]int)
	if len(stmtList) == 0 {
		return pendingByGid
	}

	// Get all tasks from Allocate operations only (not Pipeline or Evict operations)
	pendingByTask := make(map[api.TaskID]bool)
	for _, stmt := range stmtList {
		tasks := stmt.GetTasksFromAllocateOperations()
		for taskID := range tasks {
			pendingByTask[taskID] = true
		}
	}

	// Group by SubJob and count unique SubJobs per GID
	pendingMap := make(map[api.SubJobGID]map[api.SubJobID]bool)
	for taskID := range pendingByTask {
		subJobID, exists := job.TaskToSubJob[taskID]
		if !exists {
			continue
		}

		subJob, exists := job.SubJobs[subJobID]
		if !exists {
			continue
		}

		if pendingMap[subJob.GID] == nil {
			pendingMap[subJob.GID] = make(map[api.SubJobID]bool)
		}
		pendingMap[subJob.GID][subJobID] = true
	}

	// Count partitions per GID
	for gid, subJobs := range pendingMap {
		pendingByGid[gid] = len(subJobs)
	}

	return pendingByGid
}

func (ep *expectedPartitionsPlugin) getExpectedSubJobsReady(job *api.JobInfo, stmtList []*framework.Statement) map[api.SubJobGID]bool {
	result := make(map[api.SubJobGID]bool)

	// Collect all GIDs for this job that have ExpectedPartitions
	jobGIDs := make(map[api.SubJobGID]bool)
	for _, subJob := range job.SubJobs {
		if _, exists := ep.gidNextExpected[subJob.GID]; exists {
			jobGIDs[subJob.GID] = true
		}
	}

	// If no ExpectedPartitions configuration for this job, don't block allocation
	if len(jobGIDs) == 0 {
		klog.V(4).Infof("Job %s has no expected Partitions.", job.UID)
		return result
	}

	// Calculate newly allocated partitions from stmtList
	pendingMap := ep.getPendingFromStmtList(stmtList, job)

	// Check each GID using cached values
	for gid := range jobGIDs {
		nextExpected := ep.gidNextExpected[gid]
		currentAllocated := ep.gidAllocated[gid]
		pending := pendingMap[gid] // default is 0 if not exists

		targetAllocated := currentAllocated + pending

		if targetAllocated < nextExpected {
			result[gid] = false
		} else {
			result[gid] = true
		}
	}

	return result
}

// expectedJobReady returns whether all sub jobs are ready.
func (ep *expectedPartitionsPlugin) expectedJobReady(job *api.JobInfo, stmtList interface{}) bool {
	// Type assertion: convert interface{} to []*framework.Statement
	statements, ok := stmtList.([]*framework.Statement)
	if !ok {
		klog.Errorf("expectedJobReady: invalid stmtList type")
		return true
	}

	subJobsReady := ep.getExpectedSubJobsReady(job, statements)

	// If no expected partitions configured, return true
	if len(subJobsReady) == 0 {
		return true
	}

	// Check if all subgroups are ready
	for _, ready := range subJobsReady {
		if !ready {
			return false
		}
	}

	return true
}

// getStatementGIDs returns the set of SubJobGIDs that a statement belongs to.
// It extracts all tasks from the statement and finds their corresponding SubJob GIDs.
func getStatementGIDs(stmt *framework.Statement, job *api.JobInfo) map[api.SubJobGID]bool {
	gids := make(map[api.SubJobGID]bool)

	tasks := stmt.GetTasksFromAllocateOperations()
	for taskID := range tasks {
		subJobID, exists := job.TaskToSubJob[taskID]
		if !exists {
			continue
		}

		subJob, exists := job.SubJobs[subJobID]
		if !exists {
			continue
		}

		gids[subJob.GID] = true
	}

	return gids
}

// discardStatements filters out statements for SubJobs that haven't reached their expected targets.
// It checks which SubJob GIDs are not ready (would create intermediate state), then removes and discards
// statements belonging to those GIDs from the statement list to avoid meaningless intermediate states.
// For example: if expected=[1,2,4] and current=2, allocating only 1 more would result in 3,
// which is not in the expected list, so we discard those statements to keep it at 2.
// Returns the filtered statement list.
func (ep *expectedPartitionsPlugin) discardStatements(job *api.JobInfo, stmtList interface{}) interface{} {
	// Type assertion: convert interface{} to []*framework.Statement
	statements, ok := stmtList.([]*framework.Statement)
	if !ok {
		klog.Errorf("discardStatements: invalid stmtList type")
		return stmtList
	}

	// Get ready status for all SubJob GIDs
	subJobsReady := ep.getExpectedSubJobsReady(job, statements)

	// If no expected partitions configured, return all statements
	if len(subJobsReady) == 0 {
		return statements
	}

	// Filter statements
	var keptStmts []*framework.Statement
	var discardedStmts []*framework.Statement
	notReadyGIDs := make(map[api.SubJobGID]bool)

	for _, stmt := range statements {
		stmtGIDs := getStatementGIDs(stmt, job)
		shouldKeep := true

		// Check if this statement contains any GID that is not ready
		// If not ready (targetAllocated < nextExpected), we discard to avoid intermediate state
		for gid := range stmtGIDs {
			if ready, exists := subJobsReady[gid]; exists && !ready {
				shouldKeep = false
				notReadyGIDs[gid] = true
				break
			}
		}

		if shouldKeep {
			keptStmts = append(keptStmts, stmt)
		} else {
			discardedStmts = append(discardedStmts, stmt)
		}
	}

	// Log if any statements were discarded
	if len(discardedStmts) > 0 {
		klog.V(3).Infof("Job %s: discarded %d statements for not-ready GIDs %v to avoid intermediate state (kept: %d, total: %d)",
			job.UID, len(discardedStmts), notReadyGIDs, len(keptStmts), len(statements))
	}

	// Discard the filtered statements
	for _, stmt := range discardedStmts {
		stmt.Discard()
	}

	return keptStmts
}

// updateAllocationContext updates gidAllocated and gidNextExpected after resources have been allocated.
// This should be called after stmt.Commit() to reflect the new allocation state.
func (ep *expectedPartitionsPlugin) updateAllocationContext(job *api.JobInfo) {
	if ep.session == nil || job.PodGroup == nil {
		return
	}

	for _, policy := range job.PodGroup.Spec.SubGroupPolicy {
		gid := api.GetSubJobGID(job.UID, policy.Name)
		if len(policy.ExpectedSubGroups) == 0 {
			continue
		}

		// Recount how many SubJobs are ready for this GID
		current := 0
		for _, subJob := range job.SubJobs {
			if subJob.GID != gid {
				continue
			}
			if ep.session.SubJobReady(job, subJob) {
				current++
			}
		}

		oldAllocated := ep.gidAllocated[gid]
		ep.gidAllocated[gid] = current

		// Update nextExpected to the next target
		// Default to the last (maximum) expected value
		nextTarget := policy.ExpectedSubGroups[len(policy.ExpectedSubGroups)-1]
		for _, e := range policy.ExpectedSubGroups {
			if int(e) > current {
				nextTarget = e
				break
			}
		}
		ep.gidNextExpected[gid] = int(nextTarget)

		klog.V(3).Infof("Updated Job %s GID %s: allocated %d->%d, nextExpected=%d",
			job.UID, gid, oldAllocated, current, int(nextTarget))
	}
}

// jobAllocated is called after a job has been allocated resources and the statement has been committed.
// stmtList is interface{} to avoid circular dependency, actual type is []*framework.Statement
func (ep *expectedPartitionsPlugin) jobAllocated(job *api.JobInfo, stmtList interface{}) {
	ep.updateAllocationContext(job)
}
