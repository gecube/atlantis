// Copyright 2025 The Atlantis Authors
// SPDX-License-Identifier: Apache-2.0

package gitlab

import (
	"fmt"
	"sync"
	"time"

	"github.com/runatlantis/atlantis/server/events/models"
)

// pipelineCacheTTL bounds how long UpdateStatus reuses the pipeline it found for
// a commit. One command updates several statuses of the same commit within
// seconds (pre-workflow hooks, plan, each project, apply, policy check), and
// every lookup is a GetCommit request. A newer pipeline for the same commit (a
// re-run) is picked up once the entry expires.
const pipelineCacheTTL = time.Minute

// pipelineCache remembers the CI pipeline that UpdateStatus attached a commit
// status to, per merge request and head commit. The zero value is ready to use.
type pipelineCache struct {
	mu      sync.Mutex
	entries map[string]pipelineCacheEntry
	// now returns the current time; time.Now when nil (set in tests).
	now func() time.Time
}

type pipelineCacheEntry struct {
	id      int
	expires time.Time
}

func pipelineCacheKey(repo models.Repo, pull models.PullRequest) string {
	return fmt.Sprintf("%s!%d %s@%s", repo.FullName, pull.Num, pull.HeadBranch, pull.HeadCommit)
}

func (c *pipelineCache) clock() time.Time {
	if c.now != nil {
		return c.now()
	}
	return time.Now()
}

// get returns the pipeline ID stored for key if it has not expired.
func (c *pipelineCache) get(key string) (int, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[key]
	if !ok {
		return 0, false
	}
	if !c.clock().Before(e.expires) {
		delete(c.entries, key)
		return 0, false
	}
	return e.id, true
}

// put stores the pipeline ID for key and drops the expired entries.
func (c *pipelineCache) put(key string, id int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	now := c.clock()
	if c.entries == nil {
		c.entries = make(map[string]pipelineCacheEntry)
	}
	for k, e := range c.entries {
		if !now.Before(e.expires) {
			delete(c.entries, k)
		}
	}
	c.entries[key] = pipelineCacheEntry{id: id, expires: now.Add(pipelineCacheTTL)}
}

// drop forgets the pipeline stored for key.
func (c *pipelineCache) drop(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, key)
}
