package sync

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"sort"

	"github.com/go-kit/kit/log"
	"github.com/pkg/errors"

	"github.com/weaveworks/flux/cluster"
	"github.com/weaveworks/flux/policy"
	"github.com/weaveworks/flux/resource"
)

// Checksum generates a unique identifier for all apply actions in the stack
func getStackChecksum(repoResources map[string]resource.Resource) string {
	checksum := sha1.New()

	sortedKeys := make([]string, 0, len(repoResources))
	for resourceID := range repoResources {
		sortedKeys = append(sortedKeys, resourceID)
	}
	sort.Strings(sortedKeys)
	for resourceIDIndex := range sortedKeys {
		checksum.Write(repoResources[sortedKeys[resourceIDIndex]].Bytes())
	}
	return hex.EncodeToString(checksum.Sum(nil))
}

// Sync synchronises the cluster to the files under a directory.
func Sync(logger log.Logger, m cluster.Manifests, repoResources map[string]resource.Resource, clus cluster.Cluster, tracks, deletes bool) error {
	// Get a map of resources defined in the cluster
	clusterBytes, err := clus.Export()

	if err != nil {
		return errors.Wrap(err, "exporting resource defs from cluster")
	}
	clusterResources, err := m.ParseManifests(clusterBytes)
	if err != nil {
		return errors.Wrap(err, "parsing exported resources")
	}

	sync := cluster.SyncDef{}

	stacks := map[string]string{}
	checksums := map[string]string{}
	if tracks {
		stackName := "default" // TODO: multiple stack support
		stackChecksum := getStackChecksum(repoResources)

		fmt.Printf("[stack-tracking] stack=%s, checksum=%s\n", stackName, stackChecksum)

		for id := range repoResources {
			fmt.Printf("[stack-tracking] resource=%s, applying checksum=%s\n", id, stackChecksum)
			stacks[id] = stackName
			checksums[id] = stackChecksum
		}
	}

	for id, res := range repoResources {
		prepareSyncApply(logger, clusterResources, id, res, &sync)
	}

	if err := clus.Sync(sync, stacks, checksums); err != nil {
		return err
	}
	return nil
}

func prepareSyncApply(logger log.Logger, clusterResources map[string]resource.Resource, id string, res resource.Resource, sync *cluster.SyncDef) {
	if res.Policy().Has(policy.Ignore) {
		logger.Log("resource", res.ResourceID(), "ignore", "apply")
		return
	}
	if cres, ok := clusterResources[id]; ok {
		if cres.Policy().Has(policy.Ignore) {
			logger.Log("resource", res.ResourceID(), "ignore", "apply")
			return
		}
	}
	sync.Actions = append(sync.Actions, cluster.SyncAction{
		Apply: res,
	})
}
