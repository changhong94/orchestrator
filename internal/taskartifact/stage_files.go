package taskartifact

import "github.com/kuasar-sandbox/orchestrator/internal/configsock"

// StageFiles pins the file versions already checked by artifact preparation.
// These non-secret stamps remain in the privileged slot holder, not conductor.
func StageFiles(spec configsock.ArtifactPrepareSpec, result *Result) (any, error) {
	return preparedStamps(spec, result)
}
