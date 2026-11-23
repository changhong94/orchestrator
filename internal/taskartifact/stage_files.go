package taskartifact

import (
	"github.com/kuasar-sandbox/orchestrator/internal/configsock"
	"github.com/kuasar-sandbox/sandboxer/pkg/restore"
)

// StageFiles pins the file versions already checked by artifact preparation.
// The typed copy has the same fields as the legacy JSON wire representation.
func StageFiles(spec configsock.ArtifactPrepareSpec, result *Result) (map[string]restore.StageFileVersion, error) {
	stamps, err := preparedStamps(spec, result)
	if err != nil {
		return nil, err
	}
	out := make(map[string]restore.StageFileVersion, len(stamps))
	for path, stamp := range stamps {
		out[path] = restore.StageFileVersion(stamp)
	}
	return out, nil
}
