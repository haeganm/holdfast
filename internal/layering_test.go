package internal_test

// docs/DESIGN.md: dependencies point inward. The clock knows nothing of the
// map, the map nothing of the network, and the node nothing of where reports
// come from or where it runs. Outside cloud/, only the standard library is used,
// so the edge binary has no third-party code. This test holds the code to that.

import (
	"go/build"
	"os"
	"path"
	"slices"
	"strings"
	"testing"
)

const module = "github.com/haeganm/holdfast/"

// Each package and the project packages it may import, relative to the module.
var layers = map[string][]string{
	"internal/hlc":    nil,
	"internal/crdt":   {"internal/hlc"},
	"internal/node":   {"internal/crdt", "internal/hlc"},
	"internal/replay": nil,
	"internal/adsb":   {"internal/crdt"},
	"cmd/holdfast":    {"internal/adsb", "internal/crdt", "internal/node", "internal/replay"},
	"cmd/gentrace":    {"internal/adsb", "internal/replay"},
	"cloud":           {"internal/crdt", "internal/node"},
}

func TestDependenciesPointInward(t *testing.T) {
	// Every package directory has to be in the table, so a new one can't skip the check.
	dirs := []string{"cloud"}
	for _, root := range []string{"internal", "cmd"} {
		entries, err := os.ReadDir("../" + root)
		if err != nil {
			t.Fatal(err)
		}
		for _, e := range entries {
			if e.IsDir() {
				dirs = append(dirs, path.Join(root, e.Name()))
			}
		}
	}
	for _, dir := range dirs {
		allowed, ok := layers[dir]
		if !ok {
			t.Errorf("%s is not in the layering table", dir)
			continue
		}
		pkg, err := build.ImportDir("../"+dir, 0)
		if err != nil {
			t.Fatalf("%s: %v", dir, err)
		}
		for _, imp := range pkg.Imports {
			if rel, ours := strings.CutPrefix(imp, module); ours {
				if !slices.Contains(allowed, rel) {
					t.Errorf("%s imports %s, which points the wrong way", dir, rel)
				}
				continue
			}
			thirdParty := strings.Contains(strings.SplitN(imp, "/", 2)[0], ".")
			if thirdParty && !(dir == "cloud" && strings.HasPrefix(imp, "github.com/aws/")) {
				t.Errorf("%s imports %s; outside cloud/'s AWS SDK, only the standard library is allowed", dir, imp)
			}
		}
	}
}
