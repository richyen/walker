package test

import (
	"path"
	"runtime"
	"testing"

	"github.com/iParadigms/walker"
)

func init() {
	// Tests outside of config_test.go also require this configuration to be
	// loaded; Config tests should reset it by making this call
	loadTestConfig("test-walker.yaml")
}

// loadTestConfig loads the given test config yaml file. The given path is
// assumed to be relative to the `walker/test/` directory, the location of this
// test file.
func loadTestConfig(filename string) {
	_, thisname, _, ok := runtime.Caller(0)
	if !ok {
		panic("Failed to get location of test source file")
	}
	walker.ReadConfigFile(path.Join(path.Dir(thisname), filename))
}

func TestConfigLoading(t *testing.T) {
	walker.Config.UserAgent = "Test Agent (set inline)"
	walker.SetDefaultConfig()
	expectedAgentInline := "Walker (http://github.com/iParadigms/walker)"
	if walker.Config.UserAgent != expectedAgentInline {
		t.Errorf("Failed to reset default config value (user_agent), expected: %v\nBut got: %v",
			expectedAgentInline, walker.Config.UserAgent)
	}
	walker.ReadConfigFile("test-walker2.yaml")
	expectedAgentYaml := "Test Agent (set in yaml)"
	if walker.Config.UserAgent != expectedAgentYaml {
		t.Errorf("Failed to set config value (user_agent) via yaml, expected: %v\nBut got: %v",
			expectedAgentYaml, walker.Config.UserAgent)
	}

	// Need this to reset config needed for the rest of the tests
	loadTestConfig("test-walker.yaml")
}
