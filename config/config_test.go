package config

import (
	"fmt"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
)

var dummyInstanceID = "i-11111111111111111"

func TestInitializeConfig_FileNotExist(t *testing.T) {
	_, err := InitalizeConfig(dummyInstanceID, []string{"non-exist-path"})
	assert.Error(t, err)
}

func TestInitializeConfig_FilePermissionError(t *testing.T) {
	t.Skip("skip for now because the codebuild environment seems ignoring the file mode.")
	f, err := os.CreateTemp("", "*.toml")
	assert.NoError(t, err)
	defer os.Remove(f.Name())
	err = os.Chmod(f.Name(), 0000)
	assert.NoError(t, err)

	_, err = InitalizeConfig(dummyInstanceID, []string{f.Name()})
	assert.Error(t, err)
}

func TestInitializeConfig_NoFile(t *testing.T) {
	c, err := InitalizeConfig(dummyInstanceID, nil)
	assert.NoError(t, err)
	assert.Equal(t, DefaultLogGroup, c.LogGroup)
	assert.Equal(t, dummyInstanceID, c.LogStream)
	assert.Equal(t, DefaultStateFile, c.StateFile)
}

func TestInitializeConfig_FileOK(t *testing.T) {
	cases := []struct {
		name           string
		fileContent    string
		expectedConfig *Config
	}{
		{
			name:        "emepty file",
			fileContent: "",
			expectedConfig: &Config{
				LogGroup:  DefaultLogGroup,
				LogStream: dummyInstanceID,
				StateFile: DefaultStateFile,
			},
		},
		{
			name: "with customized value",
			fileContent: `
				log_group = "log-group-1"
				log_stream = "log-stream-1"
				state_file = "/dir-1/state-file-1"
				other_field = "other_value"`,
			expectedConfig: &Config{
				LogGroup:  "log-group-1",
				LogStream: "log-stream-1",
				StateFile: "/dir-1/state-file-1",
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f, err := os.CreateTemp("", "*.toml")
			assert.NoError(t, err)
			defer os.Remove(f.Name())
			_, err = fmt.Fprintf(f, "%s\n", tc.fileContent)
			assert.NoError(t, err)
			c, err := InitalizeConfig(dummyInstanceID, []string{f.Name()})
			assert.NoError(t, err)
			assert.Equal(t, *tc.expectedConfig, *c)
		})
	}
}
