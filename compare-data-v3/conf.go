package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/openark/golib/log"
)

// Configuration makes for orchestrator configuration input, which can be provided by user via JSON formatted file.
// Some of the parameteres have reasonable default values, and some (like database credentials) are
// strictly expected from user.
type Configuration struct {
	SourceMySQLHost         string
	SourceMySQLPort         int
	SourceMySQLDB           string
	SourceMySQLUser         string
	SourceMySQLUserPassword string
	TargetMySQLHost         string
	TargetMySQLPort         int
	TargetMySQLDB           string
	TargetMySQLUser         string
	TargetMySQLUserPassword string
	ChecksumHost            string
	ChecksumPort            int
	ChecksumDB              string
	ChecksumUser            string
	ChecksumPassword        string
	CheckTables             string
}

// ToJSONString will marshal this configuration as JSON
func (this *Configuration) ToJSONString() string {
	b, _ := json.Marshal(this)
	return string(b)
}

// Config is *the* configuration instance, used globally to get configuration data
var Config = newConfiguration()
var readFileNames []string

func newConfiguration() *Configuration {
	return &Configuration{
		SourceMySQLHost:         "",
		SourceMySQLPort:         0,
		SourceMySQLDB:           "",
		SourceMySQLUser:         "",
		SourceMySQLUserPassword: "",
		TargetMySQLHost:         "",
		TargetMySQLPort:         0,
		TargetMySQLDB:           "",
		TargetMySQLUser:         "",
		TargetMySQLUserPassword: "",
		ChecksumHost:            "",
		ChecksumPort:            0,
		ChecksumDB:              "",
		ChecksumUser:            "",
		ChecksumPassword:        "",
		CheckTables:             "",
	}
}

// read reads configuration from given file, or silently skips if the file does not exist.
// If the file does exist, then it is expected to be in valid JSON format or the function bails out.
func read(fileName string) (*Configuration, error) {
	if fileName == "" {
		return Config, fmt.Errorf("Empty file name")
	}
	file, err := os.Open(fileName)
	if err != nil {
		return Config, err
	}
	decoder := json.NewDecoder(file)
	err = decoder.Decode(Config)
	if err == nil {
		log.Infof("Read config: %s", fileName)
	} else {
		log.Fatal("Cannot read config file:", fileName, err)
	}

	return Config, err
}

// Read reads configuration from zero, either, some or all given files, in order of input.
// A file can override configuration provided in previous file.
func Read(fileNames ...string) *Configuration {
	for _, fileName := range fileNames {
		read(fileName)
	}
	readFileNames = fileNames
	return Config
}

// ForceRead reads configuration from given file name or bails out if it fails
func ForceRead(fileName string) *Configuration {
	_, err := read(fileName)
	if err != nil {
		log.Fatal("Cannot read config file:", fileName, err)
	}
	readFileNames = []string{fileName}
	return Config
}

// Reload re-reads configuration from last used files
func Reload(extraFileNames ...string) *Configuration {
	for _, fileName := range readFileNames {
		read(fileName)
	}
	for _, fileName := range extraFileNames {
		read(fileName)
	}
	return Config
}
