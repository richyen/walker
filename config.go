package walker

import (
	"io/ioutil"

	"code.google.com/p/log4go"

	"gopkg.in/yaml.v1"
)

// Config is the configuration instance the rest of walker should access for
// global configuration values. See WalkerConfig for available config members.
var Config WalkerConfig

// ConfigName is the path (can be relative or absolute) to the config file that
// should be read.
var ConfigName string = "walker.yaml"

func init() {
	readConfig()
}

// WalkerConfig defines the available global configuration parameters for
// walker. It reads values straight from the config file (walker.yaml by
// default). See sample-walker.yaml for explanations and default values.
type WalkerConfig struct {
	AddNewDomains           bool     `yaml:"add_new_domains"`
	UserAgent               string   `yaml:"user_agent"`
	DefaultCrawlDelay       int      `yaml:"default_crawl_delay"`
	MaxHTTPContentSizeBytes int64    `yaml:"max_http_content_size_bytes"`
	IgnoreTags              []string `yaml:"ignore_tags"`
	MaxLinksPerPage         int      `yaml:"max_links_per_page"`

	// TODO: consider these config items
	// allowed schemes (file://, https://, etc.)
	// allowed return content types (or file extensions)
	// http timeout
	// http max delays (how many attempts to give a webserver that's reporting 'busy')
	// http content size limit
	// ftp content limit
	// ftp timeout
	// regex matchers for hosts, paths, etc. to include or exclude
	// max crawl delay (exclude or notify of sites that try to set a massive crawl delay)
	// max simultaneous fetches/crawls/segments
}

// SetDefaultConfig resets the Config object to default values, regardless of
// what was set by any configuration file.
func SetDefaultConfig() {
	Config.AddNewDomains = false
	Config.UserAgent = "Walker (http://github.com/iParadigms/walker)"
	Config.DefaultCrawlDelay = 1
	Config.MaxHTTPContentSizeBytes = 20 * 1024 * 1024 // 20MB
	Config.IgnoreTags = []string{"script", "img", "link"}
}

// ReadConfigFile sets a new path to find the walker yaml config file and
// forces a reload of the config.
func ReadConfigFile(path string) {
	ConfigName = path
	readConfig()
}

func readConfig() {
	SetDefaultConfig()

	data, err := ioutil.ReadFile(ConfigName)
	if err != nil {
		log4go.Error("Failed to read config file (%v): %v", ConfigName, err)
		return
	}
	err = yaml.Unmarshal(data, &Config)
	if err != nil {
		log4go.Error("Failed to unmarshal yaml from config file (%v): %v", ConfigName, err)
		return
	}
}
