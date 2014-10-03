package walker

import (
	"io/ioutil"
	"strings"

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
	AddedDomainsCacheSize   int      `yaml:"added_domains_cache_size"`
	MaxDNSCacheEntries      int      `yaml:"max_dns_cache_entries"`
	UserAgent               string   `yaml:"user_agent"`
	AcceptFormats           []string `yaml:"accept_formats"`
	DefaultCrawlDelay       int      `yaml:"default_crawl_delay"`
	MaxHTTPContentSizeBytes int64    `yaml:"max_http_content_size_bytes"`
	IgnoreTags              []string `yaml:"ignore_tags"`
	//TODO: allow -1 as a no max value
	MaxLinksPerPage         int  `yaml:"max_links_per_page"`
	NumSimultaneousFetchers int  `yaml:"num_simultaneous_fetchers"`
	BlacklistPrivateIPs     bool `yaml:"blacklist_private_ips"`

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

	Cassandra struct {
		Hosts             []string `yaml:"hosts"`
		Keyspace          string   `yaml:"keyspace"`
		ReplicationFactor int      `yaml:"replication_factor"`

		//TODO: Currently only exposing values needed for testing; should expose more?
		//CQLVersion       string
		//ProtoVersion     int
		//Timeout          time.Duration
		//Port             int
		//NumConns         int
		//NumStreams       int
		//Consistency      Consistency
		//Compressor       Compressor
		//Authenticator    Authenticator
		//RetryPolicy      RetryPolicy
		//SocketKeepalive  time.Duration
		//ConnPoolType     NewPoolFunc
		//DiscoverHosts    bool
		//MaxPreparedStmts int
		//Discovery        DiscoveryConfig
	} `yaml:"cassandra"`
}

// SetDefaultConfig resets the Config object to default values, regardless of
// what was set by any configuration file.
func SetDefaultConfig() {
	Config.AddNewDomains = false
	Config.AddedDomainsCacheSize = 20000
	Config.MaxDNSCacheEntries = 20000
	Config.UserAgent = "Walker (http://github.com/iParadigms/walker)"
	Config.AcceptFormats = []string{"text/html", "text/*;"} //NOTE you can add quality factors by doing "text/html; q=0.4"
	Config.DefaultCrawlDelay = 1
	Config.MaxHTTPContentSizeBytes = 20 * 1024 * 1024 // 20MB
	Config.IgnoreTags = []string{"script", "img", "link"}
	Config.MaxLinksPerPage = 1000
	Config.NumSimultaneousFetchers = 10
	Config.BlacklistPrivateIPs = true
	Config.Cassandra.Hosts = []string{"localhost"}
	Config.Cassandra.Keyspace = "walker"
	Config.Cassandra.ReplicationFactor = 3
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
		if strings.Contains(err.Error(), "no such file or directory") {
			log4go.Info("Did not find config file %v, continuing with defaults", ConfigName)
		} else {
			log4go.Error("Failed to read config file (%v): %v", ConfigName, err)
		}
		return
	}
	err = yaml.Unmarshal(data, &Config)
	if err != nil {
		log4go.Error("Failed to unmarshal yaml from config file (%v): %v", ConfigName, err)
		return
	}
	log4go.Info("Loaded config file %v", ConfigName)
}
