package config

type Device struct {
	Host     string
	Port     int
	Username string
	Password string
	SSHKey   string
}

type ZMQConfig struct {
	BackendMetrics string `yaml:"backend_metrics"`
	ConfigEndpoint string `yaml:"config_endpoint"`
}

type Config struct {
	ZMQ           ZMQConfig `yaml:"zmq"`
	Devices       []Device
	PollInterval  int    `yaml:"pollinterval"` // in seconds
	BackendURL    string `yaml:"backendurl"`   // URL to send metrics to
	MetricsToGet  []string
	SSHTimeout    int `yaml:"sshtimeout"`    // in seconds
	RetryAttempts int `yaml:"retryattempts"`
}

var DefaultConfig = Config{
	PollInterval:  3,
	SSHTimeout:    10,
	RetryAttempts: 3,
	MetricsToGet: []string{
		"cpu",
		"memory",
	},
	ZMQ: ZMQConfig{
		BackendMetrics: "tcp://localhost:5556",
		ConfigEndpoint: "tcp://*:5557",
	},
}
