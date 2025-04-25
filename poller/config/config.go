package config

type Device struct {
	Host     string
	Port     int
	Username string
	Password string
	SSHKey   string
}

type Config struct {
	Devices       []Device
	PollInterval  int    // in seconds
	BackendURL    string // URL to send metrics to
	MetricsToGet  []string
	SSHTimeout    int // in seconds
	RetryAttempts int
}

var DefaultConfig = Config{
	PollInterval:  30,
	SSHTimeout:    10,
	RetryAttempts: 3,
	MetricsToGet: []string{
		"cpu",
		"memory",
		"disk",
		"network",
		"processes",
	},
} 