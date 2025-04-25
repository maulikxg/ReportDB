package utils

import (
	"fmt"
	"github.com/spf13/viper"
	"golang.org/x/crypto/ssh"
	"io/ioutil"
	"log"
	"packxpoller/collector"
	"packxpoller/config"
	"time"
)

func InitConfig() (*config.Config, error) {

	viper.SetConfigName("config")

	viper.SetConfigType("yaml")

	viper.AddConfigPath(".")

	viper.AddConfigPath("./config")

	if err := viper.ReadInConfig(); err != nil {
		return nil, fmt.Errorf("error reading config file: %v", err)
	}

	var cfg config.Config

	if err := viper.Unmarshal(&cfg); err != nil {
		return nil, fmt.Errorf("unable to decode config: %v", err)
	}

	return &cfg, nil
}

func CreateSSHClient(device config.Device) (*ssh.Client, error) {

	var authMethods []ssh.AuthMethod

	if device.Password != "" {

		authMethods = append(authMethods, ssh.Password(device.Password))

	}

	if device.SSHKey != "" {

		key, err := ioutil.ReadFile(device.SSHKey)

		if err == nil {

			signer, err := ssh.ParsePrivateKey(key)

			if err == nil {
				authMethods = append(authMethods, ssh.PublicKeys(signer))
			}

		}
	}

	config := &ssh.ClientConfig{

		User: device.Username,

		Auth: authMethods,

		HostKeyCallback: ssh.InsecureIgnoreHostKey(),

		Timeout: time.Second * 10,
	}

	addr := fmt.Sprintf("%s:%d", device.Host, device.Port)

	return ssh.Dial("tcp", addr, config)
}

func LogMetrics(metrics *collector.Metrics) {
	log.Printf("=== Metrics for %s at %s ===", metrics.DeviceID, metrics.Timestamp.Format(time.RFC3339))

	// CPU Metrics
	log.Printf("CPU: Usage=%.2f%%", metrics.CPU.Usage)

	// Memory Metrics
	log.Printf("Memory: Used=%d MB", metrics.Memory.Used/1024/1024)

	log.Println("=== End of Metrics ===")
}
