package utils

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"os"
	"time"
)

var Logger *zap.Logger

func InitLogger() error {

	logDir := "./logs/"

	if _, err := os.Stat(logDir); os.IsNotExist(err) {

		// Only create if it doesn't exist
		if err := os.Mkdir(logDir, os.ModePerm); err != nil {

			return err

		}
	}

	IsProductionEnvironment := isProductionEnvironment()

	if IsProductionEnvironment {

		productionConfig := zap.NewProductionConfig()

		productionConfig.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

		productionConfig.Level.SetLevel(zapcore.ErrorLevel)

		productionConfig.OutputPaths = []string{

			"./logs/production_" + time.Now().Format("2006_01_02") + ".log",
		}

		Logger = zap.Must(productionConfig.Build())

	} else {

		developmentConfig := zap.NewDevelopmentConfig()

		developmentConfig.EncoderConfig.EncodeTime = zapcore.ISO8601TimeEncoder

		developmentConfig.Level.SetLevel(zapcore.DebugLevel)

		developmentConfig.OutputPaths = []string{

			"./logs/development_" + time.Now().Format("2006_01_02") + ".log",
		}

		Logger = zap.Must(developmentConfig.Build())

	}

	return nil

}
