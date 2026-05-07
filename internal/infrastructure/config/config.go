package config

import (
	"log"
	"os"

	"github.com/joho/godotenv"
)

type Config struct {
	RabbitMQ RabbitMQConfig
}

type RabbitMQConfig struct {
	URL string
}

func LoadConfig() *Config {

	err := godotenv.Load()
	if err != nil {
		log.Println(".env file not found, using environment variables")
	}

	return &Config{
		RabbitMQ: RabbitMQConfig{
			URL: getEnv(
				"RABBITMQ_URL",
				"amqp://guest:guest@localhost:5672/",
			),
		},
	}
}

func getEnv(key, fallback string) string {

	value := os.Getenv(key)

	if value == "" {
		return fallback
	}

	return value
}
