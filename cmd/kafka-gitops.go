package main

import (
	"flag"
	"fmt"
	"github.com/hineso11/go-schema-registry/pkg/api/client"
	"github.com/hineso11/kafka-gitops/internal"
	"github.com/hineso11/kafka-gitops/internal/schemas"
	"path/filepath"
)

const (
	colourReset = "\033[0m"
	colorGreen = "\033[32m"
	colorBlue = "\033[34m"
	colorCyan = "\033[36m"
)

func main() {

	kafkaFilePath := flag.String(
		"kafka-file-path",
		"kafka.yaml",
		"The path to your kafka file")
	schemaRegistryUrl := flag.String(
		"schema-registry-url",
		"localhost",
		"The URL where your Confluent Schema Registry is hosted")
	schemaRegistryApiKey := flag.String(
		"schema-registry-api-key",
		"",
		"The API key used to authenticate with your Confluent Schema Registry")
	schemaRegistryApiSecret := flag.String(
		"schema-registry-api-secret",
		"",
		"The API secret used to authenticate with your Confluent Schema Registry")

	dryRun := flag.Bool(
		"dry-run",
		false,
		"Set as true to check compatibility but not apply changes")

	flag.Parse()

	kafkaFile, err := internal.ParseKafkaFile(*kafkaFilePath)

	if err != nil {
		panic(err)
	}

	kafkaFileDirectoryPath, err := filepath.Abs(filepath.Dir(*kafkaFilePath))

	if err != nil {
		panic(err)
	}

	var schemaRegistryClient *client.ConfluentSchemaRegistry
	if *schemaRegistryApiKey != "" && *schemaRegistryApiSecret != "" {
		schemaRegistryClient, err = schemas.NewSchemaRegistryClientWithBasicAuth(
			*schemaRegistryUrl,
			*schemaRegistryApiKey,
			*schemaRegistryApiSecret)
	} else {
		schemaRegistryClient, err = schemas.NewSchemaRegistryClient(*schemaRegistryUrl)
	}

	if err != nil {
		panic(err)
	}

	if !(*dryRun) {
		fmt.Println("Executing Kafka-GitOps in active mode, changes may be made.")
	} else {
		fmt.Println("Executing Kafka-GitOps in dry-run mode, no changes will be made.")
	}

	actions, err := schemas.ReconcileSchemas(kafkaFile, schemaRegistryClient, *dryRun, kafkaFileDirectoryPath)

	if err != nil {
		panic(err)
	}

	fmt.Println("Took the following actions on schemas:")

	outputActions(actions)
}

func outputActions(actions []internal.ReconcileAction) {

	for _, action := range actions {

		switch action.Type {
		case internal.UpdateActionType:
			fmt.Print(colorBlue)
		case internal.CreateActionType:
			fmt.Print(colorGreen)
		case internal.NoopActionType:
			fmt.Print(colorCyan)
		}

		fmt.Print(string(action.Type), " ", action.Subject)
		fmt.Println(colourReset)
	}
}