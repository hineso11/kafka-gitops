package internal

import (
	"errors"
	"fmt"
	"github.com/hineso11/go-schema-registry/pkg/api/client"
	"github.com/hineso11/go-schema-registry/pkg/api/client/operations"
	"github.com/hineso11/go-schema-registry/pkg/api/models"
	"io/ioutil"
	"path/filepath"
	"strings"
)

type ReconcileActionType string
type SchemaType	string

const (
	CreateAction = ReconcileActionType("CREATE")
	UpdateAction = ReconcileActionType("UPDATE")
	NoopAction = ReconcileActionType("NOOP")

	AvroType = SchemaType("AVRO")
	ProtobufType = SchemaType("PROTOBUF")
	JsonType = SchemaType("JSON")
)

type ReconcileAction struct {
	Type ReconcileActionType
	executed bool
}

type Subject struct {
	Name string
	SchemaType SchemaType
	Schema string
}

func ReconcileSchemaFile(file *SchemaFile, schemaRegistryClient *client.ConfluentSchemaRegistry, dryRun bool, relativePath string) error {

	desiredSubjects, err := getSubjects(file, relativePath)

	if err != nil {
		return err
	}

	existingSubjectNames, err := subjectNamesMap(schemaRegistryClient)

	if err != nil {
		return err
	}

	for _, subject := range desiredSubjects {

		// Check if the subject exists already or not
		if _, exists := existingSubjectNames[subject.Name]; !exists {
			// If the subject does not exist, then create it
			fmt.Println("subject " + subject.Name + " does not exist, creating it...")

			err = registerNewSchema(schemaRegistryClient, subject.Name, subject.Schema, subject.SchemaType)

			if err != nil {
				return err
			}

			continue
		}

		// Check if the schema is the same as the latest
		latestSchema, err := getLatestSchema(schemaRegistryClient, subject.Name)

		if err != nil {
			return err
		}

		if latestSchema == subject.Schema {
			fmt.Println("subject " + subject.Name + " is the same as before, noop necessary")
			continue
		}

		// Check if new schema is compatible with subject
		isCompatible, err := checkSchemaIsCompatible(schemaRegistryClient, subject.Name, subject.Schema, subject.SchemaType)

		if err != nil {
			return err
		}

		if !isCompatible {
			return errors.New("subject " + subject.Name + " is not compatible with latest schema")
		}

		// Schema is compatible, so update it
		fmt.Println("subject " + subject.Name + " is compatible, will update schema")
		err = registerNewSchema(schemaRegistryClient, subject.Name, subject.Schema, subject.SchemaType)

		if err != nil {
			return err
		}
	}

	return nil
}

func getSubjects(file *SchemaFile, relativePath string) ([]Subject, error) {

	var subjects []Subject

	for _, topic := range file.Topics {

		keySchemaPath := filepath.Join(relativePath, topic.Key)
		keyFileExtension := filepath.Ext(keySchemaPath)

		var keySchemaType SchemaType
		switch keyFileExtension {
		case ".proto":
			keySchemaType = ProtobufType
		case ".avsc":
			keySchemaType = AvroType
		case ".json":
			keySchemaType = JsonType
		default:
			return nil, errors.New("schema file extension must be one of .avro, .json or .proto")
		}

		// Read in the specified schema
		keySchema, err := ioutil.ReadFile(keySchemaPath)

		if err != nil {
			return nil, err
		}

		subjects = append(subjects, Subject{
			Name:       topic.Name + "-key",
			SchemaType: keySchemaType,
			Schema:     strings.TrimSpace(string(keySchema)),
		})

		valueSchemaPath := filepath.Join(relativePath, topic.Value)
		valueFileExtension := filepath.Ext(valueSchemaPath)

		var valueSchemaType SchemaType
		switch valueFileExtension {
		case ".proto":
			valueSchemaType = ProtobufType
		case ".avsc":
			valueSchemaType = AvroType
		case ".json":
			valueSchemaType = JsonType
		default:
			return nil, errors.New("schema file extension must be one of .avro, .json or .proto")
		}

		// Read in the specified schema
		valueSchema, err := ioutil.ReadFile(valueSchemaPath)

		if err != nil {
			return nil, err
		}

		subjects = append(subjects, Subject{
			Name:       topic.Name + "-value",
			SchemaType: valueSchemaType,
			Schema:     string(valueSchema),
		})
	}

	return subjects, nil
}

func subjectNamesMap(schemaRegistryClient *client.ConfluentSchemaRegistry) (map[string]bool, error) {

	res, err := schemaRegistryClient.Operations.List(operations.NewListParams())

	if err != nil {
		return nil, err
	}

	subjectsMap := make(map[string]bool)
	for _, subjectName := range res.Payload {
		subjectsMap[subjectName] = true
	}

	return subjectsMap, nil
}

func getLatestSchema(schemaRegistryClient *client.ConfluentSchemaRegistry, subject string) (string, error) {

	params := operations.NewGetSchemaByVersionParams()
	params.SetSubject(subject)
	params.SetVersion("latest")

	res, err := schemaRegistryClient.Operations.GetSchemaByVersion(params)

	if err != nil {
		return "", err
	}

	return res.Payload.Schema, nil
}

func checkSchemaIsCompatible(schemaRegistryClient *client.ConfluentSchemaRegistry, subject string, schema string, schemaType SchemaType) (bool, error) {

	req := models.RegisterSchemaRequest{
		Schema:     schema,
		SchemaType: string(schemaType),
	}

	params := operations.NewTestCompatibilityBySubjectNameParams()
	params.SetSubject(subject)
	params.SetVersion("latest")
	params.SetBody(&req)

	res, err := schemaRegistryClient.Operations.TestCompatibilityBySubjectName(params)

	if err != nil {
		return false, err
	}

	return res.Payload.IsCompatible, nil
}

func registerNewSchema(schemaRegistryClient *client.ConfluentSchemaRegistry, name string, schema string, schemaType SchemaType) error {

	req := models.RegisterSchemaRequest{
		Schema:     schema,
		SchemaType: string(schemaType),
	}

	params := operations.NewRegisterParams()
	params.SetBody(&req)
	params.SetSubject(name)

	_, err := schemaRegistryClient.Operations.Register(params)

	if err != nil {
		return err
	}

	return nil
}