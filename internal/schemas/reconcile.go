package schemas

import (
	"encoding/json"
	"errors"
	"github.com/hineso11/go-schema-registry/pkg/api/client"
	"github.com/hineso11/go-schema-registry/pkg/api/client/operations"
	"github.com/hineso11/go-schema-registry/pkg/api/models"
	"github.com/hineso11/kafka-gitops/internal"
	"io/ioutil"
	"path/filepath"
	"reflect"
	"strings"
)

type SchemaType	string

const (
	AvroType = SchemaType("AVRO")
	ProtobufType = SchemaType("PROTOBUF")
	JsonType = SchemaType("JSON")
)

type Subject struct {
	Name       string
	SchemaType SchemaType
	Schema     string
}

func ReconcileSchemas(file *internal.KafkaFile, schemaRegistryClient *client.ConfluentSchemaRegistry, dryRun bool, relativePath string) ([]internal.ReconcileAction, error) {

	desiredSubjects, err := getSubjects(file, relativePath)

	if err != nil {
		return nil, err
	}

	existingSubjectNames, err := getExistingSubjectNames(schemaRegistryClient)

	if err != nil {
		return nil, err
	}

	var reconcileActions []internal.ReconcileAction

	for _, subject := range desiredSubjects {

		// Check if the subject exists already or not
		if _, exists := existingSubjectNames[subject.Name]; !exists {
			// If the subject does not exist, then create it

			if !dryRun {
				err = registerNewSchema(schemaRegistryClient, subject.Name, subject.Schema, subject.SchemaType)

				if err != nil {
					return nil, err
				}
			}

			reconcileActions = append(reconcileActions, internal.ReconcileAction{
				Type:    internal.CreateActionType,
				Subject: subject.Name,
			})

			continue
		}

		latestSchema, err := getLatestSchema(schemaRegistryClient, subject.Name)

		if err != nil {
			return nil, err
		}

		// Check if the schema is the same as the latest
		schemasEqual, err := schemasAreEqual(latestSchema, subject.Schema, subject.SchemaType)

		if err != nil {
			return nil, err
		}

		if schemasEqual {
			
			reconcileActions = append(reconcileActions, internal.ReconcileAction{
				Type:    internal.NoopActionType,
				Subject: subject.Name,
			})
			
			continue
		}

		// Check if new schema is compatible with subject
		isCompatible, err := checkSchemaIsCompatible(schemaRegistryClient, subject.Name, subject.Schema, subject.SchemaType)

		if err != nil {
			return nil, err
		}

		if !isCompatible {
			return nil, errors.New("subject " + subject.Name + " is not compatible with latest schema")
		}

		// Schema is compatible, so update it
		if !dryRun {
			err = registerNewSchema(schemaRegistryClient, subject.Name, subject.Schema, subject.SchemaType)

			if err != nil {
				return nil, err
			}
		}
		
		reconcileActions = append(reconcileActions, internal.ReconcileAction{
			Type:    internal.UpdateActionType,
			Subject: subject.Name,
		})
	}

	return reconcileActions, nil
}

func schemasAreEqual(schema1 string, schema2 string, schemaType SchemaType) (bool, error) {

	if schemaType == JsonType || schemaType == AvroType {

		var parsedSchema1 interface{}
		var parsedSchema2 interface{}

		var err error
		err = json.Unmarshal([]byte(schema1), &parsedSchema1)
		if err != nil {
			return false, err
		}
		err = json.Unmarshal([]byte(schema2), &parsedSchema2)
		if err != nil {
			return false, err
		}

		return reflect.DeepEqual(parsedSchema1, parsedSchema2), nil
	}

	return strings.TrimSpace(schema1) == strings.TrimSpace(schema2), nil
}

func getSubjects(file *internal.KafkaFile, relativePath string) ([]Subject, error) {

	var subjects []Subject

	for topicName, topic := range file.Topics {

		keySchemaPath := filepath.Join(relativePath, topic.Schemas.Key)
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
			Name:       topicName + "-key",
			SchemaType: keySchemaType,
			Schema:     strings.TrimSpace(string(keySchema)),
		})

		valueSchemaPath := filepath.Join(relativePath, topic.Schemas.Value)
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
			Name:       topicName + "-value",
			SchemaType: valueSchemaType,
			Schema:     string(valueSchema),
		})
	}

	return subjects, nil
}

func getExistingSubjectNames(schemaRegistryClient *client.ConfluentSchemaRegistry) (map[string]bool, error) {

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