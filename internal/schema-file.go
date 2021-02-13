package internal

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

type SchemaFile struct {
	APIVersion string `yaml:"apiVersion"`
	Topics map[string]struct{
		Schemas struct{
			Key	string `yaml:"key"`
			Value string `yaml:"value"`
		} `yaml:"schemas"`
	} `yaml:"topics"`
}

func ParseSchemaFile(fileName string) (*SchemaFile, error) {

	data, err := ioutil.ReadFile(fileName)

	if err != nil {
		return nil, err
	}

	schemaFile := SchemaFile{}
	err = yaml.Unmarshal(data, &schemaFile)

	if err != nil {
		return nil, err
	}

	return &schemaFile, nil
}
