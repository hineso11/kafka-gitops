package internal

import (
	"gopkg.in/yaml.v2"
	"io/ioutil"
)

type KafkaFile struct {
	APIVersion string `yaml:"apiVersion"`
	Topics map[string]struct{
		Schemas struct{
			Key	string `yaml:"key"`
			Value string `yaml:"value"`
		} `yaml:"schemas"`
	} `yaml:"topics"`
}

func ParseKafkaFile(fileName string) (*KafkaFile, error) {

	data, err := ioutil.ReadFile(fileName)

	if err != nil {
		return nil, err
	}

	kafkaFile := KafkaFile{}
	err = yaml.Unmarshal(data, &kafkaFile)

	if err != nil {
		return nil, err
	}

	return &kafkaFile, nil
}
