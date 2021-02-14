package schemas

import (
	"context"
	"github.com/go-openapi/runtime"
	httpTransport "github.com/go-openapi/runtime/client"
	"github.com/go-openapi/strfmt"
	"github.com/hineso11/go-schema-registry/pkg/api/client"
	"net/url"
)

func NewSchemaRegistryClientWithBasicAuth(schemaRegistryUrl string, apiKey string, apiSecret string) (*client.ConfluentSchemaRegistry, error) {

	parsedUrl, err := url.Parse(schemaRegistryUrl)

	if err != nil {
		return nil, err
	}

	schemes := []string{parsedUrl.Scheme}

	transport := httpTransport.New(parsedUrl.Host, parsedUrl.Path, schemes)
	transport.DefaultAuthentication = httpTransport.BasicAuth(apiKey, apiSecret)
	transport.Producers["application/vnd.schemaregistry.v1+json"] = runtime.JSONProducer()
	transport.Consumers["application/vnd.schemaregistry.v1+json"] = runtime.JSONConsumer()
	transport.Context = context.Background()

	schemaRegistryClient := client.New(transport, strfmt.Default)

	return schemaRegistryClient, nil
}

func NewSchemaRegistryClient(schemaRegistryUrl string)	(*client.ConfluentSchemaRegistry, error)  {

	parsedUrl, err := url.Parse(schemaRegistryUrl)

	if err != nil {
		return nil, err
	}

	schemes := []string{parsedUrl.Scheme}

	transport := httpTransport.New(parsedUrl.Host, parsedUrl.Path, schemes)
	transport.Producers["application/vnd.schemaregistry.v1+json"] = runtime.JSONProducer()
	transport.Consumers["application/vnd.schemaregistry.v1+json"] = runtime.JSONConsumer()
	transport.Context = context.Background()

	schemaRegistryClient := client.New(transport, strfmt.Default)

	return schemaRegistryClient, nil
}