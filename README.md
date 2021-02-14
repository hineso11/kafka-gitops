# Kafka GitOps

Kafka GitOps is a command line utility designed to allow users to 
take a declarative approach to managing Kafka configuration. 

It is best used as part of a GitOps methodology but can also be used
as part of a more traditional workflow.

## Usage

### TLDR
Currently, the tool only supports managing schemas 
declaratively as this was the main gap we identified in other tools.

In the future we hope to support other parts of the Kafka setup. In 
particular, supporting topics and ACLs is high up the agenda.
If you require these tools immediately though, I'd advise checking 
out [devshawn/kafka-gitops](https://github.com/devshawn/kafka-gitops)
or [teslamotors/kafka-helmsman](https://github.com/teslamotors/kafka-helmsman)
for similar functionality.


We support the Confluent Schema Registry to manage schemas. Its 
open-source and you can run it yourself or get a managed instance
at [Confluent Cloud](https://confluent.cloud).

### The Kafka File

The Kafka File is the root of the configuration declarations. By default, it 
is presumed to be `kafka.yaml` and located in the same directory the command
is executed in. It has the following format
```yaml
# version included mainly for future compatibility reasons, no real meaning as yet
apiVersion: v1
topics:
  # map of topic names to corresponding configurations
  user-created:
    # configuration of schemas for topic
    schemas:
      # paths to where schemas are located relative to Kafka File
      key: schemas/user-created/key.avsc
      value: schemas/user-created/value.avsc
  order-cancelled:
    schemas:
      key: schemas/order-cancelled/key.proto
      value: schemas/order-cancelled/value.proto
  profile-viewed:
    schemas:
      key: schemas/profile-viewed/key.json
      value: schemas/profile-viewed/value.json
  client-created:
    schemas:
      key: schemas/client-created/key.proto
      value: schemas/client-created/value.proto
```

### Getting Started
Initialise a Kafka File called `kafka.yaml` in your project according to the
above specification.
Then run the command:
```shell
kafka-gitops -schema-registry-url https://blah.com -schema-registry-api-key my-key -schema-registry-api-secret my-secret
```

Add the flag `-dry-run` to execute all the checks without applying any changes. 

The output should look something like this, depending on what state the 
existing configuration is in.

```shell
Executing Kafka-GitOps in dry-run mode, no changes will be made.
Took the following actions on schemas:
NOOP profile-viewed-key
NOOP profile-viewed-value
CREATE client-created-key
CREATE client-created-value
NOOP user-created-key
NOOP user-created-value
UPDATE order-cancelled-key
NOOP order-cancelled-value
```
### Schemas
We support Confluent Schema Registry and as such we hope to support the 
serialisation formats they support fully. In principle this is true and we support the following
serialisation formats:
- **Avro** (in .avsc files)
- **JSON** (in .json files)
- **Protocol Buffers** (in .proto files)

However, in practice support is first-class for Avro and JSON but is not 
quite there for Protocol Buffers. **TLDR:** the tool should work for 
Protocol Buffers, but the output might not be entirely intuitive. 

The tool will determine the compatibility of schemas against the latest
version in the schema registry before it applies any changes (and also in
dry-run mode). 