# DR2 Dynamo Formatters

This library contains case classes for the Dynamo tables dr2-files and dr2-ingest-lock table along with implicit `DynamoFormat` instances.

These will carry out the following validation:

## Files table
This stores information about the folders, assets and files of an ingest.
### Validation
#### Mandatory fields
These fields must always be present
* id (UUID)
* batchId (String)
* type (Type) - Type is a custom Trait defined in this library.
* name (String)

#### Numeric fields
These fields must be a number field in Dynamo and must parse into a numeric value in Scala.
* fileSize (Long)
* sortOrder (Int)

### Identifiers
All fields which are prefixed with `id_` will be read into a list of `Identifier` case classes.

All identifier case classes will be written to `id_` fields. 
Their identifierName will be prefixed with id_ and used as the field name 

### Error accumulation
The formatter will report all errors with a particular Dynamo row.

## Lock table
This holds a lock with an asset ID, a message ID, the parentMessage ID and the execution ID.
### Validation
#### Mandatory fields
These fields must always be present
* assetId (UUID)
* messageId (UUID)
