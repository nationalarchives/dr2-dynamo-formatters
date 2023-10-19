# DR2 Dynamo Formatters

This library contains case classes for the Dynamo files table along with implicit `DynamoFormat` instances.

These will carry out the following validation:

## Validation
### Mandatory fields
These fields must always be present
* id (UUID)
* batchId (String)
* type (Type) - Type is a custom Trait defined in this library.
* name (String)

### Numeric fields
These fields must be a number field in Dynamo and must parse into a numeric value in Scala.
* fileSize (Long)
* sortOrder (Int)

## Identifiers
All fields which are prefixed with `id_` will be read into a list of `Identifier` case classes.

All identifier case classes will be written to `id_` fields. 
Their identifierName will be prefixed with id_ and used as the field name 

## Error accumulation
The formatter will report all errors with a particular Dynamo row.
