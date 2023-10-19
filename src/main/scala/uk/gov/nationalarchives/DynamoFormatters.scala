package uk.gov.nationalarchives

import cats.data._
import cats.implicits._
import org.scanamo._
import org.scanamo.generic.semiauto._
import software.amazon.awssdk.services.dynamodb.model.AttributeValue.Type._

import java.util.UUID
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.util.Try

object DynamoFormatters {

  val batchId = "batchId"
  val id = "id"
  val name = "name"
  val typeField = "type"
  val fileSize = "fileSize"
  val sortOrder = "sortOrder"
  val parentPath = "parentPath"
  val title = "title"
  val description = "description"
  val checksumSha256 = "checksumSha256"
  val fileExtension = "fileExtension"

  private type InvalidProperty = (String, DynamoReadError)

  private def stringToType(potentialTypeString: Option[String]): ValidatedNel[InvalidProperty, Type] =
    potentialTypeString match {
      case Some("ArchiveFolder") => ArchiveFolder.validNel
      case Some("ContentFolder") => ContentFolder.validNel
      case Some("Asset")         => Asset.validNel
      case Some("File")          => File.validNel
      case Some(otherTypeString) =>
        (typeField -> TypeCoercionError(new Exception(s"Type $otherTypeString not found"))).invalidNel
      case None => (typeField -> MissingProperty).invalidNel
    }

  implicit val dynamoTableFormat: DynamoFormat[DynamoTable] = new DynamoFormat[DynamoTable] {
    override def read(dynamoValue: DynamoValue): Either[DynamoReadError, DynamoTable] = {
      val folderRowAsMap = dynamoValue.toAttributeValue.m().asScala.toMap

      def getPotentialNumber[T](name: String, toNumberFunction: String => T)(implicit
          classTag: ClassTag[T]
      ): ValidatedNel[InvalidProperty, Option[T]] = {
        folderRowAsMap
          .get(name)
          .map { attributeValue =>
            attributeValue.`type`() match {
              case N =>
                val value = attributeValue.n()
                val potentiallyConvertedValue = Try(toNumberFunction(value))
                if (potentiallyConvertedValue.isSuccess) potentiallyConvertedValue.toOption.validNel
                else
                  (name -> TypeCoercionError(
                    new RuntimeException(s"Cannot parse $value for field $name into ${classTag.runtimeClass}")
                  )).invalidNel
              case _ => (name -> NoPropertyOfType("Number", DynamoValue.fromAttributeValue(attributeValue))).invalidNel
            }
          }
          .getOrElse(None.validNel)
      }

      def getValidatedMandatoryFieldAsString(name: String): ValidatedNel[InvalidProperty, String] =
        getPotentialStringValue(name)
          .map(_.validNel)
          .getOrElse((name -> MissingProperty).invalidNel)

      def getPotentialStringValue(name: String) = folderRowAsMap.get(name).map(_.s())

      (
        getValidatedMandatoryFieldAsString(batchId),
        getValidatedMandatoryFieldAsString(id),
        getValidatedMandatoryFieldAsString(name),
        stringToType(getPotentialStringValue(typeField)),
        getPotentialNumber(fileSize, _.toLong),
        getPotentialNumber(sortOrder, _.toInt)
      ).mapN { (batchId, id, name, typeName, fileSize, sortOrder) =>
        val identifiers = folderRowAsMap.collect {
          case (name, value) if name.startsWith("id_") => Identifier(name.drop(3), value.s())
        }.toList

        DynamoTable(
          batchId,
          UUID.fromString(id),
          getPotentialStringValue(parentPath),
          name,
          typeName,
          getPotentialStringValue(title),
          getPotentialStringValue(description),
          sortOrder,
          fileSize,
          getPotentialStringValue(checksumSha256),
          getPotentialStringValue(fileExtension),
          identifiers
        )
      }.toEither
        .left
        .map(InvalidPropertiesError.apply)
    }

    override def write(t: DynamoTable): DynamoValue = {
      val potentialValues = Map(
        batchId -> Option(DynamoValue.fromString(t.batchId)),
        id -> Option(DynamoValue.fromString(t.id.toString)),
        parentPath -> t.parentPath.map(DynamoValue.fromString),
        name -> Option(DynamoValue.fromString(t.name)),
        typeField -> Option(DynamoValue.fromString(t.`type`.toString)),
        title -> t.title.map(DynamoValue.fromString),
        description -> t.description.map(DynamoValue.fromString),
        sortOrder -> t.sortOrder.map(DynamoValue.fromNumber[Int]),
        fileSize -> t.fileSize.map(DynamoValue.fromNumber[Long]),
        checksumSha256 -> t.checksumSha256.map(DynamoValue.fromString),
        fileExtension -> t.fileExtension.map(DynamoValue.fromString)
      ) ++ t.identifiers.map(id => s"id_${id.identifierName}" -> Option(DynamoValue.fromString(id.value))).toMap
      val values = potentialValues.flatMap {
        case (fieldName, Some(potentialValue)) => Map(fieldName -> potentialValue)
        case _                                 => Map.empty
      }
      DynamoObject(values).toDynamoValue
    }
  }
  implicit val pkFormat: Typeclass[PartitionKey] = deriveDynamoFormat[PartitionKey]

  sealed trait Type {
    override def toString: String = this match {
      case ArchiveFolder => "ArchiveFolder"
      case ContentFolder => "ContentFolder"
      case Asset         => "Asset"
      case File          => "File"
    }
  }

  case object ArchiveFolder extends Type

  case object ContentFolder extends Type

  case object Asset extends Type

  case object File extends Type

  case class DynamoTable(
      batchId: String,
      id: UUID,
      parentPath: Option[String],
      name: String,
      `type`: Type,
      title: Option[String] = None,
      description: Option[String] = None,
      sortOrder: Option[Int] = None,
      fileSize: Option[Long] = None,
      checksumSha256: Option[String] = None,
      fileExtension: Option[String] = None,
      identifiers: List[Identifier] = Nil
  )

  case class Identifier(identifierName: String, value: String)

  case class PartitionKey(id: UUID)

}
