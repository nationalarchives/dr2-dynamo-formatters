package uk.gov.nationalarchives

import cats.data._
import cats.implicits._
import org.scanamo._
import org.scanamo.generic.semiauto._
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import software.amazon.awssdk.services.dynamodb.model.AttributeValue.Type._

import java.time.OffsetDateTime
import java.util.UUID
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.util.Try

object DynamoFormatters {
  private type InvalidProperty = (String, DynamoReadError)
  val batchId = "batchId"
  val id = "id"
  val name = "name"
  val typeField = "type"
  val fileSize = "fileSize"
  val sortOrder = "sortOrder"
  val parentPath = "parentPath"
  val title = "title"
  val description = "description"
  val checksumSha256 = "checksum_sha256"
  val fileExtension = "fileExtension"
  val transferringBody = "transferringBody"
  val transferCompleteDatetime = "transferCompleteDatetime"
  val upstreamSystem = "upstreamSystem"
  val digitalAssetSource = "digitalAssetSource"
  val digitalAssetSubtype = "digitalAssetSubtype"
  val originalFiles = "originalFiles"
  val originalMetadataFiles = "originalMetadataFiles"
  private val typeCoercionError =
    (name: String, message: String) => (name -> TypeCoercionError(new RuntimeException(message))).invalidNel

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

  private def stringToScalaType[T](name: String, potentialString: Option[String], toScalaTypeFunction: String => T)(
      implicit classTag: ClassTag[T]
  ): ValidatedNel[InvalidProperty, T] =
    potentialString match {
      case Some(value) =>
        val potentialDate = Try(toScalaTypeFunction(value))
        if (potentialDate.isSuccess) potentialDate.get.validNel
        else typeCoercionError(name, s"Cannot parse $value for field $name into ${classTag.runtimeClass}")

      case None => (name -> MissingProperty).invalidNel
    }

  private def convertListOfStringsToT[T](fromStringToAnotherType: String => T)(
      attributeName: String,
      attributes: List[AttributeValue]
  )(implicit classTag: ClassTag[T]): ValidatedNel[(FieldName, TypeCoercionError), List[T]] = {
    val potentiallyConvertedValues =
      Try(attributes.map(_.s().toString)) // if result of .s() is 'null', convert to String and invoke NPE

    if (potentiallyConvertedValues.isSuccess) {
      val stringsPotentiallyConvertedToT =
        potentiallyConvertedValues.get.map(stringValue =>
          stringToScalaType(attributeName, Some(stringValue), fromStringToAnotherType)
        )
      val allStringsConvertedToT = stringsPotentiallyConvertedToT.forall(_.isValid)
      if (allStringsConvertedToT) stringsPotentiallyConvertedToT.flatMap(validNelOfT => validNelOfT.toList).validNel
      else
        typeCoercionError(attributeName, s"Cannot parse $attributes for field $attributeName ${classTag.runtimeClass}")
    } else typeCoercionError(attributeName, s"Cannot parse $attributes for field $attributeName into Strings")
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
                else typeCoercionError(name, s"Cannot parse $value for field $name into ${classTag.runtimeClass}")
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

      def getPotentialListOfValues[T](
          name: String,
          convertListOfAttributesToT: (
              String,
              List[AttributeValue]
          ) => ValidatedNel[(FieldName, TypeCoercionError), List[T]]
      ): ValidatedNel[InvalidProperty, List[T]] =
        folderRowAsMap
          .get(name)
          .map { attributeValue =>
            attributeValue.`type`() match {
              case L =>
                val attributes: List[AttributeValue] = attributeValue.l().asScala.toList
                convertListOfAttributesToT(name, attributes)
              case _ => (name -> NoPropertyOfType("List", DynamoValue.fromAttributeValue(attributeValue))).invalidNel
            }
          }
          .getOrElse((name -> MissingProperty).invalidNel)

      (
        getValidatedMandatoryFieldAsString(batchId),
        getValidatedMandatoryFieldAsString(id),
        getValidatedMandatoryFieldAsString(name),
        stringToType(getPotentialStringValue(typeField)),
        getPotentialNumber(fileSize, _.toLong),
        getPotentialNumber(sortOrder, _.toInt),
        getValidatedMandatoryFieldAsString(transferringBody),
        stringToScalaType(
          transferCompleteDatetime,
          getPotentialStringValue(transferCompleteDatetime),
          str => OffsetDateTime.parse(str)
        ),
        getValidatedMandatoryFieldAsString(upstreamSystem),
        getValidatedMandatoryFieldAsString(digitalAssetSource),
        getValidatedMandatoryFieldAsString(digitalAssetSubtype),
        getPotentialListOfValues(originalFiles, convertListOfStringsToT(UUID.fromString)),
        getPotentialListOfValues(originalMetadataFiles, convertListOfStringsToT(UUID.fromString))
      ).mapN {
        (
            batchId,
            id,
            name,
            typeName,
            fileSize,
            sortOrder,
            transferringBody,
            transferCompleteDatetime,
            upstreamSystem,
            digitalAssetSource,
            digitalAssetSubtype,
            originalFiles,
            originalMetadataFiles
        ) =>
          val identifiers = folderRowAsMap.collect {
            case (name, value) if name.startsWith("id_") => Identifier(name.drop(3), value.s())
          }.toList

          DynamoTable(
            batchId,
            UUID.fromString(id),
            getPotentialStringValue(parentPath),
            name,
            typeName,
            transferringBody,
            transferCompleteDatetime,
            upstreamSystem,
            digitalAssetSource,
            digitalAssetSubtype,
            originalFiles,
            originalMetadataFiles,
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
        transferringBody -> Option(DynamoValue.fromString(t.transferringBody)),
        transferCompleteDatetime -> Option(DynamoValue.fromString(t.transferCompleteDatetime.toString)),
        upstreamSystem -> Option(DynamoValue.fromString(t.upstreamSystem)),
        digitalAssetSource -> Option(DynamoValue.fromString(t.digitalAssetSource)),
        digitalAssetSubtype -> Option(DynamoValue.fromString(t.digitalAssetSubtype)),
        originalFiles -> Option(DynamoValue.fromStrings(t.originalFiles.map(_.toString))),
        originalMetadataFiles -> Option(DynamoValue.fromStrings(t.originalMetadataFiles.map(_.toString))),
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

  case class DynamoTable(
      batchId: String,
      id: UUID,
      parentPath: Option[String],
      name: String,
      `type`: Type,
      transferringBody: String,
      transferCompleteDatetime: OffsetDateTime,
      upstreamSystem: String,
      digitalAssetSource: String,
      digitalAssetSubtype: String,
      originalFiles: List[UUID],
      originalMetadataFiles: List[UUID],
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

  case object ArchiveFolder extends Type

  case object ContentFolder extends Type

  case object Asset extends Type

  case object File extends Type

}
