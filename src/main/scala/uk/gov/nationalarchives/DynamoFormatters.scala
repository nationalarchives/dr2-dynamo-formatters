package uk.gov.nationalarchives

import cats.data._
import cats.implicits._
import org.scanamo._
import org.scanamo.generic.semiauto._

import java.util.UUID
import scala.jdk.CollectionConverters._
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
      case Some(typeString) => (typeField -> TypeCoercionError(new Exception(s"Type $typeString not found"))).invalidNel
      case None             => (typeField -> MissingProperty).invalidNel
    }

  implicit val dynamoTableFormat: DynamoFormat[DynamoTable] = new DynamoFormat[DynamoTable] {
    override def read(dynamoValue: DynamoValue): Either[DynamoReadError, DynamoTable] = {
      val map = dynamoValue.toAttributeValue.m().asScala

      def readNumber[T](name: String, toNumberFunction: String => T): ValidatedNel[InvalidProperty, Option[T]] = {
        map
          .get(name)
          .map { value =>
            Option(value.n()) match {
              case Some(value) if Try(toNumberFunction(value)).isSuccess => Option(toNumberFunction(value)).validNel
              case None => (name -> NoPropertyOfType("Number", DynamoValue.fromAttributeValue(value))).invalidNel
              case Some(value) =>
                (name -> TypeCoercionError(
                  new RuntimeException(s"Cannot parse $value for field $name to a number")
                )).invalidNel
            }
          }
          .getOrElse(None.validNel)
      }
      def valueOrInvalid(name: String): ValidatedNel[InvalidProperty, String] = value(name)
        .map(_.validNel)
        .getOrElse((name -> MissingProperty).invalidNel)

      def value(name: String) = map.get(name).map(_.s())

      (
        valueOrInvalid(batchId),
        valueOrInvalid(id),
        valueOrInvalid(name),
        stringToType(value(typeField)),
        readNumber(fileSize, _.toLong),
        readNumber(sortOrder, _.toInt)
      ).mapN { (batchId, id, name, typeName, fileSize, sortOrder) =>
        val identifiers = map
          .filter(_._1.startsWith("id_"))
          .map { case (name, value) =>
            Identifier(name.drop(3), value.s())
          }
          .toList
        DynamoTable(
          batchId,
          UUID.fromString(id),
          value(parentPath),
          name,
          typeName,
          value(title),
          value(description),
          sortOrder,
          fileSize,
          value(checksumSha256),
          value(fileExtension),
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
      val values = for {
        (fieldName, Some(potentialValue)) <- potentialValues
      } yield fieldName -> potentialValue
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
