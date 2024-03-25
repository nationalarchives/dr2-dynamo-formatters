package uk.gov.nationalarchives

import cats.data.ValidatedNel
import org.scanamo._
import org.scanamo.generic.semiauto.{FieldName, Typeclass, deriveDynamoFormat}
import uk.gov.nationalarchives.DynamoWriteUtils._

import java.time.OffsetDateTime
import java.util.UUID
import scala.jdk.CollectionConverters._

object DynamoFormatters {

  private def createReadDynamoUtils(dynamoValue: DynamoValue) = {
    val folderRowAsMap = dynamoValue.toAttributeValue.m().asScala.toMap
    new DynamoReadUtils(folderRowAsMap)
  }

  implicit val archiveFolderTableFormat: DynamoFormat[ArchiveFolderDynamoTable] =
    new DynamoFormat[ArchiveFolderDynamoTable] {
      override def read(dynamoValue: DynamoValue): Either[DynamoReadError, ArchiveFolderDynamoTable] =
        createReadDynamoUtils(dynamoValue).readArchiveFolderRow

      override def write(table: ArchiveFolderDynamoTable): DynamoValue =
        writeArchiveFolderTable(table)
    }

  implicit val contentFolderTableFormat: DynamoFormat[ContentFolderDynamoTable] =
    new DynamoFormat[ContentFolderDynamoTable] {
      override def read(dynamoValue: DynamoValue): Either[DynamoReadError, ContentFolderDynamoTable] =
        createReadDynamoUtils(dynamoValue).readContentFolderRow

      override def write(table: ContentFolderDynamoTable): DynamoValue =
        writeContentFolderTable(table)
    }

  implicit val assetTableFormat: DynamoFormat[AssetDynamoTable] = new DynamoFormat[AssetDynamoTable] {
    override def read(dynamoValue: DynamoValue): Either[DynamoReadError, AssetDynamoTable] =
      createReadDynamoUtils(dynamoValue).readAssetRow

    override def write(table: AssetDynamoTable): DynamoValue =
      writeAssetTable(table)
  }

  implicit val fileTableFormat: DynamoFormat[FileDynamoTable] = new DynamoFormat[FileDynamoTable] {
    override def read(dynamoValue: DynamoValue): Either[DynamoReadError, FileDynamoTable] =
      createReadDynamoUtils(dynamoValue).readFileRow

    override def write(table: FileDynamoTable): DynamoValue =
      writeFileTable(table)
  }

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
  val representationType = "representationType"
  val representationSuffix = "representationSuffix"

  implicit val pkFormat: Typeclass[PartitionKey] = deriveDynamoFormat[PartitionKey]

  sealed trait Type {
    override def toString: String = this match {
      case ArchiveFolder => "ArchiveFolder"
      case ContentFolder => "ContentFolder"
      case Asset         => "Asset"
      case File          => "File"
    }
  }

  sealed trait DynamoTable {
    def batchId: String
    def id: UUID
    def parentPath: Option[String]
    def name: String
    def `type`: Type
    def title: Option[String]
    def description: Option[String]
    def identifiers: List[Identifier]
  }

  private type ValidatedField[T] = ValidatedNel[(FieldName, DynamoReadError), T]

  case class ValidatedFields(
      batchId: ValidatedField[String],
      id: ValidatedField[UUID],
      name: ValidatedField[String],
      parentPath: Option[String],
      title: Option[String],
      description: Option[String],
      `type`: ValidatedField[Type],
      transferringBody: ValidatedField[String],
      transferCompleteDatetime: ValidatedField[OffsetDateTime],
      upstreamSystem: ValidatedField[String],
      digitalAssetSource: ValidatedField[String],
      digitalAssetSubtype: ValidatedField[String],
      originalFiles: ValidatedField[List[UUID]],
      originalMetadataFiles: ValidatedField[List[UUID]],
      sortOrder: ValidatedField[Int],
      fileSize: ValidatedField[Long],
      checksumSha256: ValidatedField[String],
      fileExtension: ValidatedField[String],
      representationType: ValidatedField[RepresentationType],
      representationSuffix: ValidatedField[String],
      identifiers: List[Identifier]
  )

  case class ArchiveFolderDynamoTable(
      batchId: String,
      id: UUID,
      parentPath: Option[String],
      name: String,
      `type`: Type,
      title: Option[String],
      description: Option[String],
      identifiers: List[Identifier]
  ) extends DynamoTable

  case class ContentFolderDynamoTable(
      batchId: String,
      id: UUID,
      parentPath: Option[String],
      name: String,
      `type`: Type,
      title: Option[String],
      description: Option[String],
      identifiers: List[Identifier]
  ) extends DynamoTable

  case class AssetDynamoTable(
      batchId: String,
      id: UUID,
      parentPath: Option[String],
      name: String,
      `type`: Type,
      title: Option[String],
      description: Option[String],
      transferringBody: String,
      transferCompleteDatetime: OffsetDateTime,
      upstreamSystem: String,
      digitalAssetSource: String,
      digitalAssetSubtype: String,
      originalFiles: List[UUID],
      originalMetadataFiles: List[UUID],
      identifiers: List[Identifier]
  ) extends DynamoTable

  case class FileDynamoTable(
      batchId: String,
      id: UUID,
      parentPath: Option[String],
      name: String,
      `type`: Type,
      title: Option[String],
      description: Option[String],
      sortOrder: Int,
      fileSize: Long,
      checksumSha256: String,
      fileExtension: String,
      representationType: RepresentationType,
      representationSuffix: String,
      identifiers: List[Identifier]
  ) extends DynamoTable

  case class Identifier(identifierName: String, value: String)

  case class PartitionKey(id: UUID)

  case object ArchiveFolder extends Type

  case object ContentFolder extends Type

  case object Asset extends Type

  case object File extends Type

  sealed trait RepresentationType

  case object Preservation extends RepresentationType

  case object Access extends RepresentationType

}
