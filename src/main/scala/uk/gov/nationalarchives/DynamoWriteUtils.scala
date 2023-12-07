package uk.gov.nationalarchives

import org.scanamo.{DynamoObject, DynamoValue}
import org.scanamo.generic.semiauto.FieldName
import uk.gov.nationalarchives.DynamoFormatters._

object DynamoWriteUtils {

  private def commonFieldsToMap(table: DynamoTable): Map[String, DynamoValue] = {
    val optionalFields: Map[FieldName, DynamoValue] = Map(
      "title" -> table.title.map(DynamoValue.fromString),
      "description" -> table.description.map(DynamoValue.fromString),
      "parentPath" -> table.parentPath.map(DynamoValue.fromString)
    ).flatMap {
      case (fieldName, Some(potentialValue)) => Map(fieldName -> potentialValue)
      case _                                 => Map.empty
    }
    Map(
      "batchId" -> DynamoValue.fromString(table.batchId),
      "id" -> DynamoValue.fromString(table.id.toString),
      "name" -> DynamoValue.fromString(table.name),
      "type" -> DynamoValue.fromString(table.`type`.toString)
    ) ++ table.identifiers.map(id => s"id_${id.identifierName}" -> DynamoValue.fromString(id.value)).toMap ++
      optionalFields
  }

  def writeArchiveFolderTable(archiveFolderDynamoTable: ArchiveFolderDynamoTable): DynamoValue =
    DynamoObject {
      commonFieldsToMap(archiveFolderDynamoTable)
    }.toDynamoValue

  def writeContentFolderTable(contentFolderDynamoTable: ContentFolderDynamoTable): DynamoValue =
    DynamoObject {
      commonFieldsToMap(contentFolderDynamoTable)
    }.toDynamoValue

  def writeAssetTable(assetDynamoTable: AssetDynamoTable): DynamoValue =
    DynamoObject {
      commonFieldsToMap(assetDynamoTable) ++
        Map(
          "transferringBody" -> DynamoValue.fromString(assetDynamoTable.transferringBody),
          "transferCompleteDatetime" -> DynamoValue.fromString(assetDynamoTable.transferCompleteDatetime.toString),
          "upstreamSystem" -> DynamoValue.fromString(assetDynamoTable.upstreamSystem),
          "digitalAssetSource" -> DynamoValue.fromString(assetDynamoTable.digitalAssetSource),
          "digitalAssetSubtype" -> DynamoValue.fromString(assetDynamoTable.digitalAssetSubtype),
          "originalFiles" -> DynamoValue.fromStrings(assetDynamoTable.originalFiles.map(_.toString)),
          "originalMetadataFiles" -> DynamoValue.fromStrings(assetDynamoTable.originalMetadataFiles.map(_.toString))
        )
    }.toDynamoValue

  def writeFileTable(fileDynamoTable: FileDynamoTable): DynamoValue =
    DynamoObject {
      commonFieldsToMap(fileDynamoTable) ++
        Map(
          "sortOrder" -> DynamoValue.fromNumber[Int](sortOrder.toInt),
          "fileSize" -> DynamoValue.fromNumber[Long](fileSize.toLong),
          "checksumSha256" -> DynamoValue.fromString(checksumSha256),
          "fileExtension" -> DynamoValue.fromString(fileExtension)
        )
    }.toDynamoValue
}
