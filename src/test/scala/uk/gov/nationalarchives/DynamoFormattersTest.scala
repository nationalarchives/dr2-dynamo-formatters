package uk.gov.nationalarchives

import cats.implicits._
import org.scalatest.EitherValues
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers._
import org.scalatest.prop.{TableDrivenPropertyChecks, TableFor2}
import org.scanamo._
import software.amazon.awssdk.services.dynamodb.model.AttributeValue
import uk.gov.nationalarchives.DynamoFormatters._

import java.util.UUID
import scala.jdk.CollectionConverters._

class DynamoFormattersTest extends AnyFlatSpec with TableDrivenPropertyChecks with EitherValues {

  val allFieldsPopulated: Map[String, AttributeValue] = {
    Map(
      batchId -> AttributeValue.fromS("testBatchId"),
      id -> AttributeValue.fromS(UUID.randomUUID().toString),
      parentPath -> AttributeValue.fromS("testParentPath"),
      name -> AttributeValue.fromS("testName"),
      typeField -> AttributeValue.fromS("ArchiveFolder"),
      title -> AttributeValue.fromS("testTitle"),
      description -> AttributeValue.fromS("testDescription"),
      sortOrder -> AttributeValue.fromN("2"),
      fileSize -> AttributeValue.fromN("1"),
      checksumSha256 -> AttributeValue.fromS("testChecksumSha256"),
      fileExtension -> AttributeValue.fromS("testFileExtension"),
      "id_Test" -> AttributeValue.fromS("testIdentifier"),
      "id_Test2" -> AttributeValue.fromS("testIdentifier2")
    )
  }

  def buildAttributeValue(map: Map[String, AttributeValue]): AttributeValue =
    AttributeValue.builder().m(map.asJava).build()

  def allMandatoryFieldsMap(typeValue: String): Map[String, AttributeValue] = {
    List((id, UUID.randomUUID().toString), (batchId, "batchId"), (name, "name"), (typeField, "ArchiveFolder"))
      .map(field => field._1 -> AttributeValue.fromS(field._2))
      .toMap +
      (typeField -> AttributeValue.fromS(typeValue))
  }

  def invalidNumericField(fieldName: String): AttributeValue = buildAttributeValue(
    allFieldsPopulated + (fieldName -> AttributeValue.fromS("1"))
  )
  def invalidNumericValue(fieldName: String): AttributeValue = buildAttributeValue(
    allFieldsPopulated + (fieldName -> AttributeValue.fromN("NaN"))
  )

  def invalidTypeAttributeValue: AttributeValue =
    AttributeValue.builder().m(allMandatoryFieldsMap("Invalid").asJava).build()

  def missingFieldsInvalidNumericField(invalidNumericField: String, missingFields: String*): AttributeValue =
    buildAttributeValue(missingFieldsMap(missingFields: _*) + (invalidNumericField -> AttributeValue.fromS("1")))

  def missingFieldsMap(fieldsToExclude: String*): Map[String, AttributeValue] = allMandatoryFieldsMap("ArchiveFolder")
    .filterNot(fields => fieldsToExclude.contains(fields._1))

  def missingFieldsAttributeValue(fieldsToExclude: String*): AttributeValue = {
    val fieldMap = missingFieldsMap(fieldsToExclude: _*)
    AttributeValue.builder().m(fieldMap.asJava).build()
  }

  val invalidDynamoAttributeValues: TableFor2[AttributeValue, String] = Table(
    ("attributeValue", "expectedErrorMessage"),
    (missingFieldsAttributeValue(id), "'id': missing"),
    (missingFieldsAttributeValue(batchId), "'batchId': missing"),
    (missingFieldsAttributeValue(name), "'name': missing"),
    (missingFieldsAttributeValue(typeField), "'type': missing"),
    (missingFieldsAttributeValue(typeField, batchId, name), "'batchId': missing, 'name': missing, 'type': missing"),
    (invalidNumericField(fileSize), "'fileSize': not of type: 'Number' was 'DynString(1)'"),
    (
      invalidNumericValue(fileSize),
      "'fileSize': could not be converted to desired type: java.lang.RuntimeException: Cannot parse NaN for field fileSize to a number"
    ),
    (invalidNumericField(sortOrder), "'sortOrder': not of type: 'Number' was 'DynString(1)'"),
    (
      invalidNumericValue(sortOrder),
      "'sortOrder': could not be converted to desired type: java.lang.RuntimeException: Cannot parse NaN for field sortOrder to a number"
    ),
    (
      invalidTypeAttributeValue,
      "'type': could not be converted to desired type: java.lang.Exception: Type Invalid not found"
    ),
    (
      missingFieldsInvalidNumericField(fileSize, id, batchId),
      "'batchId': missing, 'id': missing, 'fileSize': not of type: 'Number' was 'DynString(1)'"
    )
  )
  forAll(invalidDynamoAttributeValues) { (attributeValue, expectedErrors) =>
    "dynamoTableFormat read" should s"return an error $expectedErrors" in {
      val res = dynamoTableFormat.read(attributeValue)
      res.isLeft should be(true)
      res.left.value.show should equal(expectedErrors)
    }
  }

  "dynamoTableFormat read" should "return a valid object when all fields are populated" in {
    val res = dynamoTableFormat.read(buildAttributeValue(allFieldsPopulated)).value

    res.batchId should equal("testBatchId")
    res.id should equal(UUID.fromString(allFieldsPopulated(id).s()))
    res.parentPath.get should equal("testParentPath")
    res.name should equal("testName")
    res.`type` should equal(ArchiveFolder)
    res.title.get should equal("testTitle")
    res.description.get should equal("testDescription")
    res.sortOrder.get should equal(2)
    res.fileSize.get should equal(1)
    res.checksumSha256.get should equal("testChecksumSha256")
    res.fileExtension.get should equal("testFileExtension")
    res.identifiers should equal(List(Identifier("Test2", "testIdentifier2"), Identifier("Test", "testIdentifier")))
  }

  "dynamoTableFormat write" should "write all mandatory fields and ignore any optional ones" in {
    val uuid = UUID.randomUUID()
    val dynamoTable = DynamoTable(batchId, uuid, None, name, ContentFolder, None, None, None)
    val res = dynamoTableFormat.write(dynamoTable)
    val resultMap = res.toAttributeValue.m().asScala
    resultMap(batchId).s() should equal(batchId)
    resultMap(id).s() should equal(uuid.toString)
    resultMap(name).s() should equal(name)
    resultMap(typeField).s() should equal("ContentFolder")
    List(parentPath, title, description, sortOrder, fileSize, checksumSha256, fileExtension, "identifiers")
      .forall(resultMap.contains) should be(false)
  }

  "dynamoTableFormat write" should "write all fields when all fields are populated" in {
    val uuid = UUID.randomUUID()
    val identifiers = List(Identifier("Test1", "Value1"), Identifier("Test2", "Value2"))
    val dynamoTable = DynamoTable(
      batchId,
      uuid,
      Option(parentPath),
      name,
      Asset,
      Option(title),
      Option(description),
      Option(1),
      Option(2),
      Option(checksumSha256),
      Option(fileExtension),
      identifiers
    )
    val res = dynamoTableFormat.write(dynamoTable)
    val resultMap = res.toAttributeValue.m().asScala
    resultMap(batchId).s() should equal(batchId)
    resultMap(id).s() should equal(uuid.toString)
    resultMap(name).s() should equal(name)
    resultMap(typeField).s() should equal("Asset")
    resultMap(parentPath).s() should equal(parentPath)
    resultMap(title).s() should equal(title)
    resultMap(description).s() should equal(description)
    resultMap(sortOrder).n() should equal("1")
    resultMap(fileSize).n() should equal("2")
    resultMap(checksumSha256).s() should equal("checksumSha256")
    resultMap(fileExtension).s() should equal(fileExtension)
    resultMap("id_Test1").s() should equal("Value1")
    resultMap("id_Test2").s() should equal("Value2")
  }

  "pkFormat read" should "read the correct fields" in {
    val uuid = UUID.randomUUID()
    val input = AttributeValue.fromM(Map(id -> AttributeValue.fromS(uuid.toString)).asJava)
    val res = pkFormat.read(input).value
    res.id should equal(uuid)
  }

  "pkFormat read" should "error if the field is missing" in {
    val uuid = UUID.randomUUID()
    val input = AttributeValue.fromM(Map("invalid" -> AttributeValue.fromS(uuid.toString)).asJava)
    val res = pkFormat.read(input)
    res.isLeft should be(true)
    val isMissingPropertyError = res.left.value.asInstanceOf[InvalidPropertiesError].errors.head._2 match {
      case MissingProperty => true
      case _               => false
    }
    isMissingPropertyError should be(true)
  }

  "pkFormat write" should "write the correct fields" in {
    val uuid = UUID.randomUUID()
    val attributeValueMap = pkFormat.write(PartitionKey(uuid)).toAttributeValue.m().asScala
    UUID.fromString(attributeValueMap(id).s()) should equal(uuid)
  }
}
