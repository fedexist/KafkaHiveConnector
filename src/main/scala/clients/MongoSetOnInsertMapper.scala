package clients

import org.apache.storm.mongodb.common.mapper.MongoMapper
import org.apache.storm.tuple.ITuple
import org.bson.Document


/**
  * Created by Stefano on 19/07/2017.
  */
class MongoSetOnInsertMapper extends MongoMapper {

  var set_fields: List[String] = List[String]()
  var setoninsert_fields: List[String] = List[String]()

  def toDocument(tuple: ITuple): Document = {
    val setDocument = new Document()
    val setOnInsertDocument = new Document()
    for (field <- set_fields) {
      setDocument.append(field, tuple.getValueByField(field))
    }
    for (field <- setoninsert_fields) {
      setOnInsertDocument.append(field, tuple.getValueByField(field))
    }

    //$set operator: Sets the value of a field in a document.
    new Document("$set", setDocument).append("$setOnInsert", setOnInsertDocument)
  }

  def withSetFields(set_fields: List[String]): MongoSetOnInsertMapper = {
    this.set_fields = set_fields
    this
  }

  def withSetOnInsertFields(setoninsert_fields: List[String]): MongoSetOnInsertMapper = {

    this.setoninsert_fields = setoninsert_fields
    this

  }

}
