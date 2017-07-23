package clients.mongo

/**
  * Created by Stefano on 19/07/2017.
  */

import java.util

import com.mongodb.{MongoClient, MongoClientURI}
import com.mongodb.client.MongoDatabase
import com.mongodb.client.model._
import org.bson.Document
import org.bson.conversions.Bson
import org.slf4j.LoggerFactory


class MongoDBClient(val url: String, val collectionName: String){

   //Creates a MongoURI from the given string.
  val uri = new MongoClientURI(url)
  //Creates a MongoClient described by a URI.
  private val client = new MongoClient(uri)
  //Gets a Database.
  private val db: MongoDatabase = client.getDatabase(uri.getDatabase)
  //Gets a collection.
  private val collection = db.getCollection(collectionName)
  private val LOG = LoggerFactory.getLogger(classOf[MongoDBClient])

  /**
    * Inserts one or more documents.
    * This method is equivalent to a call to the bulkWrite method.
    * The documents will be inserted in the order provided,
    * stopping on the first failed insertion.
    *
    * @param documents documents
    */
  def insert(documents: util.List[Document], ordered: Boolean): Unit = {
    val options = new InsertManyOptions
    if (!ordered) options.ordered(false)
    collection.insertMany(documents, options)
  }

  /**
    * Update a single or all documents in the collection according to the specified arguments.
    * When upsert set to true, the new document will be inserted if there are no matches to the query filter.
    *
    * @param filter   Bson filter
    * @param document Bson document
    * @param upsert   a new document should be inserted if there are no matches to the query filter
    * @param many     whether find all documents according to the query filter
    */
  def update(filter: Bson, document: Bson, upsert: Boolean, many: Boolean): Unit = {
    val options = new UpdateOptions
    if (upsert) options.upsert(true)
    if (many) collection.updateMany(filter, document, options)
    else collection.updateOne(filter, document, options)
  }

  /**
    * Finds a single document in the collection according to the specified arguments.
    *
    * @param filter Bson filter
    */
  def find(filter: Bson): Document = {
    collection.find(filter).first
  }

  /**
    * Closes all resources associated with this instance.
    */
  def close(): Unit = {
    client.close()
  }


  /**
    * Update a single or all documents in the collection according to the specified arguments.
    * When upsert set to true, the new document will be inserted if there are no matches to the query filter.
    *
    * @param documents_filters Bson documents and filters pair list
    * @param upsert   a new document should be inserted if there are no matches to the query filter
    * @param many     whether find all documents according to the query filter
    */
  def updateBulk(documents_filters: util.ArrayList[(Document, Bson)], upsert: Boolean, many: Boolean): Unit = {

    val options = new UpdateOptions
    val bulkoptions = new BulkWriteOptions
    val operations = new util.ArrayList[WriteModel[Document]]

    val listIterator = documents_filters.listIterator()

    if (upsert) options.upsert(true)
    while(listIterator.hasNext ){
      val current = listIterator.next()
      if(!many)
        operations.add(new UpdateOneModel[Document](current._2, current._1, options))
      else
        operations.add(new UpdateManyModel[Document](current._2, current._1, options))

    }
    val result = collection.bulkWrite(operations, bulkoptions)
    if(!result.wasAcknowledged())
      LOG.warn("BulkWrite was not acknowledged by target db.")
    LOG.info(s"BulkWrite was acknowledged with ${result.getInsertedCount} inserts, ${result.getModifiedCount} updates and ${result.getMatchedCount} matches.")

  }


}