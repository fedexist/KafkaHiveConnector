package clients

import java.{lang, util}

import org.apache.storm.topology.FailedException

import scala.collection.JavaConversions._
import com.google.common.collect.Lists
import org.apache.commons.lang.Validate
import org.apache.storm.trident.operation.TridentCollector
import org.apache.storm.trident.tuple.TridentTuple
import org.bson.Document
import org.apache.storm.trident.state.State
import org.apache.storm.mongodb.common.mapper.MongoMapper
import org.slf4j.{Logger, LoggerFactory}

class Options extends Serializable {
  var url : String = _
  var collectionName : String = _
  var mapper : MongoMapper = _

  def withUrl(url: String): Options = {
    this.url = url
    this
  }

  def withCollectionName(collectionName: String): Options = {
    this.collectionName = collectionName
    this
  }

  def withMapper(mapper: MongoMapper): Options = {
    this.mapper = mapper
    this
  }
}

class MongoState(_map : util.Map[_,_], _options: Options) extends State {

  private val LOG = LoggerFactory.getLogger(classOf[MongoState])
  private val options: Options = _options
  private var mongoClient: MongoDBClient = _
  private val map: util.Map[_, _] = _map

  def prepare(): Unit = {
    Validate.notEmpty(this.options.url, "url can not be blank or null")
    Validate.notEmpty(this.options.collectionName, "collectionName can not be blank or null")
    Validate.notNull(this.options.mapper, "MongoMapper can not be null")
    mongoClient = new MongoDBClient(this.options.url, this.options.collectionName)
  }

  def beginCommit(txid: lang.Long): Unit = {
    LOG.debug("beginCommit is noop.")
  }

  def commit(txid: lang.Long): Unit = {
    LOG.debug("commit is noop.")
  }

  def updateState (tuples: util.List[TridentTuple], collector: TridentCollector): Unit = {
    //val documents = Lists.newArrayList[Document]

    for (tuple <- tuples) {
      val document = options.mapper.toDocument (tuple)
      //documents.add(document)

      val filter : Document = new Document().append("_id", tuple.getIntegerByField("_id"))

      try
        mongoClient.update(filter, document , upsert = true, many = false)
      catch {
        case e: Exception => LOG.warn ("Batch write failed but some requests might have succeeded. Triggering replay.", e)
          throw new FailedException (e)
      }


    }


  }

}




