package clients.mongo

import java.util

import org.apache.storm.task.IMetricsContext
import org.apache.storm.trident.state.{State, StateFactory}

/**
  * Created by Stefano on 19/07/2017.
  */

  class MongoStateFactory(var options: Options) extends StateFactory {

    override def makeState(conf: util.Map[_, _], metrics: IMetricsContext, partitionIndex: Int, numPartitions: Int): State = {
      val state = new MongoState(conf, this.options)
      state.prepare()
      state
    }
}
