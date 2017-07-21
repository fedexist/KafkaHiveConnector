package clients

import org.apache.storm.task.IMetricsContext
import org.apache.storm.trident.state.{State, StateFactory}
import java.util

/**
  * Created by Stefano on 19/07/2017.
  */

  class MongoStateFactory(var options: MongoState#Options) extends StateFactory {

    override def makeState(conf: util.Map[_, _], metrics: IMetricsContext, partitionIndex: Int, numPartitions: Int): State = {
      val state = new MongoState(conf, this.options)
      state.prepare()
      state
    }
}
