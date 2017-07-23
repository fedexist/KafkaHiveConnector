package clients.mongo

import java.util

import org.apache.storm.trident.operation.TridentCollector
import org.apache.storm.trident.state.BaseStateUpdater
import org.apache.storm.trident.tuple.TridentTuple

class MongoStateUpdater() extends BaseStateUpdater[MongoState] {

  override def updateState(state: MongoState, tuples: util.List[TridentTuple], collector: TridentCollector): Unit = {
    state.updateState(tuples, collector)
  }

}