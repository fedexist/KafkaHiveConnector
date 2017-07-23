package clients.hbase

import org.apache.storm.hbase.bolt.mapper.HBaseMapper
import org.apache.storm.hbase.common.ColumnList
import org.apache.storm.tuple.Tuple

class HBaseFlightsMapper extends HBaseMapper {
  def columns(tuple: Tuple): ColumnList = ???

  def rowKey(tuple: Tuple): Array[Byte] = ???
}
