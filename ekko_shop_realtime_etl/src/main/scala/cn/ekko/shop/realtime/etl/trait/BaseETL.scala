package cn.ekko.shop.realtime.etl.`trait`

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.DataStream
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper

/**
 * @Author Ekko
 * @Date $MONTH--2021/3/5 上午 08:19
 * @Version 1.0
 *         定义特质(接口)抽取所有etl操作公共的方法
 */
trait BaseETL[T] {

  /*
   * 所有的etl操作都会操作kafka,抽取kafka读取方法
   */
  def getKafkaDataStream(topic:String):DataStream[T];


    //
  //根据业务抽取出来process方法(因为所有的ETL都有操作方法)
  def process()

}
