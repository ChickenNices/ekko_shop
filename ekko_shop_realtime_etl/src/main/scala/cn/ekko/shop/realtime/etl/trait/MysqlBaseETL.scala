package cn.ekko.shop.realtime.etl.`trait`

import cn.ekko.canal.bean.CanalRowData
import cn.ekko.shop.realtime.etl.utils.{CanalRowDataDeserialzerSchema, GlobalConfigUtil, KafkaProps}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.api.scala._
/**
 * @Author Ekko
 * @Date $MONTH--2021/3/5 上午 08:34
 * @Version 1.0
 *
 *         根据数据的来源不同,可以抽象出来两个抽象类
 *     消费canaltopic中的数据,需要将消费到的字节码数据反序列化成canalRowdata
 */
abstract  class MysqlBaseETL(env:StreamExecutionEnvironment) extends BaseETL [CanalRowData]{

  override def getKafkaDataStream(topic: String = GlobalConfigUtil.`input.topic.canal`): DataStream[CanalRowData] = {
    //反序列化

   val canalKafkaConsumer =  new FlinkKafkaConsumer011[CanalRowData](
      topic,
      //
     new CanalRowDataDeserialzerSchema(),
     KafkaProps.getKafkaProperties()
    )
    //将消费者实例添加到环境中
    val canalDataStream = env.addSource(canalKafkaConsumer)
    //返回消费到的数据
    canalDataStream

  }

}
