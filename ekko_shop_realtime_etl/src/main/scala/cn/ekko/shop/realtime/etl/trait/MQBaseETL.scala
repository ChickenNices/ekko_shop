package cn.ekko.shop.realtime.etl.`trait`

import cn.ekko.shop.realtime.etl.utils.KafkaProps
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011
import org.apache.flink.streaming.api.scala._



/**
 * @Author Ekko
 * @Date $MONTH--2021/3/5 上午 08:35
 * @Version 1.0
 *          根据数据的来源不同,可以抽象出来两个抽象类
 *     主要消费日志数据和购物车以及评论数据,存储的是字符串,直接kafka的String 反序列化
 *
 */
abstract class MQBaseETL(env:StreamExecutionEnvironment) extends BaseETL [String]{

  //根据业务抽取出来的kafk读取方法
  override def getKafkaDataStream(topic: String): DataStream[String] = {

    //编写kafka消费者对象实例
    val kafkaProducer: FlinkKafkaConsumer011[String] = new FlinkKafkaConsumer011[String](
      topic,
      new SimpleStringSchema(),
      KafkaProps.getKafkaProperties()
    )
    //将消费者对象添加到数据源

      val logDataStream: DataStream[String] = env.addSource(kafkaProducer)


    //返回消费到的数据
    logDataStream
  }


}
