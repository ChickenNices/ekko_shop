package cn.ekko.shop.realtime.etl.`trait`

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment

/**
 * @Author Ekko
 * @Date $MONTH--2021/3/9 下午 09:32
 * @Version 1.0
 */
class TestETL(env:StreamExecutionEnvironment) extends MysqlBaseETL(env) {
  override def process(): Unit = {
    System.out.println("===调用process==")
    getKafkaDataStream().print()
  }
}
