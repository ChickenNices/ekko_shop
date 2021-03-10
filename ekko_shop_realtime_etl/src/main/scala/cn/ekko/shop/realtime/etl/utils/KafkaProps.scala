package cn.ekko.shop.realtime.etl.utils

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerConfig

/**
 * @Author Ekko
 * @Date 2021/3/5 上午 09:04
 * @Version 1.0
 *
 *         定义kafka的属性配置类
 */
object KafkaProps {
  /**
   * 返回封装好的kafka配置项信息
   */
  def getKafkaProperties()={
    val props = new Properties();
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,GlobalConfigUtil.`bootstrap.servers`)
    // TODO: zookeeper地址
    //zookeeper地址

    props.setProperty(ConsumerConfig.GROUP_ID_CONFIG,GlobalConfigUtil.`group.id`)

    props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,GlobalConfigUtil.`enable.auto.commit`)

    props.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,GlobalConfigUtil.`auto.commit.interval.ms`)

    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,GlobalConfigUtil.`auto.offset.reset`)

    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,GlobalConfigUtil.`key.serializer`)

    //返回封装后的配置项
    props
  }
 }
