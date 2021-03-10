package cn.ekko.shop.realtime.etl.app

import cn.ekko.shop.realtime.etl.`trait`.TestETL
import cn.ekko.shop.realtime.etl.process.SyncDimData
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
/**
 * @Author Ekko
 * @Date $MONTH--2021/3/1 上午 09:54
 * @Version 1.0
 *
 *         创建实时etl模块的驱动类
 *         1.
 *         2.
 *
 */
object App {

  def main(args: Array[String]): Unit = {


    // TODO: 设置hadoop权限
    System.setProperty("HADOOP_USER_NAME","foo")

    //1.初始化flink流式运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment


    //2.将flink默认的开发环境并行度设置为1,生产环境尽可能使用client递交作业时指定并行度
    env.setParallelism(1)


    //3.配置checkpont,运行周期,5s一次checkpiont
    env.enableCheckpointing(5000)

    //当作业被取消的时候,保留以前的checkpoint,避免数据丢失
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    //设置同一个时间只能有一个检查点
  env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)

    //指定重启策略,异常重启五次,每次延迟5s如果超过5次,程序退出
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(5,5000))


    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // checkpoint的HDFS保存位置
    env.setStateBackend(new FsStateBackend("hdfs://hadoop001:9000/flink/checkpoint"))
    // 配置两次checkpoint的最小时间间隔
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(1000)
    // 配置最大checkpoint的并行度
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // 配置checkpoint的超时时长
    env.getCheckpointConfig.setCheckpointTimeout(60000)



    //  编写测试代码,
    //4.接入kafka的数据源,消费kafka的数据

    //5实现所有ETL业务

    new TestETL(env).process()
    //6.执行任务
    env.execute("runtime_etl")
  }
}
