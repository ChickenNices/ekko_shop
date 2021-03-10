package cn.ekko.shop.realtime.etl.utils

import cn.ekko.canal.bean.CanalRowData
import cn.ekko.canal.protobuf.CanalModel.RowData
import org.apache.flink.api.common.serialization.AbstractDeserializationSchema

/**
 * @Author Ekko
 * @Date $MONTH--2021/3/5 下午 04:30
 * @Version 1.0
 *
 *         自定义反序列化实现类,继承AbstractDeserializationSchema
 *         SimpleStringSchema
 */
class CanalRowDataDeserialzerSchema extends AbstractDeserializationSchema[CanalRowData]{
  //
  override def deserialize(bytes: Array[Byte]): CanalRowData =  {
    //将二进制字节码数据转换成CanalRowData对象返回
    new CanalRowData(bytes)
  }
}
