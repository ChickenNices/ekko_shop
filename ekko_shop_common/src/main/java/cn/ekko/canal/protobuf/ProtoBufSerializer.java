package cn.ekko.canal.protobuf;

import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

/**
 * @Author Ekko
 * @Date 2021/2/25 上午 09:33
 * @Version 1.0
 *
 *  数据写入到kafka中
 *  需要遵循kafka的序列化方式
 *  传递的泛型必须是继承自ProtoBufable接口的实现类,才可以被序列化成功
 */
public class ProtoBufSerializer implements Serializer<ProtoBufable> {


    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {

    }

    @Override
    public byte[] serialize(String topic, ProtoBufable data) {
        return data.toByte();
    }

    @Override
    public void close() {

    }
}
