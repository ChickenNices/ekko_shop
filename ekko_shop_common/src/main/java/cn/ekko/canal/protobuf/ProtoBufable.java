package cn.ekko.canal.protobuf;

/**
 * @Author Ekko
 * @Date 2021/2/25 上午 09:41
 * @Version 1.0
 *
 * 定义protobuf的序列化接口
 * 这个接口定义的是返回的byte[] 二进制字节码对象
 * 所有的能使用protobuf序列化的bean都需要实现该接口
 */
public interface ProtoBufable {
    /**
     *将对象转换成二进制字节数组
     */
    byte[] toByte();
}
