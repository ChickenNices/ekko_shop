package cn.ekko.canal.bean;

import cn.ekko.canal.protobuf.ProtoBufable;
import cn.ekko.canal.protobuf.CanalModel;
import com.alibaba.fastjson.JSON;
import com.google.protobuf.InvalidProtocolBufferException;

import java.util.HashMap;
import java.util.Map;

/**
 * @Author Ekko
 * @Date 2021/2/25 下午 06:58
 * @Version 1.0
 *
 * canal数据的protobuf的实现类
 * 能够使用protobuf序列化成bean对象
 *
 * 将binlog解析后的map对象,转换成protobuf序列化后的字节码数据 最终写入到kafka集群
 */
public class CanalRowData implements ProtoBufable {

    private String logfilename;
    private Long logfileoffset;
    private Long executeTime;
    private String schemaName;
    private String tableName;
    private String eventType;
    private Map<String, String> columns;

    public String getLogfilename() {
        return logfilename;
    }

    public void setLogfilename(String logfilename) {
        this.logfilename = logfilename;
    }

    public Long getLogfileoffset() {
        return logfileoffset;
    }

    public void setLogfileoffset(Long logfileoffset) {
        this.logfileoffset = logfileoffset;
    }

    public Long getExecuteTime() {
        return executeTime;
    }

    public void setExecuteTime(Long executeTime) {
        this.executeTime = executeTime;
    }

    public String getSchemaName() {
        return schemaName;
    }

    public void setSchemaName(String schemaName) {
        this.schemaName = schemaName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getEventType() {
        return eventType;
    }

    public void setEventType(String eventType) {
        this.eventType = eventType;
    }

    public Map<String, String> getColumns() {
        return columns;
    }

    public void setColumns(Map<String, String> columns) {
        this.columns = columns;
    }

    public CanalRowData(){

    }


    /**
     *定义构造方法解析binlog日志
     */
    public CanalRowData(Map map){
        //解析map对象所有的参数
        if (map.size() > 0){
            this.logfilename = map.get("logfileName").toString();
            this.logfileoffset = Long.parseLong(map.get("logfileOffset").toString());
            this.executeTime = Long.parseLong(map.get("executeTime").toString());
            this.schemaName = map.get("schemaName").toString();
            this.tableName = map.get("tableName").toString();
            this.eventType = map.get("eventType").toString();
            this.columns = (Map<String, String>)map.get("columns");
        }
    }
    /**
     *传递一个字节码数据 将字节码数据反序列化成对象
     */
    public CanalRowData(byte[] bytes){
        try {

            //将字节码数据反序列化成对象
            CanalModel.RowData rowData = CanalModel.RowData.parseFrom(bytes);
            this.logfilename = rowData.getLogfileName();
            this.logfileoffset = rowData.getLogfileOffset();
            this.executeTime = rowData.getExecuteTime();
            this.schemaName = rowData.getSchemaName();
            this.tableName = rowData.getTableName();
            this.eventType = rowData.getEventType();

            //将所有的列的集合添加到map中
            this.columns = new HashMap<>();
            this.columns.putAll(rowData.getColumnsMap());


        } catch (InvalidProtocolBufferException e) {
            e.printStackTrace();
        }
    }

    /**
     *需要将map对象解析出来的参数,赋值给protobuF对象,然后序列化后返回
     */
    @Override
    public byte[] toByte() {
        CanalModel.RowData.Builder builder = CanalModel.RowData.newBuilder();
        builder.setLogfileName(this.logfilename);
        builder.setLogfileOffset(this.logfileoffset);
        builder.setExecuteTime(this.executeTime);
        builder.setTableName(this.tableName);
        builder.setEventType(this.eventType);
        for (String key : this.columns.keySet()) {
            builder.putColumns(key, this.columns.get(key));
        }
        //将传递的binlog数据解析后序列化成字节码数据返回
        return builder.build().toByteArray();
    }

    @Override
    public String toString() {
        return JSON.toJSONString(this);
    }
}
