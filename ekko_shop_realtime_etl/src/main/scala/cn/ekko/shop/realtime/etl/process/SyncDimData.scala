package cn.ekko.shop.realtime.etl.process

import cn.ekko.canal.bean.CanalRowData
import cn.ekko.shop.realtime.etl.`trait`.MysqlBaseETL
import cn.ekko.shop.realtime.etl.bean.DimShopCatDBEntity.DimGoodsCatDBEntity
import cn.ekko.shop.realtime.etl.bean.{DimGoodsDBEntity, DimOrgDBEntity, DimShopCatDBEntity, DimShopsDBEntity}
import cn.ekko.shop.realtime.etl.utils.RedisUtil
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.sink.{RichSinkFunction, SinkFunction}
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import redis.clients.jedis.Jedis

/**
 * @Author Ekko
 * @Date 2021/3/8 上午 08:28
 * @Version 1.0
 *          增量更新维度数据到redis中
 */
case class SyncDimData(env: StreamExecutionEnvironment) extends MysqlBaseETL(env) {



  //定义redis对象
  var jedis: Jedis = _

  override def process(): Unit = {
    //获取数据源
    val canalDataStream: DataStream[CanalRowData] = getKafkaDataStream()

    //过滤出维度表
    val dimRowDataStream: DataStream[CanalRowData] = canalDataStream.filter {
      rowData =>
        //模式匹配
        rowData.getTableName match {
          case "ekko_goods" => true
          case "ekko_shops" => true
          case "ekko_org" => true
          case "ekko_goods_cats" => true
          case "ekko_shop_cats" => true
          //加上else,比曼抛出异常
          case _ => false
        }
    }



    //处理同步过来的数据更新到redis中
    dimRowDataStream.addSink(new RichSinkFunction[CanalRowData] {

      //初始化外部数据源,只被调用一次
      override def open(parameters: Configuration): Unit = {
        //获取连接
        val jedis = RedisUtil.getJedis()
        //维度数据在第二个数据中
        jedis.select(1)


      }

      //释放数据源,只被调用一次
      override def close(): Unit = {

        if (jedis.isConnected) {
          jedis.close()
        }

      }

      //处理数据,一条一条的处理
      override def invoke(rowData: CanalRowData, context: SinkFunction.Context[_]): Unit = {
        //根据操作类型的不同,调用不同的业务逻辑实现数据的更新
        rowData.getEventType match {
          case eventType if (eventType == "insert" || eventType == "update") => updateDimData(rowData)
          case "delete" => deleteDimData(rowData)
          case _ =>
        }
      }

      /**
       * 更新维度数据
       */
      def updateDimData(rowData: CanalRowData): Unit = {
        //TODO 区分出来是操作的哪张维度表
        rowData.getTableName match {
          case "ekko_goods" => {
            //商品维表
            val goodsId = rowData.getColumns.get("goodsId").toLong
            val goodsName = rowData.getColumns.get("goodsName")
            val shopId = rowData.getColumns.get("shopId").toLong
            val goodsCatId = rowData.getColumns.get("goodsCatId").toInt
            val shopPrice = rowData.getColumns.get("shopPrice").toDouble

            //需要将获取到的商品维度表数据写入到redis中
            //redis是一个k/v数据库,需要将以上五个字段封装成json结构保存到reids中
            val goodsDBEntity: DimGoodsDBEntity = DimGoodsDBEntity(goodsId, goodsName, shopId, goodsCatId, shopPrice)
            jedis.hset("ekko_shop:dim_goods", goodsId.toString, JSON.toJSONString(goodsDBEntity, SerializerFeature.DisableCircularReferenceDetect))
          }
          case "ekko_shops" => {
            //店铺维表
            val shopId = rowData.getColumns.get("shopId").toInt
            val areaId = rowData.getColumns.get("areaId").toInt
            val shopName = rowData.getColumns.get("shopName")
            val shopCompany = rowData.getColumns.get("shopCompany")

            val dimShop = DimShopsDBEntity(shopId, areaId, shopName, shopCompany)

            jedis.hset("ekko_shop:dim_shops", shopId + "", JSON.toJSONString(dimShop, SerializerFeature.DisableCircularReferenceDetect))

          }
          case "ekko_org" => {
            //组织机构维表
            val orgId = rowData.getColumns.get("orgId").toInt
            val parentId = rowData.getColumns.get("parentId").toInt
            val orgName = rowData.getColumns.get("orgName")
            val orgLevel = rowData.getColumns.get("orgLevel").toInt

            val entity = DimOrgDBEntity(orgId, parentId, orgName, orgLevel)
            println(entity)
            jedis.hset("ekko_shop:dim_org", orgId + "", JSON.toJSONString(entity, SerializerFeature.DisableCircularReferenceDetect))

          }
          case "ekko_goods_cats" => {
            //商品分类维表
            val catId = rowData.getColumns.get("catId")
            val parentId = rowData.getColumns.get("parentId")
            val catName = rowData.getColumns.get("catName")
            val cat_level = rowData.getColumns.get("cat_level")

            val entity = DimGoodsCatDBEntity(catId, parentId, catName, cat_level)


            jedis.hset("ekko_shop:dim_goods_cats", catId, JSON.toJSONString(entity, SerializerFeature.DisableCircularReferenceDetect))


          }
          case "ekko_shop_cats" => {
            //门店商品分类维表

            val catId = rowData.getColumns.get("catId")
            val parentId = rowData.getColumns.get("parentId")
            val catName = rowData.getColumns.get("catName")
            val cat_level = rowData.getColumns.get("catSort")

            val entity = DimShopCatDBEntity(catId, parentId, catName, cat_level)


            jedis.hset("ekko_shop:dim_shop_cats", catId, JSON.toJSONString(entity, SerializerFeature.DisableCircularReferenceDetect))


          }

        }
      }

      /**
       * 删除维度数据
       */
      def deleteDimData(rowData: CanalRowData) = {

        rowData.getTableName match {
          case "ekko_goods" => {
            //商品维表
            jedis.del("ekko_shop:dim_goods", rowData.getColumns.get("goodsId"))
          }
          case "ekko_shops" => {
            //店铺维表
            jedis.del("ekko_shop:dim_shops", rowData.getColumns.get("shopId"))

          }
          case "ekko_org" => {
            //组织机构维表
            jedis.del("ekko_shop:dim_org", rowData.getColumns.get("catId"))

          }
          case "ekko_goods_cats" => {
            //商品分类维表
            jedis.del("ekko_shop:dim_goods_cats", rowData.getColumns.get("orgId"))


          }
          case "ekko_shop_cats" => {
            //门店商品分类维表


            jedis.del("ekko_shop:dim_shop_cats", rowData.getColumns.get("catId"))


          }

        }
      }

    })

  }
}
