package cn.ekko.shop.realtime.etl.dataloader

import java.sql.{Connection, DriverManager, Statement}

import cn.ekko.shop.realtime.etl.bean.{DimGoodsDBEntity, DimOrgDBEntity, DimShopCatDBEntity, DimShopsDBEntity}
import cn.ekko.shop.realtime.etl.utils.GlobalConfigUtil
import cn.ekko.shop.realtime.etl.utils.RedisUtil.RedisUtil
import cn.ekko.shop.realtime.etl.bean.DimShopCatDBEntity.DimGoodsCatDBEntity
import cn.ekko.shop.realtime.etl.bean.DimShopCatDBEntity
import cn.ekko.shop.realtime.etl.utils.RedisUtil.RedisUtil
import com.alibaba.fastjson.JSON
import com.alibaba.fastjson.serializer.SerializerFeature
import redis.clients.jedis.Jedis

/**
 * @Author Ekko
 * @Date $MONTH--2021/3/5 下午 08:01
 * @Version 1.0
 *
 *         维度表数据的全量装载实现类
 *         需要将5个维度表的数据同步到redis中
 *         商品维度表
 *         商品分类维度表
 *         店铺表
 *         组织机构表
 *         门店商品分类表
 */
object DimensionDataLoader {
  def main(args: Array[String]): Unit = {


    //1.注册mysql的驱动
    Class.forName("com.mysql.jdbc.Driver")
    //2.创建连接
    val connection = DriverManager.getConnection(s"jdbc:mysql://${GlobalConfigUtil.`mysql.server.ip`}:${GlobalConfigUtil.`mysql.server.port`}/${GlobalConfigUtil.`mysql.server.database`}",
      GlobalConfigUtil.`mysql.server.username`, GlobalConfigUtil.`mysql.server.password`
    )
    //3.创建redis的连接
    val jedis = RedisUtil.getJedis()
    //redis 中默认有16个数据库,需要指定一下维度数据保存到那个数据库中,默认是保存到第一个数据库
    jedis.select(1)

    //4.加载维度表的数据到redis中
    //执行
    LoadDimGods(connection,jedis )
    loadDimShops(connection,jedis)
    loadDimOrg(connection,jedis)
    loadDimGoodsCats(connection,jedis)
    LoadDimShopCats(connection,jedis)

    //退出
    System.exit(0)

  }

  /**
   * 1. 商品维度表
   * @param connection
   * @param jedis
   */
  def LoadDimGods(connection:Connection,jedis:Jedis)={
    val sql=
      """
        |SELECT
        |	goodsId,
        |	goodsName,
        |	shopId,
        |	goodsCatId,
        |	shopPrice
        |FROM
        |	ekko_goods
        |""".stripMargin

    //创建statement
    val statement: Statement = connection.createStatement()
    val resultSet = statement.executeQuery(sql)

    //遍历商品表的数据
    while(resultSet.next()){
      val goodsId = resultSet.getLong("goodsId")
      val goodsName = resultSet.getString("goodsName")
      val shopId = resultSet.getLong("shopId")
      val goodsCatId = resultSet.getInt("goodsCatId")
      val shopPrice = resultSet.getDouble("shopPrice")

      //需要将获取到的商品维度表数据写入到redis中
      //redis是一个k/v数据库,需要将以上五个字段封装成json结构保存到reids中
     val goodsDBEntity: DimGoodsDBEntity = DimGoodsDBEntity(goodsId,goodsName,shopId,goodsCatId,shopPrice)


      //将样类例转换成json字符串写入到redis中
      val json = JSON.toJSONString(goodsDBEntity,SerializerFeature.DisableCircularReferenceDetect)
      jedis.hset("ekko_shop:dim_goods",goodsId.toString,json)
    }
    resultSet.close()
    statement.close()
  }

  // 加载商铺维度数据到Redis
  // 加载商铺维度数据到Redis
  def loadDimShops(connection: Connection, jedis: Jedis) = {
    val sql =
      """
        |SELECT
        |	t1.`shopId`,
        |	t1.`areaId`,
        |	t1.`shopName`,
        |	t1.`shopCompany`
        |FROM
        |	ekko_shops t1
      """.stripMargin

    val statement = connection.createStatement()
    val resultSet = statement.executeQuery(sql)

    while (resultSet.next()) {
      val shopId = resultSet.getInt("shopId")
      val areaId = resultSet.getInt("areaId")
      val shopName = resultSet.getString("shopName")
      val shopCompany = resultSet.getString("shopCompany")

      val dimShop = DimShopsDBEntity(shopId, areaId, shopName, shopCompany)
      println(dimShop)
      jedis.hset("ekko_shop:dim_shops", shopId + "", JSON.toJSONString(dimShop, SerializerFeature.DisableCircularReferenceDetect))
    }

    resultSet.close()
    statement.close()
  }

  def loadDimOrg(connection: Connection, jedis: Jedis) = {
    val sql = """
                |SELECT
                |	orgid,
                |	parentid,
                |	orgName,
                |	orgLevel
                |FROM
                |	ekko_org
              """.stripMargin

    val statement = connection.createStatement()
    val resultSet = statement.executeQuery(sql)

    while(resultSet.next()) {
      val orgId = resultSet.getInt("orgId")
      val parentId = resultSet.getInt("parentId")
      val orgName = resultSet.getString("orgName")
      val orgLevel = resultSet.getInt("orgLevel")

      val entity = DimOrgDBEntity(orgId, parentId, orgName, orgLevel)

      jedis.hset("ekko_shop:dim_org", orgId + "", JSON.toJSONString(entity, SerializerFeature.DisableCircularReferenceDetect))
    }
  }

  def loadDimGoodsCats(connection: Connection, jedis: Jedis) = {
    val sql = """
                |SELECT
                |	t1.`catId`,
                |	t1.`parentId`,
                |	t1.`catName`,
                |	t1.`cat_level`
                |FROM
                |	ekko_goods_cats t1
              """.stripMargin

    val statement = connection.createStatement()
    val resultSet = statement.executeQuery(sql)

    while(resultSet.next()) {
      val catId = resultSet.getString("catId")
      val parentId = resultSet.getString("parentId")
      val catName = resultSet.getString("catName")
      val cat_level = resultSet.getString("cat_level")

      val entity = DimGoodsCatDBEntity(catId, parentId, catName, cat_level)
      println(entity)

      jedis.hset("ekko_shop:dim_goods_cats", catId, JSON.toJSONString(entity, SerializerFeature.DisableCircularReferenceDetect))
    }

    resultSet.close()
    statement.close()
  }

  // 加载门店商品分类维度数据到Redis
  def LoadDimShopCats(connection: Connection, jedis: Jedis): Unit ={
    val sql = """
                |SELECT
                |	t1.`catId`,
                |	t1.`parentId`,
                |	t1.`catName`,
                |	t1.`catSort`
                |FROM
                |	ekko_shop_cats t1
              """.stripMargin

    val statement = connection.createStatement()
    val resultSet = statement.executeQuery(sql)

    while(resultSet.next()) {
      val catId = resultSet.getString("catId")
      val parentId = resultSet.getString("parentId")
      val catName = resultSet.getString("catName")
      val cat_level = resultSet.getString("catSort")

      val entity = DimShopCatDBEntity(catId, parentId, catName, cat_level)
      println(entity)

      jedis.hset("ekko_shop:dim_shop_cats", catId, JSON.toJSONString(entity, SerializerFeature.DisableCircularReferenceDetect))
    }

    resultSet.close()
    statement.close()
  }
}
