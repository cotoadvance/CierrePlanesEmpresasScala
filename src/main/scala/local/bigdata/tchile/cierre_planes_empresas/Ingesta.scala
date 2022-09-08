package local.bigdata.tchile.cierre_planes_empresas

import org.apache.spark.SparkConf
import org.apache.spark.sql.functions.regexp_replace
import org.apache.spark.sql.SparkSession

object Ingesta {
  def main(args: Array[String]): Unit = {
    // ---------------------------------------------------------------
    // INICIO PARAMETRIZACION
    // ---------------------------------------------------------------

    // BASE

    val conf = new SparkConf().setMaster("yarn").setAppName("B2B").set("spark.driver.allowMultipleContexts", "true").set("spark.yarn.queue", "default").set("spark.sql.crossJoin.enabled", "true").set("spark.sql.debug.maxToStringFields", "1000").set("spark.sql.autoBroadcastJoinThreshold", "-1")
    val spark: SparkSession = SparkSession.builder().config(conf).config("spark.sql.warehouse.dir", "/usr/hdp/current/spark-client/conf/hive-site.xml").config("spark.submit.deployMode", "cluster").enableHiveSupport().getOrCreate()
    import spark.implicits._

    // ACCESO EXADATA

    val exaDriver = "oracle.jdbc.OracleDriver"
    val exaUrl = "jdbc:oracle:thin:@(DESCRIPTION =(ENABLE=BROKEN)(ADDRESS=(PROTOCOL=TCP)(HOST=smt-scan.tchile.local)(PORT=1521))(CONNECT_DATA=(SERVER=DEDICATED)(SERVICE_NAME=EXPLOTA)))"
    val exaUser = args(1)
    val exaPass = args(2)

    // PATHS

    val hdfsPath = "/data/parque_asignado/parque/b2b/scel/planes_empresa/par_planes_dsc_gpl_v2"

    // ORIGEN EXADATA

    val schema = "ODSCHILE"
    val prdDiscountBlngOffersTable = "PRD_DISCOUNT_BLNG_OFFERS"
    val productTable = "PRODUCT"

    // ---------------------------------------------------------------
    // FIN PARAMETRIZACION
    // ---------------------------------------------------------------

    // ---------------------------------------------------------------
    // INICIO PROCESO
    // ---------------------------------------------------------------

    val prdDiscountBlngOffers = spark.read.format("jdbc").
      option("url", exaUrl).
      option("driver", exaDriver).
      option("dbtable", s"$schema.$prdDiscountBlngOffersTable").
      option("user", exaUser).
      option("password", exaPass).
      option("numPartitions", 2).
      load()

    val product = spark.read.format("jdbc").
      option("url", exaUrl).
      option("driver", exaDriver).
      option("dbtable", s"$schema.$productTable").
      option("user", exaUser).
      option("password", exaPass).
      option("numPartitions", 2).
      load()

    prdDiscountBlngOffers.
      withColumn("PRODUCT_KEY", regexp_replace($"APPLIED_TO_IND", "[*;*;RC;]", "")).
      join(product, Seq("PRODUCT_KEY"), "left").
      filter(!$"PRODUCT_DESC".isNull).
      select($"PRODUCT_KEY", $"PRODUCT_DESC").
      distinct().
      repartition(1).
      write.mode("overwrite").
      parquet(hdfsPath)

    // ---------------------------------------------------------------
    // FIN PROCESO
    // ---------------------------------------------------------------

    spark.close()
  }
}