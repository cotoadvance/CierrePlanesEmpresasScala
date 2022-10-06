package local.bigdata.tchile.cierre_planes_empresas

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.hadoop.fs.{FileSystem, Path}

object nuevoProcesoDiario extends App {

  val conf = new SparkConf().setMaster("yarn").setAppName("B2B").set("spark.driver.allowMultipleContexts", "true").set("spark.yarn.queue", "default").set("spark.sql.crossJoin.enabled", "true").set("spark.sql.debug.maxToStringFields", "1000").set("spark.sql.autoBroadcastJoinThreshold", "-1")
  val spark: SparkSession = SparkSession.builder().config(conf).config("spark.sql.warehouse.dir", "/usr/hdp/current/spark-client/conf/hive-site.xml").config("spark.submit.deployMode", "cluster").enableHiveSupport().getOrCreate()

  val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)

  val hiveTable = "fil_planes_empresa"
  val conformadoBasePath = s"/data/parque_asignado/parque/b2b/scel/planes_empresa/$hiveTable"

  val parqueMovilKfactorBasePath = "/data/producto_asignado/parque_movil/kfactor/tbln_parque_full/raw"
  val parqueMovilKfactorLastYear = fs.listStatus(new Path(parqueMovilKfactorBasePath)).filter(_.isDirectory()).map(_.getPath.toString).sortWith(_ > _)(0)
  val parqueMovilKfactorLastYearMonth = fs.listStatus(new Path(parqueMovilKfactorLastYear)).filter(_.isDirectory()).map(_.getPath.toString).sortWith(_ > _)(0)
  val parqueMovilKfactorPath = fs.listStatus(new Path(parqueMovilKfactorLastYearMonth)).filter(_.isDirectory()).map(_.getPath.toString).sortWith(_ > _)(0)

  val parqueMovilKfactor=spark.read.parquet(parqueMovilKfactorPath).select("SUBSCRIBER_KEY","PRODUCT","financial_account_key")


  val detalleFacturacion = spark.read.parquet("/modelos/finanzas/detalle_facturacion/conformado").
    withColumn("SUBSCRIBER_KEY", trim(col("SUBSCRIBER_KEY"))).
    filter(
      col("bill_date").between(trunc(current_timestamp(),"Month"),last_day(current_timestamp()))
        and col("customer_sub_type_desc").isin("Corporaciones", "Gran Empresa", "Mediana", "Pequeña", "Micro", "Empresa Nueva", "Empresa Relacionada")
        and col("payment_category_desc").isin("Postpaid Payment", "Hybrid Payment")
        and col("charge_code_id").isin("RM_VOZ_PL_POS", "RM_DAT_PL_POS", "RM_VOZ_PL_HIB", "RM_DAT_PL_HIB", "RM_M2M_PL1", "RM_BAM_PL_POS", "RM_GPS_PL1", "OM_EQU_SUBS")
    ).
    groupBy("SUBSCRIBER_KEY").
    agg(sum("charge_amount").cast("int").as("arpu"))

  val subscribers = spark.read.parquet("/modelos/ods/subscribers/conformado").
    filter(
      col("SUBSCRIBER_STATUS_KEY").isin(1376, 1378, 1379)
        and col("CALCULATED_PAYMENT_CATEGORY_KE").isin("1402", "1403")
      and !col("PRIMARY_RESOURCE_VALUE").cast("bigint").isNull
    ).
    select(
      "SUBSCRIBER_KEY",
      "PRIMARY_RESOURCE_VALUE",
      "CUSTOMER_KEY",
      "ORIGINAL_ACTIVATION_DATE"
    )

  val customer = spark.read.parquet("/modelos/ods/customer/conformado").
    filter(
      col("CUSTOMER_TYPE_IND") === "C"
        and col("rut_id") =!= "#"
        and col("DML_IND") =!= "DELETE"
        and col("customer_sub_type_key").
        isin(3042, 3043, 3048, 3053, 3055, 3059, 3060, 3081, 3084)
    ).
    select(
      "RUT_ID",
      "CUSTOMER_LEGAL_NAME",
      "CUSTOMER_KEY",
      "CUSTOMER_SUB_TYPE_KEY",
      "BILLING_CYCLE_KEY"
    )

  val customerSubType = spark.read.parquet("/modelos/ods/customer_sub_type/conformado").
    filter(col("customer_sub_type_key").
    isin(3042, 3043, 3048, 3053, 3055, 3059, 3060, 3081, 3084)).
    select(col("CUSTOMER_SUB_TYPE_KEY"),
      when(col("CUSTOMER_SUB_TYPE_DESC").isin("Mediana Estándar","Mediana"),"Mediana Empresa").
        when(col("CUSTOMER_SUB_TYPE_DESC").isin("Pequeña Estándar","Pequeña"),"Pequeña Empresa").
        otherwise(col("CUSTOMER_SUB_TYPE_DESC")).as("CUSTOMER_SUB_TYPE_DESC"))

  val planABO = spark.read.parquet("/modelos/ods/assigned_billing_offer/conformado").
    filter(
      col("ASGN_BLNG_OFR_STATE_KEY") === 1331
        and col("ASGN_BLNG_OFR_STATUS_KEY").isin(1351, 1359)
        and col("PARENT_PRODUCT_KEY") === "2053"
        and col("END_DATE") > current_date()
    ).
    withColumn("ORDEN",
      row_number().over(Window.partitionBy(col("SUBSCRIBER_KEY")).
        orderBy(col("START_DATE").desc))).
    filter(col("ORDEN")===1).
    select("SUBSCRIBER_KEY", "ASSIGNED_PRODUCT_ID", "PRODUCT_KEY")

  val product = spark.read.parquet("/modelos/ods/product/conformado").
    select(col("PRODUCT_KEY"),col("PRODUCT_DESC").as("PRODUCT_DESC_PLAN"))

  val cargosFijosPBP = spark.read.parquet("/modelos/ods/product_billing_parameters/conformado").
    filter(
      col("BILLING_OFFER_EXPIRATION_DATE").isNull
        and col("PACKAGE_EXPIRATION_DATE").isNull
        and col("DML_IND") =!= "DELETE"
        and col("BILLING_ATTRIBUTE_NAME").isin("Rate", "Discount value")
    ).
    select("PRODUCT_KEY", "ITEM_NAME", "BILLING_ATTRIBUTE_DEFAULT_VAL")


  val cuentasFinancieras = spark.read.parquet(parqueMovilKfactorPath).
    filter(
      !col("SUBSCRIBER_STATUS_ID").isin("T", "C", "L")
        and col("CUSTOMER_TYPE_IND") === "C"
        and !col("financial_account_key").isNull
        and !col("subscriber_key").isNull
    ).
    select("financial_account_key", "subscriber_key").
    distinct()

  val planesPpuSchema = StructType(Array(
    StructField("cod_plan", StringType, nullable = true),
    StructField("descripcion", StringType, nullable = true)
  ))

  val planesPpu = spark.read.
    schema(planesPpuSchema).
    options(Map("delimiter" -> ",", "header" -> "true")).
    csv("/warehouse/staging/datalakeb2b/par_planes_ppu/par_planes_ppu.txt").
    withColumnRenamed("cod_plan", "PRODUCT_KEY")


  //val productCatalog = spark.read.parquet(productCatalogPath).
  //  select("PRODUCT_KEY", "PRODUCT_VALID_FROM_DATE")


  val parametricosAP = spark.read.parquet("/modelos/ods/assigned_product/conformado").
    filter(
      col("PRODUCT_KEY").isin(2448119)
        and col("ASSIGNED_PRODUCT_STATUS_KEY").isin(1351, 1359)
        and col("ASSIGNED_PRODUCT_STATE_KEY") === 1331
        and col("END_DATE") > current_date()
        and !col("customer_key").isNull
    ).
    withColumn("ORDEN",
      row_number().over(Window.partitionBy(col("SUBSCRIBER_KEY")).
        orderBy(col("START_DATE").desc))).
    filter(col("ORDEN")===1).
    select(
      "PRODUCT_KEY",
      "SUBSCRIBER_KEY",
      "ASSIGNED_PRODUCT_KEY",
      "ASSIGNED_PRODUCT_ID",
      "START_DATE"
    )

  val parametricosAPP = spark.read.parquet("/modelos/ods/assigned_product_parameter/conformado").
    filter(
      col("PRODUCT_PARAMETER_KEY").isin(26754, 27217, 27180, 27183, 27178)).
    select(
      "ASSIGNED_PRODUCT_KEY",
      "ASSIGNED_PRODUCT_PARAMETER_ID",
      "PARAMETER_VALUE",
      "PRODUCT_PARAMETER_KEY"
    )

  val descuentosParametricos=parametricosAP.join(parametricosAPP,Seq("ASSIGNED_PRODUCT_KEY"),"left").
    select("SUBSCRIBER_KEY","ASSIGNED_PRODUCT_PARAMETER_ID","PARAMETER_VALUE").distinct()


  val descuentosPuntualesABO = spark.read.parquet("/modelos/ods/assigned_billing_offer/conformado").
    filter(
      col("ASGN_BLNG_OFR_STATE_KEY") === 1331
        and col("ASGN_BLNG_OFR_STATUS_KEY").isin(1351, 1359)
        and col("PARENT_PRODUCT_KEY").isin("2713","3133","18865")
        and col("END_DATE") > current_date()
    ).
    withColumn("ORDEN",
      row_number().over(Window.partitionBy("SUBSCRIBER_KEY").
        orderBy("PROVISION_DATE"))).
    select("SUBSCRIBER_KEY", "ASSIGNED_PRODUCT_ID", "PRODUCT_KEY","PROVISION_DATE","ORDEN")

  val descuentosPuntualesPBP = spark.read.parquet("/modelos/ods/product_billing_parameters/conformado").
    filter(
      col("BILLING_OFFER_EXPIRATION_DATE").isNull
        and col("PACKAGE_EXPIRATION_DATE").isNull
        and col("DML_IND") =!= "DELETE"
        and col("BILLING_ATTRIBUTE_NAME").isin("Discount type", "Discount value")
      and trim(col("ITEM_NAME")).isin("Descuento Promocional Datos","Descuento Promocional Voz")
    ).
    select("PRODUCT_KEY", "ITEM_NAME", "BILLING_ATTRIBUTE_DEFAULT_VAL")

  val descuentosPuntualesProduct = spark.read.parquet("/modelos/ods/product/conformado").
    withColumn("PRODUCT_KEY", trim(col("PRODUCT_KEY"))).
    select(
      "PRODUCT_KEY",
      "PRODUCT_DESC",
      "PRICE_PLAN_PROMOTION_INTERVAL",
      "PRICE_PLAN_PROMOTION_DURATION"
    )

  val descuentosPuntuales=descuentosPuntualesABO.
    join(descuentosPuntualesPBP,Seq("PRODUCT_KEY"),"left").
    join(descuentosPuntualesProduct,Seq("PRODUCT_KEY"),"left").
    withColumn(
    "FIN",
    when(col("PRICE_PLAN_PROMOTION_INTERVAL") === "M", expr("add_months(PROVISION_DATE, PRICE_PLAN_PROMOTION_DURATION)")).
      when(col("PRICE_PLAN_PROMOTION_INTERVAL") === "D", expr("date_add(PROVISION_DATE, PRICE_PLAN_PROMOTION_DURATION)")).
      otherwise(lit(null))
  ).
    select("SUBSCRIBER_KEY","PROVISION_DATE","FIN","BILLING_ATTRIBUTE_DEFAULT_VAL","ORDEN").distinct()

  val descuentosFinDeCiclo=spark.read.parquet("/data/parque_asignado/parque/b2b/scel/planes_empresa/par_planes_dsc_gpl_v2")

  val planesEmpresas=customer.as("CUS").
    join(customerSubType.as("CST"),Seq("CUSTOMER_SUB_TYPE_KEY"),"inner").
    join(subscribers.as("SUB"),Seq("CUSTOMER_KEY"),"inner"). //1945571
    join(planABO,Seq("SUBSCRIBER_KEY"),"left").
    join(product,Seq("PRODUCT_KEY"),"left").
    join(cargosFijosPBP.filter(trim(col("ITEM_NAME"))==="Cargo Fijo Datos").as("CFD"),Seq("PRODUCT_KEY"),"left").
    join(cargosFijosPBP.filter(trim(col("ITEM_NAME"))==="Cargo Fijo Voz").as("CFV"),Seq("PRODUCT_KEY"),"left").
    join(cargosFijosPBP.filter(trim(col("ITEM_NAME"))==="Descuento Multiproducto Datos").as("DFD"),Seq("PRODUCT_KEY"),"left").
    join(cargosFijosPBP.filter(trim(col("ITEM_NAME"))==="Descuento Multiproducto Voz").as("DFV"),Seq("PRODUCT_KEY"),"left").
    join(descuentosParametricos.filter(trim(col("ASSIGNED_PRODUCT_PARAMETER_ID"))==="DiscountaAmountDataRC").as("DPDA"),Seq("SUBSCRIBER_KEY"),"left").
    join(descuentosParametricos.filter(trim(col("ASSIGNED_PRODUCT_PARAMETER_ID"))==="DiscountTypeData").as("DPDT"),Seq("SUBSCRIBER_KEY"),"left").
    join(descuentosParametricos.filter(trim(col("ASSIGNED_PRODUCT_PARAMETER_ID"))==="DiscountAmountVoiceRC").as("DPVA"),Seq("SUBSCRIBER_KEY"),"left").
    join(descuentosParametricos.filter(trim(col("ASSIGNED_PRODUCT_PARAMETER_ID"))==="DiscountTypeVoice").as("DPVT"),Seq("SUBSCRIBER_KEY"),"left").
    join(descuentosPuntuales.filter(col("ORDEN")===1 and !col("BILLING_ATTRIBUTE_DEFAULT_VAL").cast("int").isNull).as("DP1"),Seq("SUBSCRIBER_KEY"),"left").
    join(descuentosPuntuales.filter(col("ORDEN")===1 and col("BILLING_ATTRIBUTE_DEFAULT_VAL").cast("int").isNull).as("DP12"),Seq("SUBSCRIBER_KEY"),"left").
    join(descuentosPuntuales.filter(col("ORDEN")===2 and !col("BILLING_ATTRIBUTE_DEFAULT_VAL").cast("int").isNull).as("DP2"),Seq("SUBSCRIBER_KEY"),"left").
    join(descuentosPuntuales.filter(col("ORDEN")===2 and col("BILLING_ATTRIBUTE_DEFAULT_VAL").cast("int").isNull).as("DP22"),Seq("SUBSCRIBER_KEY"),"left").
    join(descuentosPuntuales.filter(col("ORDEN")===3 and !col("BILLING_ATTRIBUTE_DEFAULT_VAL").cast("int").isNull).as("DP3"),Seq("SUBSCRIBER_KEY"),"left").
    join(descuentosPuntuales.filter(col("ORDEN")===3 and col("BILLING_ATTRIBUTE_DEFAULT_VAL").cast("int").isNull).as("DP32"),Seq("SUBSCRIBER_KEY"),"left").
    join(descuentosPuntuales.filter(col("ORDEN")===4 and !col("BILLING_ATTRIBUTE_DEFAULT_VAL").cast("int").isNull).as("DP4"),Seq("SUBSCRIBER_KEY"),"left").
    join(descuentosPuntuales.filter(col("ORDEN")===4 and col("BILLING_ATTRIBUTE_DEFAULT_VAL").cast("int").isNull).as("DP42"),Seq("SUBSCRIBER_KEY"),"left").
    join(descuentosPuntuales.filter(col("ORDEN")===5 and !col("BILLING_ATTRIBUTE_DEFAULT_VAL").cast("int").isNull).as("DP5"),Seq("SUBSCRIBER_KEY"),"left").
    join(descuentosPuntuales.filter(col("ORDEN")===5 and col("BILLING_ATTRIBUTE_DEFAULT_VAL").cast("int").isNull).as("DP52"),Seq("SUBSCRIBER_KEY"),"left").
    join(descuentosFinDeCiclo,Seq("PRODUCT_KEY"),"left").
    join(planesPpu,Seq("PRODUCT_KEY"),"left").
    join(parqueMovilKfactor,Seq("SUBSCRIBER_KEY"),"left").
    join(detalleFacturacion,Seq("SUBSCRIBER_KEY"),"left").
    select(col("RUT_ID").as("RUT"),
    col("CUSTOMER_LEGAL_NAME").as("RAZON_SOCIAL"),
      col("BILLING_CYCLE_KEY").as("CICLO"),
      col("SUBSCRIBER_KEY"),
      col("financial_account_key"),
      col("PRIMARY_RESOURCE_VALUE").as("MSISDN"),
      col("CUSTOMER_KEY"),
      date_format(col("ORIGINAL_ACTIVATION_DATE"),"yyyyMMdd").as("FECHA_ACTIVACION"),
      col("CUSTOMER_SUB_TYPE_DESC").as("SEGMENTO"),
      col("PRODUCT_KEY").as("CODIGO_PLAN"),
      col("PRODUCT_DESC_PLAN").as("PLAN"),
      col("ARPU"),
      col("CFD.BILLING_ATTRIBUTE_DEFAULT_VAL").as("CF_DATOS"),
      col("CFV.BILLING_ATTRIBUTE_DEFAULT_VAL").as("CF_VOZ"),
      col("DFD.BILLING_ATTRIBUTE_DEFAULT_VAL").as("DSC_CF_DATOS"),
      col("DFV.BILLING_ATTRIBUTE_DEFAULT_VAL").as("DSC_CF_VOZ"),
      (col("CFD.BILLING_ATTRIBUTE_DEFAULT_VAL")+col("CFV.BILLING_ATTRIBUTE_DEFAULT_VAL")-col("DFD.BILLING_ATTRIBUTE_DEFAULT_VAL")-col("DFV.BILLING_ATTRIBUTE_DEFAULT_VAL")).cast("int").as("TOTAL_CF"),
      col("DPDA.PARAMETER_VALUE").as("DSC_ADICIONAL_DATOS"),
      col("DPDT.PARAMETER_VALUE").as("TIPO_DSC_ADI_DATOS"),
      col("DPVA.PARAMETER_VALUE").as("DSC_ADICIONAL_VOZ"),
      col("DPVT.PARAMETER_VALUE").as("TIPO_DSC_ADI_VOZ"),
      col("DP1.BILLING_ATTRIBUTE_DEFAULT_VAL").as("DSC_PUNTUAL_1"),
      col("DP12.BILLING_ATTRIBUTE_DEFAULT_VAL").as("TIPO_DSC_PUNTUAL_1"),
      col("DP1.PROVISION_DATE").as("DSC_FECHA_INICIO_1"),
      col("DP1.FIN").as("DSC_FECHA_FIN_1"),
      col("DP2.BILLING_ATTRIBUTE_DEFAULT_VAL").as("DSC_PUNTUAL_2"),
      col("DP22.BILLING_ATTRIBUTE_DEFAULT_VAL").as("TIPO_DSC_PUNTUAL_2"),
      col("DP2.PROVISION_DATE").as("DSC_FECHA_INICIO_2"),
      col("DP2.FIN").as("DSC_FECHA_FIN_2"),
      col("DP3.BILLING_ATTRIBUTE_DEFAULT_VAL").as("DSC_PUNTUAL_3"),
      col("DP32.BILLING_ATTRIBUTE_DEFAULT_VAL").as("TIPO_DSC_PUNTUAL_3"),
      col("DP3.PROVISION_DATE").as("DSC_FECHA_INICIO_3"),
      col("DP3.FIN").as("DSC_FECHA_FIN_3"),
      col("DP4.BILLING_ATTRIBUTE_DEFAULT_VAL").as("DSC_PUNTUAL_4"),
      col("DP42.BILLING_ATTRIBUTE_DEFAULT_VAL").as("TIPO_DSC_PUNTUAL_4"),
      col("DP4.PROVISION_DATE").as("DSC_FECHA_INICIO_4"),
      col("DP4.FIN").as("DSC_FECHA_FIN_4"),
      col("DP5.BILLING_ATTRIBUTE_DEFAULT_VAL").as("DSC_PUNTUAL_5"),
      col("DP52.BILLING_ATTRIBUTE_DEFAULT_VAL").as("TIPO_DSC_PUNTUAL_5"),
      col("DP5.PROVISION_DATE").as("DSC_FECHA_INICIO_5"),
      col("DP5.FIN").as("DSC_FECHA_FIN_5"),
      col("PRODUCT_DESC"),
      col("descripcion"),
      col("PRODUCT")
    ).
    withColumn(      "ARPU_DATOS",
      when(
        coalesce(col("TIPO_DSC_ADI_DATOS"), lit("Fixed")) === "Fixed",
        col("CF_DATOS") - col("DSC_CF_DATOS") - coalesce(col("DSC_ADICIONAL_DATOS"), lit(0))).
        otherwise(
          when(
            coalesce(col("DSC_ADICIONAL_DATOS"), lit(0)) =!= 0,
            (col("CF_DATOS")- col("DSC_CF_DATOS")) * (lit(100) - coalesce(col("DSC_ADICIONAL_DATOS"), lit(0))) / lit(100) ).
            otherwise(col("CF_DATOS") - col("DSC_CF_DATOS"))
        ).cast("int")
    ).
    withColumn(
      "ARPU_VOZ",
      when(
        coalesce(col("TIPO_DSC_ADI_VOZ"), lit("Fixed")) === "Fixed",
        col("CF_VOZ") - col("DSC_CF_VOZ") - coalesce(col("DSC_ADICIONAL_VOZ"), lit(0))
      ).
        otherwise(
          when(
            coalesce(col("DSC_ADICIONAL_VOZ"), lit(0)) =!= 0,
            (col("CF_VOZ") - col("DSC_CF_VOZ")) * (lit(100) - coalesce(col("DSC_ADICIONAL_VOZ"), lit(0))) / lit(100) ).
            otherwise(col("CF_VOZ") - col("DSC_CF_VOZ"))
        ).cast("int")).
    withColumn(
    "TOTAL",
      ((col("ARPU_DATOS") + col("ARPU_VOZ")) *
        ((lit(100) - coalesce(col("DSC_PUNTUAL_1"), lit(0))) / lit(100)) *
        ((lit(100) - coalesce(col("DSC_PUNTUAL_2"), lit(0))) / lit(100)) *
        ((lit(100) - coalesce(col("DSC_PUNTUAL_3"), lit(0))) / lit(100)) *
        ((lit(100) - coalesce(col("DSC_PUNTUAL_4"), lit(0))) / lit(100)) *
        ((lit(100) - coalesce(col("DSC_PUNTUAL_5"), lit(0))) / lit(100))
      ).cast("int")
    ).
    withColumn(
    "TIPO_DSC",
    when(
      (!col("DSC_ADICIONAL_DATOS").isNull or col("DSC_ADICIONAL_VOZ").isNull) and !col("PRODUCT_DESC").isNull and !col("DSC_PUNTUAL_1").isNull,
      lit("MULTIPRODUCTO+PARAMETRIZABLE+FIN DE CICLO+PUNTUAL")
    ).
      when(
        (!col("DSC_ADICIONAL_DATOS").isNull or !col("DSC_ADICIONAL_VOZ").isNull) and !col("PRODUCT_DESC").isNull and col("DSC_PUNTUAL_1").isNull,
        lit("MULTIPRODUCTO+PARAMETRIZABLE+FIN DE CICLO")
      ).
      when(
        (!col("DSC_ADICIONAL_DATOS").isNull or !col("DSC_ADICIONAL_VOZ").isNull) and col("PRODUCT_DESC").isNull and !col("DSC_PUNTUAL_1").isNull,
        lit("MULTIPRODUCTO+PARAMETRIZABLE+PUNTUAL")
      ).
      when(
        (!col("DSC_ADICIONAL_DATOS").isNull or !col("DSC_ADICIONAL_VOZ").isNull) and col("PRODUCT_DESC").isNull and col("DSC_PUNTUAL_1").isNull,
        lit("MULTIPRODUCTO+PARAMETRIZABLE")
      ).
      when(
        col("DSC_ADICIONAL_DATOS").isNull and col("DSC_ADICIONAL_VOZ").isNull and !col("PRODUCT_DESC").isNull and !col("DSC_PUNTUAL_1").isNull,
        lit("MULTIPRODUCTO+FIN DE CICLO+PUNTUAL")
      ).
      when(
        col("DSC_ADICIONAL_DATOS").isNull and col("DSC_ADICIONAL_VOZ").isNull and !col("PRODUCT_DESC").isNull and col("DSC_PUNTUAL_1").isNull,
        lit("MULTIPRODUCTO+FIN DE CICLO")
      ).
      when(
        col("DSC_ADICIONAL_DATOS").isNull and col("DSC_ADICIONAL_VOZ").isNull and col("PRODUCT_DESC").isNull and !col("DSC_PUNTUAL_1").isNull,
        lit("MULTIPRODUCTO+PUNTUAL")
      ).
      otherwise(lit("MULTIPRODUCTO"))
  ).withColumn(
    "MARCA_USABLE",
    when(
      col("TIPO_DSC").like("%FIN%CICLO%")
        or col("TOTAL") < 0
        or (col("descripcion").isNull and col("TOTAL") === 0),
      lit("NO")
    ).otherwise(lit("SI"))
  ).
    withColumn("ARPU_T", when(col("TOTAL") < 0, 0).otherwise(col("TOTAL"))).
    withColumn(
      "TIPO_PRODUCTO",
        coalesce(col("PRODUCT"),
        when(
          upper(col("PRODUCT_DESC")).like("%GPS%")
            or upper(col("PRODUCT_DESC")).like("%M2M%"),
          "M2M"
        ).
          when(
            upper(col("PRODUCT_DESC")).like("%BAM%")
              or upper(col("PRODUCT_DESC")).like("%WIFI%MOVIL%"),
            "BAM"
          ).
          otherwise("VOZ")
        )

    ).dropDuplicates("SUBSCRIBER_KEY").drop("PRODUCT_DESC","PRODUCT","TOTAL","DESCRIPCION")

  planesEmpresas.
    withColumn("FECHA_PROC",current_date()).
    withColumn("PERIODO",date_format(current_date(),"yyyyMM")).
    repartition(1).
    write.mode("overwrite").
    format("orc").
    option("path", conformadoBasePath).
    saveAsTable(s"STG_DATALAKEB2B.$hiveTable")

  val file = fs.globStatus(new Path(s"$conformadoBasePath/part-*"))(0).getPath.getName

  fs.rename(new Path(s"$conformadoBasePath/$file"), new Path(s"$conformadoBasePath/$hiveTable.orc"))


  spark.close()

}
