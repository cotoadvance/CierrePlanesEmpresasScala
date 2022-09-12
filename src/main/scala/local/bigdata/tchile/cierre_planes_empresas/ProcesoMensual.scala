package local.bigdata.tchile.cierre_planes_empresas

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import java.time.ZoneId
import java.time.format.DateTimeFormatter

object ProcesoMensual {
  def main(args: Array[String]): Unit = {
    // ---------------------------------------------------------------
    // INICIO PARAMETRIZACION
    // ---------------------------------------------------------------

    // BASE

    val conf = new SparkConf().setMaster("yarn").setAppName("B2B").set("spark.driver.allowMultipleContexts", "true").set("spark.yarn.queue", "default").set("spark.sql.crossJoin.enabled", "true").set("spark.sql.debug.maxToStringFields", "1000").set("spark.sql.autoBroadcastJoinThreshold", "-1")
    val spark: SparkSession = SparkSession.builder().config(conf).config("spark.sql.warehouse.dir", "/usr/hdp/current/spark-client/conf/hive-site.xml").config("spark.submit.deployMode", "cluster").enableHiveSupport().getOrCreate()
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    import spark.implicits._

    // PARAMETROS

    val processPeriod = args(0) // "202207"
    val processYear = processPeriod.substring(0, 4)
    val processMonth = processPeriod.substring(4, 6)

    val format = new java.text.SimpleDateFormat("yyyy-MM-dd")

    val firstDayOfProcessPeriod = format.parse(s"$processYear-$processMonth-01".format(format))
    val firstDayOfProcessPeriodDate = java.time.LocalDate.from(firstDayOfProcessPeriod.toInstant.atZone(ZoneId.of("UTC")))
    val lastDayOfProcessPeriodDate = firstDayOfProcessPeriodDate.plusMonths(1).plusDays(-1)

    // PATHS

    val hiveTable = "fil_cierre_planes_empresa"
    val conformadoBasePath = s"/data/parque_asignado/parque/b2b/scel/planes_empresa/$hiveTable"

    val parqueMovilColivingPath = s"/data/producto_asignado/parque_movil/control_de_gestion/parque_movil_coliving/raw/year=$processYear/month=$processMonth/day=01"
    val productCatalogPath = "/modelos/ods/product_catalog/conformado"
    val productBillingParametersPath = "/modelos/ods/product_billing_parameters/conformado"
//    val assignedProductPath = s"/modelos/ods/assigned_product/conformado/year=$processYear/month=$processMonth"
//    val assignedProductParameterPath = s"/modelos/ods/assigned_product_parameter/conformado/year=$processYear/month=$processMonth"
    val assignedProductPath = "/modelos/ods/assigned_product/conformado"
    val assignedProductParameterPath = "/modelos/ods/assigned_product_parameter/conformado"
    val assignedBillingOfferPath = "/modelos/ods/assigned_billing_offer/conformado"
    val productPath = "/modelos/ods/product/conformado"
    val detalleFacturacionPath = "/modelos/finanzas/detalle_facturacion/conformado"
    val descuentosPlanesPath = "/data/parque_asignado/parque/b2b/scel/planes_empresa/par_planes_dsc_gpl_v2"
    val planesPpuPath = "/warehouse/staging/datalakeb2b/par_planes_ppu/par_planes_ppu.txt"

    // SCHEMAS

    val planesPpuSchema = StructType(Array(
      StructField("cod_plan", StringType, nullable = true),
      StructField("descripcion", StringType, nullable = true)
    ))

    // ---------------------------------------------------------------
    // FIN PARAMETRIZACION
    // ---------------------------------------------------------------

    // ---------------------------------------------------------------
    // INICIO PROCESO
    // ---------------------------------------------------------------

    // Parque Movil Coliving

    // empresa nueva = persona nueva?
    val parqueMovilColivingBase = spark.read.parquet(parqueMovilColivingPath).
      filter(
        trim($"customer_sub_type_desc").isin("Corporaciones", "Gran Empresa", "Mediana Empresa", "Pequeña Empresa", "Micro", "Empresa Nueva", "Empresa Relacionada")
        and trim($"payment_category_desc").isin("Postpaid Payment", "Hybrid Payment")
        and $"valid_ind" === 1
        and $"sistema_origen" === "AMD"
      ).
      withColumn("PRODUCT_KEY", trim($"PRODUCT_KEY")).
      withColumn("SUBSCRIBER_KEY", trim($"SUBSCRIBER_KEY")).
      withColumn("ORIGINAL_ACTIVATION_DATE", date_format($"GAIN_DATE_ACCESS_MNGCTR", "yyyyMMdd")).
      select(
        "CLOSE_DATE",
        "RUT_ID",
        "CUSTOMER_NAME",
        "BILLING_CYCLE_KEY",
        "SUBSCRIBER_KEY",
        "FINANCIAL_ACCOUNT_KEY",
        "PRIMARY_RESOURCE_VALUE",
        "CUSTOMER_KEY",
        "CUSTOMER_SUB_TYPE_DESC",
        "PRODUCT_KEY",
        "PRODUCT_DESC",
        "PRODUCT",
        "ORIGINAL_ACTIVATION_DATE"
      )

    val productCatalog = spark.read.parquet(productCatalogPath).
      withColumn("PRODUCT_VALID_FROM_DATE", date_format($"PRODUCT_VALID_FROM_DATE", "yyyyMMdd")).
      select(
        "PRODUCT_KEY",
        "PRODUCT_VALID_FROM_DATE"
      )

    val parqueMovilColiving = parqueMovilColivingBase.
      join(productCatalog, Seq("PRODUCT_KEY"), "left")

    // Cargos Fijos

    val productBillingParameters = spark.read.parquet(productBillingParametersPath).
      withColumn("BILLING_OFFER_EXPIRATION_DATE", coalesce($"BILLING_OFFER_EXPIRATION_DATE", lit(java.time.LocalDate.now.toString))).
      withColumn("PRODUCT_KEY", trim($"PRODUCT_KEY")).
      select(
        "ITEM_NAME",
        "PRODUCT_KEY",
        "BILLING_ATTRIBUTE_NAME",
        "BILLING_ATTRIBUTE_DEFAULT_VAL",
        "BILLING_OFFER_EFFECTIVE_DATE",
        "BILLING_OFFER_EXPIRATION_DATE",
        "PACKAGE_EXPIRATION_DATE",
        "DML_IND"
      )

    val cargosFijos = parqueMovilColivingBase.
      join(
        productBillingParameters,
        parqueMovilColivingBase("PRODUCT_KEY") === productBillingParameters("PRODUCT_KEY")
        and $"BILLING_ATTRIBUTE_NAME".isin("Rate", "Discount value")
        and $"billing_offer_effective_date" <= lastDayOfProcessPeriodDate.toString
        and $"billing_offer_expiration_date" > lastDayOfProcessPeriodDate.toString
        ,
        "left"
      ).
      filter($"dml_ind" =!= "DELETE").
      withColumn("BILLING_ATTRIBUTE_NAME", coalesce($"BILLING_ATTRIBUTE_NAME", lit("Rate"))).
      withColumn("BILLING_ATTRIBUTE_DEFAULT_VAL", coalesce($"BILLING_ATTRIBUTE_DEFAULT_VAL", lit(0))).
      select(
        $"SUBSCRIBER_KEY",
        parqueMovilColivingBase("PRODUCT_KEY"),
        $"ITEM_NAME",
        $"BILLING_ATTRIBUTE_NAME",
        $"BILLING_ATTRIBUTE_DEFAULT_VAL"
      )

    // Descuentos Parametrizables

    val assignedProduct = spark.read.parquet(assignedProductPath).
      withColumn("SUBSCRIBER_KEY", trim($"SUBSCRIBER_KEY")).
      withColumn("ASSIGNED_PRODUCT_KEY", trim($"ASSIGNED_PRODUCT_KEY")).
      withColumn("PRODUCT_KEY", trim($"PRODUCT_KEY")).
      withColumn("CUSTOMER_KEY", trim($"CUSTOMER_KEY")).
      withColumn("ASSIGNED_PRODUCT_ID", trim($"ASSIGNED_PRODUCT_ID")).
      withColumn("ASSIGNED_PRODUCT_STATUS_KEY", trim($"ASSIGNED_PRODUCT_STATUS_KEY")).
      withColumn("ASSIGNED_PRODUCT_STATE_KEY", trim($"ASSIGNED_PRODUCT_STATE_KEY")).
      select(
        "SUBSCRIBER_KEY",
        "ASSIGNED_PRODUCT_KEY",
        "ASSIGNED_PRODUCT_ID",
        "PRODUCT_KEY",
        "CUSTOMER_KEY",
        "ASSIGNED_PRODUCT_STATUS_KEY",
        "ASSIGNED_PRODUCT_STATE_KEY",
        "start_date",
        "end_date",
        "dml_ind"
      )

    val assignedProductParameter = spark.read.parquet(assignedProductParameterPath).
      withColumn("ASSIGNED_PRODUCT_KEY", trim($"ASSIGNED_PRODUCT_KEY")).
      withColumn("PRODUCT_PARAMETER_KEY", trim($"PRODUCT_PARAMETER_KEY")).
      select(
        "ASSIGNED_PRODUCT_KEY",
        "PRODUCT_PARAMETER_KEY",
        "ASSIGNED_PRODUCT_PARAMETER_ID",
        "PARAMETER_VALUE",
        "dml_ind"
      )

    val descuentosParametrizables = assignedProduct.
      join(
        spark.read.parquet(parqueMovilColivingPath).as("parqueMovilColiving"),
        trim($"parqueMovilColiving.SUBSCRIBER_KEY") === assignedProduct("SUBSCRIBER_KEY")
          and trim($"customer_sub_type_desc").isin("Corporaciones", "Gran Empresa", "Mediana Empresa", "Pequeña Empresa", "Micro", "Empresa Nueva", "Empresa Relacionada")
          and trim($"payment_category_desc").isin("Postpaid Payment", "Hybrid Payment")
          and $"valid_ind" === 1
          and $"sistema_origen" === "AMD"
      ).
      join(
        assignedProductParameter,
        assignedProduct("ASSIGNED_PRODUCT_KEY") === assignedProductParameter("ASSIGNED_PRODUCT_KEY")
        and assignedProductParameter("PRODUCT_PARAMETER_KEY").isin(26754, 27217, 27180, 27183, 27178)
        and assignedProductParameter("dml_ind") =!= "DELETE",
        "left"
      ).
      filter(
        assignedProduct("PRODUCT_KEY") === "2448119"
        and $"ASSIGNED_PRODUCT_STATUS_KEY".isin(1351, 1359)
        and $"ASSIGNED_PRODUCT_STATE_KEY" === 1331
        and $"start_date" <= lastDayOfProcessPeriodDate.toString
        and $"end_date" > lastDayOfProcessPeriodDate.toString
        and assignedProduct("dml_ind") =!= "DELETE"
      ).
      select(
        trim($"parqueMovilColiving.SUBSCRIBER_KEY").as("SUBSCRIBER_KEY"),
        assignedProduct("PRODUCT_KEY"),
        assignedProduct("ASSIGNED_PRODUCT_KEY"),
        $"ASSIGNED_PRODUCT_PARAMETER_ID",
        $"PARAMETER_VALUE"
      )

    // Descuentos Puntuales

    val assignedBillingOffer = spark.read.parquet(assignedBillingOfferPath).
      withColumn("SUBSCRIBER_KEY", trim($"SUBSCRIBER_KEY")).
      withColumn("PRODUCT_KEY", trim($"PRODUCT_KEY")).
      withColumn("ASSIGNED_PRODUCT_ID", trim($"ASSIGNED_PRODUCT_ID")).
      select(
        "ASSIGNED_PRODUCT_ID",
        "SUBSCRIBER_KEY",
        "PRODUCT_KEY",
        "ASGN_BLNG_OFR_STATE_KEY",
        "ASGN_BLNG_OFR_STATUS_KEY",
        "PARENT_PRODUCT_KEY",
        "PROVISION_DATE",
        "start_date",
        "end_date"
      )

    val product = spark.read.parquet(productPath).
      withColumn("PRODUCT_KEY", trim($"PRODUCT_KEY")).
      select(
        "PRODUCT_KEY",
        "PRODUCT_DESC",
        "PRICE_PLAN_PROMOTION_INTERVAL",
        "PRICE_PLAN_PROMOTION_DURATION"
      )

    val descuentosPuntuales = assignedProduct.
      join(spark.read.parquet(parqueMovilColivingPath).as("parqueMovilColiving"),
        trim($"parqueMovilColiving.SUBSCRIBER_KEY") === assignedProduct("SUBSCRIBER_KEY")
          and trim($"customer_sub_type_desc").isin("Corporaciones", "Gran Empresa", "Mediana Empresa", "Pequeña Empresa", "Micro", "Empresa Nueva", "Empresa Relacionada")
          and trim($"payment_category_desc").isin("Postpaid Payment", "Hybrid Payment")
          and $"valid_ind" === 1
          and $"sistema_origen" === "AMD"
      ).
      join(
        assignedBillingOffer,
        assignedProduct("ASSIGNED_PRODUCT_ID") === assignedBillingOffer("ASSIGNED_PRODUCT_ID")
          and assignedProduct("SUBSCRIBER_KEY") === assignedBillingOffer("SUBSCRIBER_KEY")
          and $"ASGN_BLNG_OFR_STATE_KEY" === 1331
          and $"ASGN_BLNG_OFR_STATUS_KEY".isin(1351, 1359)
          and $"PARENT_PRODUCT_KEY".isin("2713", "3133", "18865")
          and assignedBillingOffer("start_date") <= lastDayOfProcessPeriodDate.toString
          and assignedBillingOffer("end_date") > lastDayOfProcessPeriodDate.toString,
        "left"
      ).
      join(product, product("PRODUCT_KEY") === assignedBillingOffer("PRODUCT_KEY"), "left").
      join(
        spark.read.parquet(productBillingParametersPath).as("productBillingParameters"),
        assignedBillingOffer("PRODUCT_KEY") === trim($"productBillingParameters.PRODUCT_KEY")
          and $"productBillingParameters.BILLING_OFFER_EXPIRATION_DATE".isNull
          and $"productBillingParameters.PACKAGE_EXPIRATION_DATE".isNull
          and $"productBillingParameters.BILLING_ATTRIBUTE_NAME".isin("Discount value", "Discount type")
          and $"productBillingParameters.DML_IND" =!= "DELETE",
        "left"
      ).
      filter(
        assignedProduct("PRODUCT_KEY").isin("2713", "3133", "18865")
        and assignedProduct("ASSIGNED_PRODUCT_STATUS_KEY").isin(1351, 1359)
        and assignedProduct("ASSIGNED_PRODUCT_STATE_KEY") === 1331
        and assignedProduct("start_date") <= lastDayOfProcessPeriodDate.toString
        and assignedProduct("end_date") > lastDayOfProcessPeriodDate.toString
        and !assignedProduct("CUSTOMER_KEY").isNull
        and !assignedBillingOffer("PRODUCT_KEY").isNull
      ).
      withColumn(
        "FIN",
        when($"PRICE_PLAN_PROMOTION_INTERVAL" === "M", expr("add_months(PROVISION_DATE, PRICE_PLAN_PROMOTION_DURATION)")).
        when($"PRICE_PLAN_PROMOTION_INTERVAL" === "D", expr("date_add(PROVISION_DATE, PRICE_PLAN_PROMOTION_DURATION)")).
        otherwise(lit(null))
      ).
      select(
        assignedProduct("SUBSCRIBER_KEY"),
        assignedBillingOffer("PRODUCT_KEY"),
        product("PRODUCT_DESC"),
        $"productBillingParameters.BILLING_ATTRIBUTE_DEFAULT_VAL",
        assignedBillingOffer("PROVISION_DATE").as("INICIO"),
        $"FIN"
      ).distinct()

    // Cargos Facturados

    val detalleFacturacion = spark.read.parquet(detalleFacturacionPath).
      withColumn("SUBSCRIBER_KEY", trim($"SUBSCRIBER_KEY")).
      filter(
        $"bill_date" between(firstDayOfProcessPeriodDate.toString, lastDayOfProcessPeriodDate.toString)
        and $"customer_sub_type_desc".isin("Corporaciones", "Gran Empresa", "Mediana", "Pequeña", "Micro", "Empresa Nueva", "Empresa Relacionada")
        and $"payment_category_desc".isin("Postpaid Payment", "Hybrid Payment")
        and $"charge_code_id".isin("RM_VOZ_PL_POS", "RM_DAT_PL_POS", "RM_VOZ_PL_HIB", "RM_DAT_PL_HIB", "RM_M2M_PL1", "RM_BAM_PL_POS", "RM_GPS_PL1", "OM_EQU_SUBS")
      ).
      groupBy("SUBSCRIBER_KEY").
      agg(sum("charge_amount").as("arpu"))

    val cargosFacturados = spark.read.parquet(parqueMovilColivingPath).as("parqueMovilColiving").
      join(detalleFacturacion, trim($"parqueMovilColiving.SUBSCRIBER_KEY") === detalleFacturacion("SUBSCRIBER_KEY"), "left").
      filter(
        trim($"customer_sub_type_desc").isin("Corporaciones", "Gran Empresa", "Mediana Empresa", "Pequeña Empresa", "Micro", "Empresa Nueva", "Empresa Relacionada")
          and trim($"payment_category_desc").isin("Postpaid Payment", "Hybrid Payment")
          and $"valid_ind" === 1
          and $"sistema_origen" === "AMD"
      ).
      select(
        $"parqueMovilColiving.CLOSE_DATE",
        trim($"parqueMovilColiving.SUBSCRIBER_KEY").as("SUBSCRIBER_KEY"),
        coalesce(detalleFacturacion("arpu"), lit(0)).as("ARPU")
      )

    // Descuentos Puntuales STG

    val descuentosPuntualesStg = descuentosPuntuales.
      filter(
        $"PRODUCT_DESC".like("%\\%%")
        and !lower($"PRODUCT_DESC").like("%sms%")
        and !lower($"PRODUCT_DESC").like("%bolsa%dato%")
        and lower($"BILLING_ATTRIBUTE_DEFAULT_VAL") =!= "percent"
      ).
      withColumn("POSICION", row_number().over(Window.partitionBy("SUBSCRIBER_KEY").orderBy("PRODUCT_KEY"))).
      withColumn("CHARGE_VAL", when($"BILLING_ATTRIBUTE_DEFAULT_VAL" === "0", null).otherwise($"BILLING_ATTRIBUTE_DEFAULT_VAL".cast("int"))).
      withColumn("tipo_dsc_punt", lit("Percent")).
      select(
        "SUBSCRIBER_KEY",
        "PRODUCT_KEY",
        "PRODUCT_DESC",
        "CHARGE_VAL",
        "tipo_dsc_punt",
        "INICIO",
        "FIN",
        "POSICION"
      ).distinct()

    // Cierre Planes Empresas

    val cargosFijosStg = cargosFijos.
      withColumn("CHARGE_VAL", $"BILLING_ATTRIBUTE_DEFAULT_VAL".cast("int")).
      withColumn("ITEM_NAME", lower($"ITEM_NAME")).
      select("SUBSCRIBER_KEY", "CHARGE_VAL", "ITEM_NAME")

    val descuentosParametrizablesStg = descuentosParametrizables.
      withColumn("PARAMETER_VALUE", $"PARAMETER_VALUE".cast("int")).
      withColumn("ASSIGNED_PRODUCT_PARAMETER_ID", lower($"ASSIGNED_PRODUCT_PARAMETER_ID")).
      select("SUBSCRIBER_KEY", "PARAMETER_VALUE", "ASSIGNED_PRODUCT_KEY", "ASSIGNED_PRODUCT_PARAMETER_ID")

    val descuentosPlanes = spark.read.parquet(descuentosPlanesPath)

    val planesPpu = spark.read.
      schema(planesPpuSchema).
      options(Map("delimiter"->",", "header"->"true")).
      csv(planesPpuPath)

    val planesEmpresas = parqueMovilColiving.
      join(cargosFijosStg.filter($"ITEM_NAME".like("%cargo%datos%")).as("cargosFijosCargoDatos"), parqueMovilColiving("SUBSCRIBER_KEY") === $"cargosFijosCargoDatos.SUBSCRIBER_KEY", "left").
      join(cargosFijosStg.filter($"ITEM_NAME".like("%cargo%voz%")).as("cargosFijosCargoVoz"), parqueMovilColiving("SUBSCRIBER_KEY") === $"cargosFijosCargoVoz.SUBSCRIBER_KEY", "left").
      join(cargosFijosStg.filter($"ITEM_NAME".like("%desc%datos%")).as("cargosFijosDescDatos"), parqueMovilColiving("SUBSCRIBER_KEY") === $"cargosFijosDescDatos.SUBSCRIBER_KEY", "left").
      join(cargosFijosStg.filter($"ITEM_NAME".like("%desc%voz%")).as("cargosFijosDescVoz"), parqueMovilColiving("SUBSCRIBER_KEY") === $"cargosFijosDescVoz.SUBSCRIBER_KEY", "left").
      join(
        descuentosParametrizablesStg.filter($"ASSIGNED_PRODUCT_PARAMETER_ID".like("%amountdata%") and $"PARAMETER_VALUE" =!= 0).as("descuentosParametrizablesAmountdata"),
        parqueMovilColiving("SUBSCRIBER_KEY") === $"descuentosParametrizablesAmountdata.SUBSCRIBER_KEY",
        "left"
      ).
      join(
        descuentosParametrizablesStg.filter($"ASSIGNED_PRODUCT_PARAMETER_ID".like("%typedata%")).as("descuentosParametrizablesTypedata"),
        parqueMovilColiving("SUBSCRIBER_KEY") === $"descuentosParametrizablesTypedata.SUBSCRIBER_KEY"
          and $"descuentosParametrizablesAmountdata.ASSIGNED_PRODUCT_KEY" === $"descuentosParametrizablesTypedata.ASSIGNED_PRODUCT_KEY",
        "left"
      ).
      join(
        descuentosParametrizablesStg.filter($"ASSIGNED_PRODUCT_PARAMETER_ID".like("%amountvoice%") and $"PARAMETER_VALUE" =!= "0").as("descuentosParametrizablesAmountvoice"),
        parqueMovilColiving("SUBSCRIBER_KEY") === $"descuentosParametrizablesAmountvoice.SUBSCRIBER_KEY",
        "left"
      ).
      join(
        descuentosParametrizablesStg.filter($"ASSIGNED_PRODUCT_PARAMETER_ID".like("%typevoice%")).as("descuentosParametrizablesTypevoice"),
        parqueMovilColiving("SUBSCRIBER_KEY") === $"descuentosParametrizablesTypevoice.SUBSCRIBER_KEY"
          and $"descuentosParametrizablesAmountvoice.ASSIGNED_PRODUCT_KEY" === $"descuentosParametrizablesTypevoice.ASSIGNED_PRODUCT_KEY",
        "left"
      ).
      join(descuentosPuntualesStg.filter($"POSICION" === 1).as("descuentosPuntuales1"), parqueMovilColiving("SUBSCRIBER_KEY") === $"descuentosPuntuales1.SUBSCRIBER_KEY", "left").
      join(descuentosPuntualesStg.filter($"POSICION" === 2).as("descuentosPuntuales2"), parqueMovilColiving("SUBSCRIBER_KEY") === $"descuentosPuntuales2.SUBSCRIBER_KEY", "left").
      join(descuentosPuntualesStg.filter($"POSICION" === 3).as("descuentosPuntuales3"), parqueMovilColiving("SUBSCRIBER_KEY") === $"descuentosPuntuales3.SUBSCRIBER_KEY", "left").
      join(descuentosPuntualesStg.filter($"POSICION" === 4).as("descuentosPuntuales4"), parqueMovilColiving("SUBSCRIBER_KEY") === $"descuentosPuntuales4.SUBSCRIBER_KEY", "left").
      join(descuentosPuntualesStg.filter($"POSICION" === 5).as("descuentosPuntuales5"), parqueMovilColiving("SUBSCRIBER_KEY") === $"descuentosPuntuales5.SUBSCRIBER_KEY", "left").
      join(descuentosPlanes, parqueMovilColiving("PRODUCT_KEY") === descuentosPlanes("PRODUCT_KEY"), "left").
      join(cargosFacturados, parqueMovilColiving("SUBSCRIBER_KEY") === cargosFacturados("SUBSCRIBER_KEY"), "left").
      withColumn("RAZON_SOCIAL", regexp_replace($"CUSTOMER_NAME", "|", "")).
      withColumn("PLAN", regexp_replace(parqueMovilColiving("PRODUCT_DESC"), "|", "")).
      withColumn("ARPU", coalesce(cargosFacturados("ARPU"), lit(0))).
      withColumn("CARGO_FIJO_DATOS", coalesce($"cargosFijosCargoDatos.CHARGE_VAL", lit(0))).
      withColumn("CARGO_FIJO_VOZ", coalesce($"cargosFijosCargoVoz.CHARGE_VAL", lit(0))).
      withColumn("DESC_CARGO_FIJO_DATOS", coalesce($"cargosFijosDescDatos.CHARGE_VAL", lit(0))).
      withColumn("DESC_CARGO_FIJO_VOZ", coalesce($"cargosFijosDescVoz.CHARGE_VAL", lit(0))).
      withColumn("TOTAL_CF", $"CARGO_FIJO_DATOS" + $"CARGO_FIJO_VOZ" - $"DESC_CARGO_FIJO_DATOS" - $"DESC_CARGO_FIJO_VOZ").
      withColumn("DSC_ADICIONAL_DATOS", $"descuentosParametrizablesAmountdata.PARAMETER_VALUE").
      withColumn("TIPO_DSC_ADI_DATOS", $"descuentosParametrizablesTypedata.PARAMETER_VALUE").
      withColumn("DSC_ADICIONAL_VOZ", $"descuentosParametrizablesAmountvoice.PARAMETER_VALUE").
      withColumn("TIPO_DSC_ADI_VOZ", $"descuentosParametrizablesTypevoice.PARAMETER_VALUE").
      withColumn(
        "ARPU_DATOS",
        when(
          coalesce($"TIPO_DSC_ADI_DATOS", lit("Fixed")) === "Fixed",
          $"CARGO_FIJO_DATOS" - $"DESC_CARGO_FIJO_DATOS" - coalesce($"DSC_ADICIONAL_DATOS", lit(0))
        ).
        otherwise(
          when(
            coalesce($"DSC_ADICIONAL_DATOS", lit(0)) =!= 0,
            ($"CARGO_FIJO_DATOS" - $"DESC_CARGO_FIJO_DATOS") * (lit(100) - coalesce($"DSC_ADICIONAL_DATOS", lit(0))) / lit(100) ).
            otherwise($"CARGO_FIJO_DATOS" - $"DESC_CARGO_FIJO_DATOS")
        )
      ).
      withColumn(
        "ARPU_VOZ",
        when(
          coalesce($"TIPO_DSC_ADI_VOZ", lit("Fixed")) === "Fixed",
          $"CARGO_FIJO_VOZ" - $"DESC_CARGO_FIJO_VOZ" - coalesce($"DSC_ADICIONAL_VOZ", lit(0))
        ).
        otherwise(
          when(
            coalesce($"DSC_ADICIONAL_VOZ", lit(0)) =!= 0,
            ($"DSC_ADICIONAL_VOZ" - $"CARGO_FIJO_VOZ") * (lit(100) - coalesce($"DSC_ADICIONAL_VOZ", lit(0))) / lit(100) ).
            otherwise($"CARGO_FIJO_VOZ" - $"DESC_CARGO_FIJO_VOZ")
          )
      ).
      withColumn("DSC_PUNTUAL_1", $"descuentosPuntuales1.CHARGE_VAL").
      withColumn("TIPO_DSC_PUNTUAL_1", when(!$"DSC_PUNTUAL_1".isNull, $"descuentosPuntuales1.tipo_dsc_punt").otherwise(lit(null))).
      withColumn("DSC_PUNTUAL_2", $"descuentosPuntuales2.CHARGE_VAL").
      withColumn("TIPO_DSC_PUNTUAL_2", when(!$"DSC_PUNTUAL_2".isNull, $"descuentosPuntuales2.tipo_dsc_punt").otherwise(lit(null))).
      withColumn("DSC_PUNTUAL_3", $"descuentosPuntuales3.CHARGE_VAL").
      withColumn("TIPO_DSC_PUNTUAL_3", when(!$"DSC_PUNTUAL_3".isNull, $"descuentosPuntuales3.tipo_dsc_punt").otherwise(lit(null))).
      withColumn("DSC_PUNTUAL_4", $"descuentosPuntuales4.CHARGE_VAL").
      withColumn("TIPO_DSC_PUNTUAL_4", when(!$"DSC_PUNTUAL_4".isNull, $"descuentosPuntuales4.tipo_dsc_punt").otherwise(lit(null))).
      withColumn("DSC_PUNTUAL_5", $"descuentosPuntuales5.CHARGE_VAL").
      withColumn("TIPO_DSC_PUNTUAL_5", when(!$"DSC_PUNTUAL_5".isNull, $"descuentosPuntuales5.tipo_dsc_punt").otherwise(lit(null))).
      withColumn(
        "TOTAL",
        (
          ($"ARPU_DATOS" + $"ARPU_VOZ") *
          ((lit(100) - coalesce($"DSC_PUNTUAL_1", lit(0))) / lit(100)) *
          ((lit(100) - coalesce($"DSC_PUNTUAL_2", lit(0))) / lit(100)) *
          ((lit(100) - coalesce($"DSC_PUNTUAL_3", lit(0))) / lit(100)) *
          ((lit(100) - coalesce($"DSC_PUNTUAL_4", lit(0))) / lit(100)) *
          ((lit(100) - coalesce($"DSC_PUNTUAL_5", lit(0))) / lit(100))
        ).cast("int")
      ).
      withColumn("FECHA_PROC", date_format(current_timestamp(), "yyyyMMdd HH:mm:ss")).
      withColumn("PERIODO", lit(processPeriod)).
      withColumn(
        "TIPO_DSC",
        when(
          (!$"DSC_ADICIONAL_DATOS".isNull or !$"DSC_ADICIONAL_VOZ".isNull) and !descuentosPlanes("PRODUCT_DESC").isNull and !$"DSC_PUNTUAL_1".isNull,
          lit("MULTIPRODUCTO+PARAMETRIZABLE+FIN DE CICLO+PUNTUAL")
        ).
        when(
          (!$"DSC_ADICIONAL_DATOS".isNull or !$"DSC_ADICIONAL_VOZ".isNull) and !descuentosPlanes("PRODUCT_DESC").isNull and $"DSC_PUNTUAL_1".isNull,
          lit("MULTIPRODUCTO+PARAMETRIZABLE+FIN DE CICLO")
        ).
        when(
          (!$"DSC_ADICIONAL_DATOS".isNull or !$"DSC_ADICIONAL_VOZ".isNull) and descuentosPlanes("PRODUCT_DESC").isNull and !$"DSC_PUNTUAL_1".isNull,
          lit("MULTIPRODUCTO+PARAMETRIZABLE+PUNTUAL")
        ).
        when(
          (!$"DSC_ADICIONAL_DATOS".isNull or !$"DSC_ADICIONAL_VOZ".isNull) and descuentosPlanes("PRODUCT_DESC").isNull and $"DSC_PUNTUAL_1".isNull,
          lit("MULTIPRODUCTO+PARAMETRIZABLE")
        ).
        when(
          $"DSC_ADICIONAL_DATOS".isNull and $"DSC_ADICIONAL_VOZ".isNull and !descuentosPlanes("PRODUCT_DESC").isNull and !$"DSC_PUNTUAL_1".isNull,
          lit("MULTIPRODUCTO+FIN DE CICLO+PUNTUAL")
        ).
        when(
          $"DSC_ADICIONAL_DATOS".isNull and $"DSC_ADICIONAL_VOZ".isNull and !descuentosPlanes("PRODUCT_DESC").isNull and $"DSC_PUNTUAL_1".isNull,
          lit("MULTIPRODUCTO+FIN DE CICLO")
        ).
        when(
          $"DSC_ADICIONAL_DATOS".isNull and $"DSC_ADICIONAL_VOZ".isNull and descuentosPlanes("PRODUCT_DESC").isNull and !$"DSC_PUNTUAL_1".isNull,
          lit("MULTIPRODUCTO+PUNTUAL")
        ).
        otherwise(lit("MULTIPRODUCTO"))
      ).
      select(
        parqueMovilColiving("RUT_ID").as("RUT"),
        $"RAZON_SOCIAL",
        parqueMovilColiving("BILLING_CYCLE_KEY").as("CICLO"),
        parqueMovilColiving("SUBSCRIBER_KEY"),
        parqueMovilColiving("FINANCIAL_ACCOUNT_KEY"),
        parqueMovilColiving("PRIMARY_RESOURCE_VALUE").as("MSISDN"),
        parqueMovilColiving("CUSTOMER_KEY"),
        parqueMovilColiving("CUSTOMER_SUB_TYPE_DESC").as("SEGMENTO"),
        parqueMovilColiving("PRODUCT_KEY").as("CODIGO_PLAN"),
        $"PLAN",
        $"ARPU",
        $"CARGO_FIJO_DATOS".as("CF_DATOS"),
        $"CARGO_FIJO_VOZ".as("CF_VOZ"),
        $"DESC_CARGO_FIJO_DATOS".as("DSC_CF_DATOS"),
        $"DESC_CARGO_FIJO_VOZ".as("DSC_CF_VOZ"),
        $"TOTAL_CF",
        $"DSC_ADICIONAL_DATOS",
        $"TIPO_DSC_ADI_DATOS",
        $"DSC_ADICIONAL_VOZ",
        $"TIPO_DSC_ADI_VOZ",
        $"ARPU_DATOS",
        $"ARPU_VOZ",
        $"DSC_PUNTUAL_1",
        $"TIPO_DSC_PUNTUAL_1",
        $"descuentosPuntuales1.INICIO".as("DSC_FECHA_INICIO_1"),
        $"descuentosPuntuales1.FIN".as("DSC_FECHA_FIN_1"),
        $"DSC_PUNTUAL_2",
        $"TIPO_DSC_PUNTUAL_2",
        $"descuentosPuntuales2.INICIO".as("DSC_FECHA_INICIO_2"),
        $"descuentosPuntuales2.FIN".as("DSC_FECHA_FIN_2"),
        $"DSC_PUNTUAL_3",
        $"TIPO_DSC_PUNTUAL_3",
        $"descuentosPuntuales3.INICIO".as("DSC_FECHA_INICIO_3"),
        $"descuentosPuntuales3.FIN".as("DSC_FECHA_FIN_3"),
        $"DSC_PUNTUAL_4",
        $"TIPO_DSC_PUNTUAL_4",
        $"descuentosPuntuales4.INICIO".as("DSC_FECHA_INICIO_4"),
        $"descuentosPuntuales4.FIN".as("DSC_FECHA_FIN_4"),
        $"DSC_PUNTUAL_5",
        $"TIPO_DSC_PUNTUAL_5",
        $"descuentosPuntuales5.INICIO".as("DSC_FECHA_INICIO_5"),
        $"descuentosPuntuales5.FIN".as("DSC_FECHA_FIN_5"),
        $"TOTAL",
        $"FECHA_PROC",
        $"PERIODO",
        parqueMovilColiving("PRODUCT").as("TIPO_PRODUCTO"),
        $"TIPO_DSC",
        parqueMovilColiving("ORIGINAL_ACTIVATION_DATE").as("FECHA_ACTIVACION"),
        parqueMovilColiving("PRODUCT_VALID_FROM_DATE").as("FECHA_CREA_PLAN")
      ).
      distinct().
      join(planesPpu, $"CODIGO_PLAN" === planesPpu("cod_plan"), "left").
      withColumn(
        "MARCA_USABLE",
        when(
          $"TIPO_DSC".like("%FIN%CICLO%")
          or $"TOTAL" < 0
          or ($"TOTAL" > 30000 and date_format($"FECHA_CREA_PLAN", "yyyy") >= 2020)
          or (planesPpu("cod_plan").isNull and $"TOTAL" === 0),
          lit("NO")
        ).otherwise(lit("SI"))
      ).
      withColumn("ARPU_T", when($"TOTAL" < 0, 0).otherwise($"TOTAL")).
      withColumn(
        "ORDEN",
        row_number().over(
          Window.partitionBy("SUBSCRIBER_KEY").
            orderBy(
              desc("CF_DATOS"),
              desc("CF_VOZ"),
              desc("DSC_CF_DATOS"),
              desc("DSC_CF_VOZ"),
              desc("DSC_ADICIONAL_DATOS"),
              desc("TIPO_DSC_ADI_DATOS"),
              desc("DSC_ADICIONAL_VOZ"),
              desc("TIPO_DSC_ADI_VOZ"),
              desc("DSC_PUNTUAL_1"),
              desc("DSC_PUNTUAL_2"),
              desc("DSC_PUNTUAL_3"),
              desc("DSC_PUNTUAL_4"),
              desc("DSC_PUNTUAL_5")
            )
        )
      ).
      filter($"ORDEN" === 1).
      select(
        "RUT",
        "RAZON_SOCIAL",
        "CICLO",
        "SUBSCRIBER_KEY",
        "FINANCIAL_ACCOUNT_KEY",
        "MSISDN",
        "CUSTOMER_KEY",
        "FECHA_ACTIVACION",
        "SEGMENTO",
        "CODIGO_PLAN",
        "PLAN",
        "ARPU",
        "CF_DATOS",
        "CF_VOZ",
        "DSC_CF_DATOS",
        "DSC_CF_VOZ",
        "TOTAL_CF",
        "DSC_ADICIONAL_DATOS",
        "TIPO_DSC_ADI_DATOS",
        "DSC_ADICIONAL_VOZ",
        "TIPO_DSC_ADI_VOZ",
        "ARPU_DATOS",
        "ARPU_VOZ",
        "DSC_PUNTUAL_1",
        "TIPO_DSC_PUNTUAL_1",
        "DSC_FECHA_INICIO_1",
        "DSC_FECHA_FIN_1",
        "DSC_PUNTUAL_2",
        "TIPO_DSC_PUNTUAL_2",
        "DSC_FECHA_INICIO_2",
        "DSC_FECHA_FIN_2",
        "DSC_PUNTUAL_3",
        "TIPO_DSC_PUNTUAL_3",
        "DSC_FECHA_INICIO_3",
        "DSC_FECHA_FIN_3",
        "DSC_PUNTUAL_4",
        "TIPO_DSC_PUNTUAL_4",
        "DSC_FECHA_INICIO_4",
        "DSC_FECHA_FIN_4",
        "DSC_PUNTUAL_5",
        "TIPO_DSC_PUNTUAL_5",
        "DSC_FECHA_INICIO_5",
        "DSC_FECHA_FIN_5",
        "ARPU_T",
        "FECHA_PROC",
        "PERIODO",
        "TIPO_PRODUCTO",
        "TIPO_DSC",
        "MARCA_USABLE"
      )

    // ---------------------------------------------------------------
    // FIN PROCESO
    // ---------------------------------------------------------------

    // ---------------------------------------------------------------
    // INICIO ESCRITURA HDFS
    // ---------------------------------------------------------------

    //planesEmpresas.repartition(1).write.mode("overwrite").orc(conformadoBasePath)

    planesEmpresas.
      repartition(1).
      write.mode("overwrite").
      format("orc").
      option("path", conformadoBasePath).
      saveAsTable(s"STG_DATALAKEB2B.$hiveTable")

    val file = fs.globStatus(new Path(s"$conformadoBasePath/part-*"))(0).getPath.getName

    fs.rename(new Path(s"$conformadoBasePath/$file"), new Path(s"$conformadoBasePath/$hiveTable.orc"))

    // ---------------------------------------------------------------
    // FIN ESCRITURA HDFS
    // ---------------------------------------------------------------

    spark.close()
  }
}