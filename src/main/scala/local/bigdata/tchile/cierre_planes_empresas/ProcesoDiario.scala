package local.bigdata.tchile.cierre_planes_empresas

import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.types.{StringType, StructField, StructType}

object ProcesoDiario {
  def main(args: Array[String]): Unit = {
    // ---------------------------------------------------------------
    // INICIO PARAMETRIZACION
    // ---------------------------------------------------------------

    // BASE

    val conf = new SparkConf().setMaster("yarn").setAppName("B2B").set("spark.driver.allowMultipleContexts", "true").set("spark.yarn.queue", "default").set("spark.sql.crossJoin.enabled", "true").set("spark.sql.debug.maxToStringFields", "1000").set("spark.sql.autoBroadcastJoinThreshold", "-1")
    val spark: SparkSession = SparkSession.builder().config(conf).config("spark.sql.warehouse.dir", "/usr/hdp/current/spark-client/conf/hive-site.xml").config("spark.submit.deployMode", "cluster").enableHiveSupport().getOrCreate()
    val fs = FileSystem.get(spark.sparkContext.hadoopConfiguration)
    import spark.implicits._

    // PATHS

    val hiveTable = "fil_planes_empresa"
    val conformadoBasePath = s"/data/parque_asignado/parque/b2b/scel/planes_empresa/$hiveTable"

    val parqueMovilKfactorBasePath = "/data/producto_asignado/parque_movil/kfactor/tbln_parque_full/raw"
    val parqueMovilKfactorLastYear = fs.listStatus(new Path(parqueMovilKfactorBasePath)).filter(_.isDirectory()).map(_.getPath.toString).sortWith(_ > _)(0)
    val parqueMovilKfactorLastYearMonth = fs.listStatus(new Path(parqueMovilKfactorLastYear)).filter(_.isDirectory()).map(_.getPath.toString).sortWith(_ > _)(0)
    val parqueMovilKfactorPath = fs.listStatus(new Path(parqueMovilKfactorLastYearMonth)).filter(_.isDirectory()).map(_.getPath.toString).sortWith(_ > _)(0)

    val parqueMovilSistPath = "/data/parque_asignado/parque/b2b/scel/prq_movil_sist/fil/"

    val odsBasePath = "/modelos/ods"
    val assignedProductPath = s"$odsBasePath/assigned_product/conformado"
    val customerPath = s"$odsBasePath/customer/conformado"
    val subscribersPath = s"$odsBasePath/subscribers/conformado"
    val assignedProductParameterPath = s"$odsBasePath/assigned_product_parameter/conformado"
    val productPath = s"$odsBasePath/product/conformado"
    val customerSubTypePath = s"$odsBasePath/customer_sub_type/conformado"
    val assignedBillingOfferPath = s"$odsBasePath/assigned_billing_offer/conformado"
    val productCatalogPath = s"$odsBasePath/product_catalog/conformado"
    val productBillingParametersPath = s"$odsBasePath/product_billing_parameters/conformado"
    val detalleFacturacionPath = "/modelos/finanzas/detalle_facturacion/conformado"
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

    // Cuentas Financieras

    val cuentasFinancieras = spark.read.parquet(parqueMovilKfactorPath).
      filter(
        !$"SUBSCRIBER_STATUS_ID".isin("T", "C", "L")
        and $"CUSTOMER_TYPE_IND" === "C"
        and !$"financial_account_key".isNull
        and !$"subscriber_key".isNull
      ).
      select("financial_account_key", "subscriber_key").
      distinct()

    // Cargos y Descuentos Multiproducto Parametrizables

    val cargosDescuentosMultiproductoParametrizablesAssignedProduct = spark.read.parquet(assignedProductPath).
      filter(
        $"PRODUCT_KEY".isin(2053, 2448119)
        and $"ASSIGNED_PRODUCT_STATUS_KEY".isin(1351, 1359)
        and $"ASSIGNED_PRODUCT_STATE_KEY" === 1331
        and $"END_DATE" > java.time.LocalDate.now.toString
        and !$"customer_key".isNull
      ).
      select(
        "PRODUCT_KEY",
        "SUBSCRIBER_KEY",
        "ASSIGNED_PRODUCT_KEY",
        "ASSIGNED_PRODUCT_ID",
        "START_DATE"
      )

    val cargosDescuentosMultiproductoParametrizablesSubscribers = spark.read.parquet(subscribersPath).
      filter(
        $"SUBSCRIBER_STATUS_KEY".isin(1376, 1378, 1379)
        and $"CALCULATED_PAYMENT_CATEGORY_KE".isin("1402", "1403")
      ).
      select(
        "SUBSCRIBER_KEY",
        "PRIMARY_RESOURCE_VALUE",
        "CUSTOMER_KEY",
        "ORIGINAL_ACTIVATION_DATE"
      )

    val cargosDescuentosMultiproductoParametrizablesCustomer = spark.read.parquet(customerPath).
      filter(
        $"CUSTOMER_TYPE_IND" === "C"
        and $"rut_id" =!= "#"
        and $"DML_IND" =!= "DELETE"
        and $"customer_sub_type_key".isin(3042, 3043, 3044, 3048, 3053, 3054, 3055, 3059, 3060, 3081, 3084)
      ).
      select(
        "RUT_ID",
        "CUSTOMER_LEGAL_NAME",
        "CUSTOMER_KEY",
        "CUSTOMER_SUB_TYPE_KEY",
        "BILLING_CYCLE_KEY"
      )

    val cargosDescuentosMultiproductoParametrizablesAssignedProductParameter = spark.read.parquet(assignedProductParameterPath).
      filter($"PRODUCT_PARAMETER_KEY".isin(26754, 27217, 27180, 27183, 27178)).
      select(
        "ASSIGNED_PRODUCT_KEY",
        "ASSIGNED_PRODUCT_PARAMETER_ID",
        "PARAMETER_VALUE",
        "PRODUCT_PARAMETER_KEY"
      )

    val cargosDescuentosMultiproductoParametrizablesProduct = spark.read.parquet(productPath).
      select("PRODUCT_KEY", "PRODUCT_DESC")

    val cargosDescuentosMultiproductoParametrizablesCustomerSubType = spark.read.parquet(customerSubTypePath).
      select("CUSTOMER_SUB_TYPE_KEY", "CUSTOMER_SUB_TYPE_DESC")

    val cargosDescuentosMultiproductoParametrizablesAssignedBillingOffer = spark.read.parquet(assignedBillingOfferPath).
      filter(
        $"ASGN_BLNG_OFR_STATE_KEY" === 1331
        and $"ASGN_BLNG_OFR_STATUS_KEY".isin(1351, 1359)
        and $"PARENT_PRODUCT_KEY" === "2053"
        and $"END_DATE" > java.time.LocalDate.now.toString
      ).
      select("SUBSCRIBER_KEY", "ASSIGNED_PRODUCT_ID", "PRODUCT_KEY")

    val cargosDescuentosMultiproductoParametrizablesProductCatalog = spark.read.parquet(productCatalogPath).
      select("PRODUCT_KEY", "PRODUCT_VALID_FROM_DATE")

    val cargosDescuentosMultiproductoParametrizablesProductBillingParameters = spark.read.parquet(productBillingParametersPath).
      filter(
        $"BILLING_OFFER_EXPIRATION_DATE".isNull
        and $"PACKAGE_EXPIRATION_DATE".isNull
        and $"DML_IND" =!= "DELETE"
        and $"BILLING_ATTRIBUTE_NAME".isin("Rate", "Discount value")
      ).
      select("PRODUCT_KEY", "ITEM_NAME", "BILLING_ATTRIBUTE_DEFAULT_VAL")

    val cargosDescuentosMultiproductoParametrizables = cargosDescuentosMultiproductoParametrizablesAssignedProduct.
      join(
        cargosDescuentosMultiproductoParametrizablesSubscribers,
        cargosDescuentosMultiproductoParametrizablesAssignedProduct("SUBSCRIBER_KEY") === cargosDescuentosMultiproductoParametrizablesSubscribers("SUBSCRIBER_KEY")
      ).
      join(
        cargosDescuentosMultiproductoParametrizablesCustomer,
        cargosDescuentosMultiproductoParametrizablesSubscribers("CUSTOMER_KEY") === cargosDescuentosMultiproductoParametrizablesCustomer("CUSTOMER_KEY")
      ).
      join(
        cargosDescuentosMultiproductoParametrizablesAssignedProductParameter,
        cargosDescuentosMultiproductoParametrizablesAssignedProduct("ASSIGNED_PRODUCT_KEY") === cargosDescuentosMultiproductoParametrizablesAssignedProductParameter("ASSIGNED_PRODUCT_KEY"),
        "left"
      ).
      join(
        cargosDescuentosMultiproductoParametrizablesProduct,
        cargosDescuentosMultiproductoParametrizablesAssignedProduct("PRODUCT_KEY") === cargosDescuentosMultiproductoParametrizablesProduct("PRODUCT_KEY"),
        "left"
      ).
      join(
        cargosDescuentosMultiproductoParametrizablesCustomerSubType,
        cargosDescuentosMultiproductoParametrizablesCustomer("CUSTOMER_SUB_TYPE_KEY") === cargosDescuentosMultiproductoParametrizablesCustomerSubType("CUSTOMER_SUB_TYPE_KEY"),
        "left"
      ).
      join(
        cargosDescuentosMultiproductoParametrizablesAssignedBillingOffer,
        cargosDescuentosMultiproductoParametrizablesAssignedProduct("SUBSCRIBER_KEY") === cargosDescuentosMultiproductoParametrizablesAssignedBillingOffer("SUBSCRIBER_KEY")
        and cargosDescuentosMultiproductoParametrizablesAssignedProduct("ASSIGNED_PRODUCT_ID") === cargosDescuentosMultiproductoParametrizablesAssignedBillingOffer("ASSIGNED_PRODUCT_ID"),
        "left"
      ).
      join(
        cargosDescuentosMultiproductoParametrizablesProduct.as("cargosDescuentosMultiproductoParametrizablesProduct2"),
        cargosDescuentosMultiproductoParametrizablesAssignedBillingOffer("PRODUCT_KEY") === $"cargosDescuentosMultiproductoParametrizablesProduct2.PRODUCT_KEY",
        "left"
      ).
      join(
        cargosDescuentosMultiproductoParametrizablesProductCatalog,
        $"cargosDescuentosMultiproductoParametrizablesProduct2.PRODUCT_KEY" === cargosDescuentosMultiproductoParametrizablesProductCatalog("PRODUCT_KEY"),
        "left"
      ).
      join(
        cargosDescuentosMultiproductoParametrizablesProductBillingParameters,
        cargosDescuentosMultiproductoParametrizablesAssignedBillingOffer("PRODUCT_KEY") === cargosDescuentosMultiproductoParametrizablesProductBillingParameters("PRODUCT_KEY"),
        "left"
      ).
      withColumn(
        "COD_DTO_TIPO",
        when(cargosDescuentosMultiproductoParametrizablesAssignedProduct("PRODUCT_KEY") =!= "2053",
          cargosDescuentosMultiproductoParametrizablesAssignedProduct("PRODUCT_KEY")).
          otherwise(null)
      ).
      withColumn(
        "PRODUCT_DESC_",
        when($"cargosDescuentosMultiproductoParametrizablesProduct2.PRODUCT_KEY" === "2053", $"cargosDescuentosMultiproductoParametrizablesProduct2.PRODUCT_DESC").
          otherwise(cargosDescuentosMultiproductoParametrizablesProduct("PRODUCT_DESC"))
      ).
      select(
        cargosDescuentosMultiproductoParametrizablesCustomer("RUT_ID"),
        cargosDescuentosMultiproductoParametrizablesCustomer("CUSTOMER_LEGAL_NAME"),
        cargosDescuentosMultiproductoParametrizablesCustomer("BILLING_CYCLE_KEY"),
        cargosDescuentosMultiproductoParametrizablesSubscribers("SUBSCRIBER_KEY"),
        cargosDescuentosMultiproductoParametrizablesSubscribers("PRIMARY_RESOURCE_VALUE").as("MSISDN"),
        cargosDescuentosMultiproductoParametrizablesCustomer("CUSTOMER_KEY"),
        cargosDescuentosMultiproductoParametrizablesCustomerSubType("CUSTOMER_SUB_TYPE_DESC").as("SUB_TYPE"),
        cargosDescuentosMultiproductoParametrizablesAssignedBillingOffer("PRODUCT_KEY").as("COD_PLAN_AMDOCS"),
        $"COD_DTO_TIPO",
        $"PRODUCT_DESC_".as("PRODUCT_DESC"),
        cargosDescuentosMultiproductoParametrizablesProductBillingParameters("ITEM_NAME").as("PRIT"),
        cargosDescuentosMultiproductoParametrizablesProductBillingParameters("BILLING_ATTRIBUTE_DEFAULT_VAL").as("CHARGE_VAL"),
        cargosDescuentosMultiproductoParametrizablesAssignedProductParameter("ASSIGNED_PRODUCT_PARAMETER_ID"),
        cargosDescuentosMultiproductoParametrizablesAssignedProductParameter("PARAMETER_VALUE"),
        cargosDescuentosMultiproductoParametrizablesAssignedProduct("START_DATE"),
        cargosDescuentosMultiproductoParametrizablesSubscribers("ORIGINAL_ACTIVATION_DATE"),
        cargosDescuentosMultiproductoParametrizablesProductCatalog("PRODUCT_VALID_FROM_DATE")
      )

    // Descuentos Puntuales

    val descuentosPuntualesAssignedProduct = spark.read.parquet(assignedProductPath)

    val descuentosPuntualesProduct = spark.read.parquet(productPath)

    val descuentosPuntualesSubscribers = spark.read.parquet(subscribersPath)

    val descuentosPuntualesCustomer = spark.read.parquet(customerPath)

    val descuentosPuntualesCustomerSubType = spark.read.parquet(customerSubTypePath)

    val descuentosPuntualesAssignedBillingOffer = spark.read.parquet(assignedBillingOfferPath)

    val descuentosPuntualesProductBillingParameters = spark.read.parquet(productBillingParametersPath)

    val descuentosPuntuales = descuentosPuntualesAssignedProduct.
      join(
        descuentosPuntualesProduct,
        descuentosPuntualesAssignedProduct("PRODUCT_KEY") === descuentosPuntualesProduct("PRODUCT_KEY"),
        "left"
      ).
      join(
        descuentosPuntualesSubscribers,
        descuentosPuntualesAssignedProduct("SUBSCRIBER_KEY") === descuentosPuntualesSubscribers("SUBSCRIBER_KEY")
        and descuentosPuntualesSubscribers("SUBSCRIBER_STATUS_KEY").isin(1376, 1378, 1379),
        "left"
      ).
      join(
        descuentosPuntualesCustomer,
        descuentosPuntualesSubscribers("CUSTOMER_KEY") === descuentosPuntualesCustomer("CUSTOMER_KEY")
        and descuentosPuntualesCustomer("CUSTOMER_TYPE_IND") === "C"
        and descuentosPuntualesCustomer("RUT_ID") =!= "#"
        and descuentosPuntualesCustomer("DML_IND") =!= "DELETE"
        and descuentosPuntualesCustomer("customer_sub_type_key").isin(3042, 3043, 3044, 3048, 3053, 3054, 3055, 3059, 3060, 3081, 3084)
      ).
      join(
        descuentosPuntualesCustomerSubType,
        descuentosPuntualesCustomer("CUSTOMER_SUB_TYPE_KEY") === descuentosPuntualesCustomerSubType("CUSTOMER_SUB_TYPE_KEY"),
        "left"
      ).
      join(
        descuentosPuntualesAssignedBillingOffer,
        descuentosPuntualesAssignedProduct("ASSIGNED_PRODUCT_ID") === descuentosPuntualesAssignedBillingOffer("ASSIGNED_PRODUCT_ID")
        and descuentosPuntualesAssignedProduct("SUBSCRIBER_KEY") === descuentosPuntualesAssignedBillingOffer("SUBSCRIBER_KEY")
        and descuentosPuntualesAssignedBillingOffer("ASGN_BLNG_OFR_STATE_KEY") === 1331
        and descuentosPuntualesAssignedBillingOffer("ASGN_BLNG_OFR_STATUS_KEY").isin(1351,1359)
        and descuentosPuntualesAssignedBillingOffer("PARENT_PRODUCT_KEY").isin("2713", "3133", "18865")
        and descuentosPuntualesAssignedBillingOffer("END_DATE") > java.time.LocalDate.now.toString,
        "left"
      ).
      join(
        descuentosPuntualesProduct.as("descuentosPuntualesProduct2"),
        descuentosPuntualesAssignedBillingOffer("PRODUCT_KEY") === $"descuentosPuntualesProduct2.PRODUCT_KEY",
        "left"
      ).
      join(
        descuentosPuntualesProductBillingParameters,
        descuentosPuntualesAssignedBillingOffer("PRODUCT_KEY") === descuentosPuntualesProductBillingParameters("PRODUCT_KEY")
        and descuentosPuntualesProductBillingParameters("BILLING_OFFER_EXPIRATION_DATE").isNull
        and descuentosPuntualesProductBillingParameters("BILLING_ATTRIBUTE_NAME").isin("Discount value", "Discount type")
        and descuentosPuntualesProductBillingParameters("DML_IND") =!= "DELETE"
        and descuentosPuntualesProductBillingParameters("PACKAGE_EXPIRATION_DATE").isNull,
        "left"
      ).
      filter(
        descuentosPuntualesAssignedProduct("PRODUCT_KEY").isin("2713", "3133", "18865")
        and descuentosPuntualesAssignedProduct("ASSIGNED_PRODUCT_STATUS_KEY").isin(1351, 1359)
        and descuentosPuntualesAssignedProduct("ASSIGNED_PRODUCT_STATE_KEY") === 1331
        and descuentosPuntualesAssignedProduct("END_DATE") > java.time.LocalDate.now.toString
        and !descuentosPuntualesAssignedProduct("CUSTOMER_KEY").isNull
      ).
      withColumn("PROVISION_DATE_", descuentosPuntualesAssignedBillingOffer("PROVISION_DATE")).
      withColumn("INTERVAL", $"descuentosPuntualesProduct2.PRICE_PLAN_PROMOTION_INTERVAL").
      withColumn("DURATION", $"descuentosPuntualesProduct2.PRICE_PLAN_PROMOTION_DURATION").
      withColumn(
        "FIN",
        when($"INTERVAL" === "M", expr("add_months(PROVISION_DATE_, DURATION)")).
        when($"INTERVAL" === "D", expr("date_add(PROVISION_DATE_, DURATION)")).
        otherwise(lit(null))
      ).
      select(
        descuentosPuntualesCustomer("RUT_ID"),
        descuentosPuntualesSubscribers("SUBSCRIBER_KEY"),
        descuentosPuntualesSubscribers("PRIMARY_RESOURCE_VALUE").as("MSISDN"),
        descuentosPuntualesCustomer("CUSTOMER_KEY"),
        //descuentosPuntualesAssignedBillingOffer("PRODUCT_KEY").as("COD_PLAN_AMDOCS"),
        descuentosPuntualesAssignedBillingOffer("PRODUCT_KEY"),
        $"descuentosPuntualesProduct2.PRODUCT_DESC",
        //descuentosPuntualesProductBillingParameters("BILLING_ATTRIBUTE_DEFAULT_VAL").as("CHARGE_VAL"),
        descuentosPuntualesProductBillingParameters("BILLING_ATTRIBUTE_DEFAULT_VAL"),
        descuentosPuntualesAssignedBillingOffer("PROVISION_DATE").as("INICIO"),
        $"FIN"
      )

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

    // Cargos Fijos Facturados

    val cargosFijosFacturados = spark.read.parquet(detalleFacturacionPath).
      filter(
        $"customer_sub_type_desc".isin("Corporaciones", "Gran Empresa", "Mediana", "Pequeña", "Micro", "Empresa Nueva", "Empresa Relacionada")
        and $"payment_category_desc".isin("Postpaid Payment", "Hybrid Payment")
        and $"charge_code_id".isin("RM_VOZ_PL_POS", "RM_DAT_PL_POS", "RM_VOZ_PL_HIB", "RM_DAT_PL_HIB", "RM_M2M_PL1", "RM_BAM_PL_POS", "RM_GPS_PL1", "OM_EQU_SUBS")
        and date_format(to_date($"bill_date"), "yyyyMM") === date_format(current_date(), "yyyyMM")
      ).
      groupBy("close_date", "SUBSCRIBER_KEY").
      agg(sum("charge_amount").as("arpu"))

    val planesPpu = spark.read.
      schema(planesPpuSchema).
      options(Map("delimiter" -> ",", "header" -> "true")).
      csv(planesPpuPath).
      withColumnRenamed("cod_plan", "COD_PLAN_AMDOCS")

    // Planes Empresas

    val rcConfigurableDiscount = cargosDescuentosMultiproductoParametrizables.
      filter(!upper($"PRODUCT_DESC").like("%RC%CONFIGURABLE%DISCOUNT%")).
      select(
        "RUT_ID",
        "CUSTOMER_LEGAL_NAME",
        "BILLING_CYCLE_KEY",
        "SUBSCRIBER_KEY",
        "MSISDN",
        "CUSTOMER_KEY",
        "SUB_TYPE",
        "COD_PLAN_AMDOCS",
        "PRODUCT_DESC",
        "START_DATE",
        "ORIGINAL_ACTIVATION_DATE",
        "PRODUCT_VALID_FROM_DATE"
      ).
      distinct()

    val cargoDatos = cargosDescuentosMultiproductoParametrizables.
      filter(trim(upper($"PRIT")).like("%CARGO%DATOS%")).
      select("SUBSCRIBER_KEY", "CHARGE_VAL", "COD_PLAN_AMDOCS", "START_DATE").
      distinct()

    val cargoVoz = cargosDescuentosMultiproductoParametrizables.
      filter(trim(upper($"PRIT")).like("%CARGO%VOZ%")).
      select("SUBSCRIBER_KEY", "CHARGE_VAL", "COD_PLAN_AMDOCS", "START_DATE").
      distinct()

    val descuentoDatos = cargosDescuentosMultiproductoParametrizables.
      filter(trim(upper($"PRIT")).like("%DESCUENTO%DATOS%")).
      select("SUBSCRIBER_KEY", "CHARGE_VAL", "COD_PLAN_AMDOCS", "START_DATE").
      distinct()

    val descuentoVoz = cargosDescuentosMultiproductoParametrizables.
      filter(trim(upper($"PRIT")).like("%DESCUENTO%VOZ%")).
      select("SUBSCRIBER_KEY", "CHARGE_VAL", "COD_PLAN_AMDOCS", "START_DATE").
      distinct()

    val discountAmountData = cargosDescuentosMultiproductoParametrizables.
      filter(trim(upper($"ASSIGNED_PRODUCT_PARAMETER_ID")).like("%DISCOUNTAAMOUNTDATARC%")).
      withColumn("PARAMETER_VALUE", when($"PARAMETER_VALUE".isin("0", "00"), null).otherwise($"PARAMETER_VALUE")).
      select("SUBSCRIBER_KEY", "PARAMETER_VALUE", "START_DATE").
      distinct()

    val discountAmountVoice = cargosDescuentosMultiproductoParametrizables.
      filter(trim(upper($"ASSIGNED_PRODUCT_PARAMETER_ID")).like("%DISCOUNTAMOUNTVOICERC%")).
      withColumn("PARAMETER_VALUE", when($"PARAMETER_VALUE".isin("0", "00"), null).otherwise($"PARAMETER_VALUE")).
      select("SUBSCRIBER_KEY", "PARAMETER_VALUE", "START_DATE").
      distinct()

    val discountTypeData = cargosDescuentosMultiproductoParametrizables.
      filter(trim(upper($"ASSIGNED_PRODUCT_PARAMETER_ID")).like("%DISCOUNTTYPEDATA%")).
      select("SUBSCRIBER_KEY", "PARAMETER_VALUE", "START_DATE").
      distinct()

    val discountTypeVoice = cargosDescuentosMultiproductoParametrizables.
      filter(trim(upper($"ASSIGNED_PRODUCT_PARAMETER_ID")).like("%DISCOUNTTYPEVOICE%")).
      select("SUBSCRIBER_KEY", "PARAMETER_VALUE", "START_DATE").
      distinct()

    val parqueMovilSist = spark.read.parquet(parqueMovilSistPath).
      select($"subcriptor".as("SUBSCRIBER_KEY"), $"tipo_plan")

    val planesEmpresas = rcConfigurableDiscount.
      join(cargoDatos.as("cargoDatos"), Seq("SUBSCRIBER_KEY", "COD_PLAN_AMDOCS"), "left").
      join(cargoVoz.as("cargoVoz"), Seq("SUBSCRIBER_KEY", "COD_PLAN_AMDOCS"), "left").
      join(descuentoDatos.as("descuentoDatos"), Seq("SUBSCRIBER_KEY", "COD_PLAN_AMDOCS"), "left").
      join(descuentoVoz.as("descuentoVoz"), Seq("SUBSCRIBER_KEY", "COD_PLAN_AMDOCS"), "left").
      join(discountAmountData.as("discountAmountData"), Seq("SUBSCRIBER_KEY"), "left").
      join(discountAmountVoice.as("discountAmountVoice"), Seq("SUBSCRIBER_KEY"), "left").
      join(discountTypeData.as("discountTypeData"), Seq("SUBSCRIBER_KEY"), "left").
      join(discountTypeVoice.as("discountTypeVoice"), Seq("SUBSCRIBER_KEY"), "left").
      join(cuentasFinancieras, Seq("SUBSCRIBER_KEY"), "left").
      join(parqueMovilSist, Seq("SUBSCRIBER_KEY"), "left").
      join(planesPpu, Seq("COD_PLAN_AMDOCS"), "left").
      join(descuentosPuntualesStg.filter($"POSICION" === 1).as("descuentosPuntuales1"), Seq("SUBSCRIBER_KEY"), "left").
      join(descuentosPuntualesStg.filter($"POSICION" === 2).as("descuentosPuntuales2"), Seq("SUBSCRIBER_KEY"), "left").
      join(descuentosPuntualesStg.filter($"POSICION" === 3).as("descuentosPuntuales3"), Seq("SUBSCRIBER_KEY"), "left").
      join(descuentosPuntualesStg.filter($"POSICION" === 4).as("descuentosPuntuales4"), Seq("SUBSCRIBER_KEY"), "left").
      join(descuentosPuntualesStg.filter($"POSICION" === 5).as("descuentosPuntuales5"), Seq("SUBSCRIBER_KEY"), "left").
      join(cargosFijosFacturados, Seq("SUBSCRIBER_KEY"), "left").

      withColumn("RAZON_SOCIAL", regexp_replace($"CUSTOMER_LEGAL_NAME", "|", "")).
      withColumn("PLAN", regexp_replace(rcConfigurableDiscount("PRODUCT_DESC"), "|", "")).
      withColumn("ARPU", coalesce(cargosFijosFacturados("ARPU"), lit(0))).
      withColumn("CARGO_FIJO_DATOS", coalesce($"cargoDatos.CHARGE_VAL", lit(0))).
      withColumn("CARGO_FIJO_VOZ", coalesce($"cargoVoz.CHARGE_VAL", lit(0))).
      withColumn("DESC_CARGO_FIJO_DATOS", coalesce($"descuentoDatos.CHARGE_VAL", lit(0))).
      withColumn("DESC_CARGO_FIJO_VOZ", coalesce($"descuentoVoz.CHARGE_VAL", lit(0))).
      withColumn("TOTAL_CF", $"CARGO_FIJO_DATOS" + $"CARGO_FIJO_VOZ" - $"DESC_CARGO_FIJO_DATOS" - $"DESC_CARGO_FIJO_VOZ").

      withColumn("DSC_ADICIONAL_DATOS", $"discountAmountData.PARAMETER_VALUE").
      withColumn("TIPO_DSC_ADI_DATOS", when($"DSC_ADICIONAL_DATOS".isNull, null).otherwise($"discountTypeData.PARAMETER_VALUE")).
      withColumn("DSC_ADICIONAL_VOZ", $"discountAmountVoice.PARAMETER_VALUE").
      withColumn("TIPO_DSC_ADI_VOZ", when($"DSC_ADICIONAL_VOZ".isNull, null).otherwise($"discountTypeVoice.PARAMETER_VALUE")).

      withColumn(
        "ARPU_DATOS",
        when(
          coalesce($"TIPO_DSC_ADI_DATOS", lit("Fixed")) === "Fixed",
          $"CARGO_FIJO_DATOS" - $"DESC_CARGO_FIJO_DATOS" - coalesce($"DSC_ADICIONAL_DATOS", lit(0))
        ).
          otherwise(
            when(
              coalesce($"DSC_ADICIONAL_DATOS", lit(0)) =!= 0,
              ($"CARGO_FIJO_DATOS" - $"DESC_CARGO_FIJO_DATOS") * (lit(100) - coalesce($"DSC_ADICIONAL_DATOS", lit(0))) / lit(100)).
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
              ($"DSC_ADICIONAL_VOZ" - $"CARGO_FIJO_VOZ") * (lit(100) - coalesce($"DSC_ADICIONAL_VOZ", lit(0))) / lit(100)).
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
      withColumn("FECHA_PROC", current_date()).
      withColumn("PERIODO", date_format(current_date(), "yyyyMM")).
      withColumn(
        "TIPO_PRODUCTO",
        coalesce(
          parqueMovilSist("tipo_plan"),
          when(
            upper(rcConfigurableDiscount("PRODUCT_DESC")).like("%GPS%")
            or upper(rcConfigurableDiscount("PRODUCT_DESC")).like("%M2M%"),
            "M2M"
          ).
            when(
              upper(rcConfigurableDiscount("PRODUCT_DESC")).like("%BAM%")
                or upper(rcConfigurableDiscount("PRODUCT_DESC")).like("%WIFI%MOVIL%"),
              "BAM"
            ).
            otherwise("VOZ")
        )
      ).
      withColumn(
        "TIPO_DSC",
        when(
          (!$"DSC_ADICIONAL_DATOS".isNull or !$"DSC_ADICIONAL_VOZ".isNull) and !descuentosPuntualesStg("PRODUCT_DESC").isNull and !$"DSC_PUNTUAL_1".isNull,
          lit("MULTIPRODUCTO+PARAMETRIZABLE+FIN DE CICLO+PUNTUAL")
        ).
          when(
            (!$"DSC_ADICIONAL_DATOS".isNull or !$"DSC_ADICIONAL_VOZ".isNull) and !descuentosPuntualesStg("PRODUCT_DESC").isNull and $"DSC_PUNTUAL_1".isNull,
            lit("MULTIPRODUCTO+PARAMETRIZABLE+FIN DE CICLO")
          ).
          when(
            (!$"DSC_ADICIONAL_DATOS".isNull or !$"DSC_ADICIONAL_VOZ".isNull) and descuentosPuntualesStg("PRODUCT_DESC").isNull and !$"DSC_PUNTUAL_1".isNull,
            lit("MULTIPRODUCTO+PARAMETRIZABLE+PUNTUAL")
          ).
          when(
            (!$"DSC_ADICIONAL_DATOS".isNull or !$"DSC_ADICIONAL_VOZ".isNull) and descuentosPuntualesStg("PRODUCT_DESC").isNull and $"DSC_PUNTUAL_1".isNull,
            lit("MULTIPRODUCTO+PARAMETRIZABLE")
          ).
          when(
            $"DSC_ADICIONAL_DATOS".isNull and $"DSC_ADICIONAL_VOZ".isNull and !descuentosPuntualesStg("PRODUCT_DESC").isNull and !$"DSC_PUNTUAL_1".isNull,
            lit("MULTIPRODUCTO+FIN DE CICLO+PUNTUAL")
          ).
          when(
            $"DSC_ADICIONAL_DATOS".isNull and $"DSC_ADICIONAL_VOZ".isNull and !descuentosPuntualesStg("PRODUCT_DESC").isNull and $"DSC_PUNTUAL_1".isNull,
            lit("MULTIPRODUCTO+FIN DE CICLO")
          ).
          when(
            $"DSC_ADICIONAL_DATOS".isNull and $"DSC_ADICIONAL_VOZ".isNull and descuentosPuntualesStg("PRODUCT_DESC").isNull and !$"DSC_PUNTUAL_1".isNull,
            lit("MULTIPRODUCTO+PUNTUAL")
          ).
          otherwise(lit("MULTIPRODUCTO"))
      ).
      select(
        rcConfigurableDiscount("RUT_ID").as("RUT"),
        $"RAZON_SOCIAL",
        rcConfigurableDiscount("BILLING_CYCLE_KEY").as("CICLO"),
        rcConfigurableDiscount("SUBSCRIBER_KEY"),
        cuentasFinancieras("FINANCIAL_ACCOUNT_KEY"),
        $"MSISDN",
        rcConfigurableDiscount("CUSTOMER_KEY"),
        rcConfigurableDiscount("SUB_TYPE").as("SEGMENTO"),
        rcConfigurableDiscount("COD_PLAN_AMDOCS").as("CODIGO_PLAN"),
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
        $"TIPO_PRODUCTO",
        $"TIPO_DSC",
        rcConfigurableDiscount("ORIGINAL_ACTIVATION_DATE").as("FECHA_ACTIVACION"),
        rcConfigurableDiscount("PRODUCT_VALID_FROM_DATE").as("FECHA_CREA_PLAN")
      ).
      distinct().
      join(planesPpu, $"CODIGO_PLAN" === planesPpu("COD_PLAN_AMDOCS"), "left").
      withColumn(
        "MARCA_USABLE",
        when(
          $"TIPO_DSC".like("%FIN%CICLO%")
            or $"TOTAL" < 0
            or ($"TOTAL" > 30000 and date_format($"FECHA_CREA_PLAN", "yyyy") >= 2020)
            or (planesPpu("COD_PLAN_AMDOCS").isNull and $"TOTAL" === 0),
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