from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, TimestampType,DoubleType
from src.main.python.ValidatorFunctions import checkLengths, checkNulls


spark = SparkSession.builder.appName("DFValidatpr").master("local").getOrCreate()
landingFileSchema = StructType([
    StructField("Sale_ID", StringType(), False),
    StructField("Product_ID", StringType(), False),
    StructField("Quantity_Sold", IntegerType(), False),
    StructField("Vendor_ID", StringType(), False),
    StructField("Sale_Date", TimestampType(), False),
    StructField("Sale_Amount", DoubleType(), False),
    StructField("Sale_Currency", StringType(), False)
  ])

configDFSchema = StructType([
    StructField("Recv_Sys", StringType(), False),
    StructField("Layer", StringType(), False),
    StructField("col_name", StringType(), False),
    StructField("col_data_type", StringType(), False),
    StructField("col_length", IntegerType(), False),
    StructField("mandatory", BooleanType(), False)
  ])

landingFileDF = spark.read\
    .schema(landingFileSchema)\
    .option("delimiter", "|")\
    .csv("<path>")

configDF = spark.read\
      .schema(configDFSchema)\
      .option("delimiter", "|")\
      .csv("<path>")

checkLengths(spark, landingFileDF, "Layer1", "SAP", configDF)