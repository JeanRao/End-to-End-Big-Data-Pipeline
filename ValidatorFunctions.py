def checkNulls(spark, inputDF, Layer, RcvSystem, config):

    checkNullQuery = "SELECT Sale_ID from inputDF WHERE "
    config.createOrReplaceTempView("config")
    inputDF.createOrReplaceTempView("inputDF")
    configToCheck = spark.sql("SELECT col_name from config where Recv_Sys ='" + RcvSystem + "'" +
      "AND Layer ='" + Layer + "'" + " AND mandatory = 'true'").collect()

    for columns in configToCheck:
      checkNullQuery += columns[0] + " IS NULL OR "

    checkNullQuery = checkNullQuery[:-3]

    invalidDF = spark.sql(checkNullQuery)

    if invalidDF.count() > 0 :
      print("Invalid")
      invalidDF.show()
    else:
      print("Valid")


def checkLengths(spark, inputDF, Layer, RcvSystem, config):
    checkLengthQuery = "SELECT Sale_ID from inputDF WHERE "
    config.createOrReplaceTempView("config")
    inputDF.createOrReplaceTempView("inputDF")
    configToCheck = spark.sql("SELECT col_name, col_length from config where Recv_Sys ='" + RcvSystem + "'" +
      "AND Layer ='" + Layer + "'" + " AND mandatory = 'true'").collect()

    for columns in configToCheck:
      checkLengthQuery += "length(" + columns[0] + ") > " + str(columns[1]) + " OR "

    checkLengthQuery = checkLengthQuery[:-3]

    invalidDF = spark.sql(checkLengthQuery)

    if invalidDF.count() > 0:
      print("Invalid")
      invalidDF.show()
    else :
      print("Valid")