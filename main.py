from pyspark.sql import SparkSession
from logger import logger
import datetime
import os


log_file = "/home/ritesh/Documents/logs/" + str(os.path.basename(__file__)) \
           + str(datetime.datetime.now().strftime("%Y%m%d_%H%M%S")) + ".log"
output_file = "/home/ritesh/Downloads/output/" + "City_Sales_Output" \
              + str(datetime.datetime.now().strftime("%Y%m%d")) + ".csv"

spark = SparkSession \
    .builder \
    .appName("POC") \
    .getOrCreate()

logs = logger(log_file)

try:
    df = spark.read \
        .format("csv") \
        .option("header", True) \
        .load("/home/ritesh/Downloads/train.csv")

    df2 = df.select("City", "Order ID").distinct() \
            .withColumnRenamed("Order ID", "Order_id")

    df2.createOrReplaceTempView("my_temp_tbl")
    spark.sql("select * from my_temp_tbl").show()
    df2.write.options(header='True', delimiter=',').mode("overwrite").csv(output_file)
    # spark.sql("select city, count(*) as Order_Count from my_temp_tbl group by city order by Order_Count desc").show()
    logs.info("Process Completed Successfully")

except Exception as e:
    logs.error("Encounter as Error : " + str(e))
