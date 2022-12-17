# Databricks notebook source
dataList = [("Java", 20000), ("Python", 300000), ("Scala", 500000)]
rdd = spark.sparkContext.parallelize(dataList)
type(rdd)

# COMMAND ----------

containerName = "ecommercecontainer"
storageAccountName = "myecomstorage"
accountkey = "nBrxaq9qc8aUEAJSKZHd0Hx8KLrFHDgxlXXAlVSSR07BOB6/Qa+mtjVz9Sj7Iu/tjcCBqIsL5Ut0+AStcGx42w==" #Copied from Accesskeys
config = "fs.azure.sas." + containerName+ "." + storageAccountName + ".blob.core.windows.net"
spark.conf.set("fs.azure.account.key.{storage}.dfs.core.windows.net".format(storage=storageAccountName), accountkey)
PATH_TEMPLATE = "abfss://ecommercecontainer@myecomstorage.dfs.core.windows.net"
RAW_PATH = PATH_TEMPLATE.format(container=containerName, storage=storageAccountName)
RAW_FOLDER_PATH = '/rawlayer/'
PATH=RAW_PATH+RAW_FOLDER_PATH
print(RAW_PATH+RAW_FOLDER_PATH)

# COMMAND ----------

print(PATH)
df=spark.read.csv(PATH,sep=',',inferSchema=True)
display(df)

# COMMAND ----------

# MAGIC %fs mkdirs /ecommerce

# COMMAND ----------

# MAGIC %fs ls ecommerce/

# COMMAND ----------

df.write.csv('/ecommerce/raw2_source/')

# COMMAND ----------

# MAGIC %fs ls ecommerce/raw2_source/

# COMMAND ----------

# MAGIC %sql
# MAGIC create table ecommerce_purchase_invoice
# MAGIC (id int, order_status string, order_products_value float, order_freight_value float, order_items_qty int, customer_city string, customer_state string,customer_zip_code_prefix int, product_name_lenght double,product_description_lenght double, product_photos_qty double, review_score double, order_purchase_timestamp string, order_aproved_at string, order_delivered_customer_date string) USING csv OPTIONS (PATH "dbfs:/ecommerce/raw2_source/")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ecommerce_purchase_invoice LIMIT 20;

# COMMAND ----------

df=spark.read.csv(PATH,sep=',',inferSchema=True)
display(df)

# COMMAND ----------

#Convert “order_purchase_timestamp” to week and day using UDF
from pyspark.sql import functions as F
df = spark.table("ecommerce_purchase_invoice")
df = df.withColumn("order_purchase_timestamp_nw", F.split("order_purchase_timestamp", ' ')[0])
df = df.withColumn("order_purchase_timestamp_nw", F.to_timestamp("order_purchase_timestamp_nw", 'dd/MM/yy'))
df = df.withColumn("day", F.dayofmonth("order_purchase_timestamp_nw"))
df = df.withColumn("week", F.weekofyear("order_purchase_timestamp_nw"))
display(df)

# COMMAND ----------

df.createOrReplaceTempView('ecommerceview')

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from ecommerceview;

# COMMAND ----------

# MAGIC 
# MAGIC %sql
# MAGIC select sum(order_freight_value) from ecommerceview;

# COMMAND ----------

df1 = spark.sql(""" select id, order_status,sum(order_products_value),sum(order_freight_value),sum(order_items_qty),customer_city,week
from ecommerceview group by id,order_status,customer_city,week
 """)
df.write.csv(RAW_PATH+'/proceessed_data2')

df1 = spark.sql(""" select id, order_status,sum(order_products_value),sum(order_freight_value),customer_state,day
from ecommerceview group by id,order_status,customer_state,day
 """)
df.write.csv(RAW_PATH+'/proceessed_data3')

df1 = spark.sql(""" select id, order_status,sum(order_products_value),sum(order_freight_value),customer_state,week
from ecommerceview group by id,order_status,customer_state,week
 """)
df.write.csv(RAW_PATH+'/proceessed_data4')

df1 = spark.sql(""" select id, avg(review_score), avg(order_freight_value),
 avg(order_products_value), order_delivered_customer_date
 from ecommerceview group by id,review_score,order_freight_value,order_products_value,order_delivered_customer_date
 """)
df.write.csv(RAW_PATH+'/proceessed_data5')

df1 = spark.sql(""" select customer_city, avg(order_freight_value) from ecommerceview group by customer_city,order_freight_value
 """)
df.write.csv(RAW_PATH+'/proceessed_data6')

df1 = spark.sql(""" select sum(order_freight_value) from ecommerceview
 """)
df.write.csv(RAW_PATH+'/proceessed_data7')

# COMMAND ----------

# MAGIC %sql
# MAGIC select id, order_status,sum(order_products_value),sum(order_freight_value),sum(order_items_qty),customer_city,day
# MAGIC from ecommerceview group by id,order_status,customer_city,day;-------------day total sales
