from pyspark.sql.functions import col, lag, sum as _sum, max as _max, coalesce
from pyspark.sql.window import Window
import pyspark,time,platform,sys,os
from datetime import datetime
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import col,lit,current_timestamp
import pandas as pd
import matplotlib.pyplot as plt 
from sqlalchemy import inspect,create_engine
from pandas.io import sql
import warnings,matplotlib
warnings.filterwarnings("ignore")
t0=time.time()
con=create_engine("mysql://root:qwaszx123!Q@localhost/spark")
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark=SparkSession.builder.appName("Hi").getOrCreate()
columns = ["id","category_id","rate","title","author"]
data = [("1", "1","5","java","author1"),
        ("2", "1","5","scala","author2"),
        ("3", "1","5","python","author3")]
if 1==11:
    df = spark.createDataFrame(data,columns)
    df.withColumn("id",col("id").cast("int"))\
        .withColumn("category_id",col("category_id").cast("int"))\
        .withColumn("rate",col("rate").cast("int"))\
        .withColumn("dt", current_timestamp())\
        .write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=qwaszx123!Q")\
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "tasketl4a")\
        .mode("overwrite").save()
    df1 = spark.read.format("com.crealytics.spark.excel")\
        .option("sheetName", "Sheet1")\
        .option("useHeader", "false")\
        .option("treatEmptyValuesAsNulls", "false")\
        .option("inferSchema", "true").option("addColorColumns", "true")\
	.option("usePlainNumberFormat","true")\
        .option("startColumn", 0)\
        .option("endColumn", 99)\
        .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")\
        .option("maxRowsInMemory", 20)\
        .option("excerptSize", 10)\
        .option("header", "true")\
        .format("excel")\
        .load("/Users/admin/OneDrive/Рабочий стол/учёба/ETL автоматизация подготовки данных (семинары)/семинар 4. Партицирование данных по дате. Динамическое партицирование/Geekbrains ETL/s4.xlsx").where(col("title") == "news")\
        .withColumn("dt", current_timestamp())\
        .write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=qwaszx123!Q")\
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "tasketl4a")\
        .mode("append").save()
#Задача 2
sql.execute("""drop table if exists spark.`tasketl4b`""",con)
sql.execute("""CREATE TABLE if not exists spark.`tasketl4b` (
	`№` INT(10) NULL DEFAULT NULL,
	`Месяц` DATE NULL DEFAULT NULL,
	`Сумма платежа` FLOAT NULL DEFAULT NULL,
	`Платеж по основному долгу` FLOAT NULL DEFAULT NULL,
	`Платеж по процентам` FLOAT NULL DEFAULT NULL,
	`Остаток долга` FLOAT NULL DEFAULT NULL,
	`проценты` FLOAT NULL DEFAULT NULL,
	`долг` FLOAT NULL DEFAULT NULL
)
COLLATE='utf8mb4_0900_ai_ci'
ENGINE=InnoDB""",con)
from pyspark.sql.window import Window
from pyspark.sql.functions import sum as sum1
w = Window.partitionBy(lit(1)).orderBy("№").rowsBetween(Window.unboundedPreceding, Window.currentRow)
df1 = spark.read.format("com.crealytics.spark.excel")\
        .option("sheetName", "Sheet1")\
        .option("useHeader", "false")\
        .option("treatEmptyValuesAsNulls", "false")\
        .option("inferSchema", "true").option("addColorColumns", "true")\
	.option("usePlainNumberFormat","true")\
        .option("startColumn", 0)\
        .option("endColumn", 99)\
        .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")\
        .option("maxRowsInMemory", 20)\
        .option("excerptSize", 10)\
        .option("header", "true")\
        .format("excel")\
        .load("/Users/admin/OneDrive/Рабочий стол/учёба/ETL автоматизация подготовки данных (семинары)/семинар 4. Партицирование данных по дате. Динамическое партицирование/Geekbrains ETL/s4_2.xlsx").limit(1000)\
        .withColumn("проценты", sum1(col("Платеж по процентам")).over(w))\
        .withColumn("долг", sum1(col("Платеж по основному долгу")).over(w))
df1.write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=qwaszx123!Q")\
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "tasketl4b")\
        .mode("append").save()
df20 = spark.read.format("com.crealytics.spark.excel")\
        .option("dataAddress", "'Лист2'!A1:F269")\
        .option("useHeader", "false")\
        .option("treatEmptyValuesAsNulls", "false")\
        .option("inferSchema", "true").option("addColorColumns", "true")\
	.option("usePlainNumberFormat","true")\
        .option("startColumn", 0)\
        .option("endColumn", 99)\
        .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")\
        .option("maxRowsInMemory", 20)\
        .option("excerptSize", 10)\
        .option("header", "true")\
        .format("excel")\
        .load("/Users/admin/OneDrive/Рабочий стол/учёба/ETL автоматизация подготовки данных (семинары)/семинар 4. Партицирование данных по дате. Динамическое партицирование/Geekbrains ETL/s4_2.xlsx").limit(1000)\
        .write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=qwaszx123!Q")\
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "tasketl4b20")\
        .mode("overwrite").save()
q  = """
SELECT №, 
Дата, 
SUM(`Сумма платежа`) `Сумма платежа`, 
SUM(`Платеж по процентам`) `Платеж по процентам`, 
round(MAX(`Долг`),2) Долг120,
round(`Проценты`,2) Проценты120 
FROM
(SELECT 
IFNULL(№, LAG(№) OVER()) AS №, 
Дата,
`Сумма платежа`,
`Платеж по основному долгу`,
`Платеж по процентам`,
`Остаток долга`,
SUM(`Платеж по основному долгу`) over( rows BETWEEN UNBOUNDED PRECEDING AND current ROW ) `Долг`,
SUM(`Платеж по процентам`) over( rows BETWEEN UNBOUNDED PRECEDING AND current ROW ) `Проценты`
FROM tasketl4b20) tab1
GROUP BY 1,2,6
ORDER BY 1
"""
df2 = spark.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=qwaszx123!Q")\
        .option("driver", "com.mysql.cj.jdbc.Driver").option("query", q)\
        .load()
df30 = spark.read.format("com.crealytics.spark.excel")\
        .option("dataAddress", "'Лист3'!A1:F93")\
        .option("useHeader", "false")\
        .option("treatEmptyValuesAsNulls", "false")\
        .option("inferSchema", "true").option("addColorColumns", "true")\
	.option("usePlainNumberFormat","true")\
        .option("startColumn", 0)\
        .option("endColumn", 99)\
        .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")\
        .option("maxRowsInMemory", 20)\
        .option("excerptSize", 10)\
        .option("header", "true")\
        .format("excel")\
        .load("/Users/admin/OneDrive/Рабочий стол/учёба/ETL автоматизация подготовки данных (семинары)/семинар 4. Партицирование данных по дате. Динамическое партицирование/Geekbrains ETL/s4_2.xlsx").limit(1000)\
# Определение оконной функции
window_spec = Window.orderBy("Дата").rowsBetween(Window.unboundedPreceding, Window.currentRow)
window_lag = Window.orderBy("Дата")

# Добавление колонок
df30 = df30.withColumn("№", coalesce(col("№"), lag("№").over(window_lag)))\
      .withColumn("Остаток долга", _sum(col("Платеж по основному долгу")).over(window_spec))
#df30.show(1000)
# Группировка данных
df3 = (
    df30.groupBy("№", "Дата")
    .agg(
        _sum("Сумма платежа").alias("Сумма платежа"),
        _sum("Платеж по основному долгу").alias("Платеж по основному долгу"),
        _sum("Платеж по процентам").alias("Платеж по процентам"),
        _max("Остаток долга").alias("Остаток долга"),
    )
    .orderBy("№")
)
#df3.show(1000)
df3 = df3.withColumn("Проценты250", sum1(col("Платеж по процентам")).over(w))\
         .withColumn("Долг250", sum1(col("Платеж по основному долгу")).over(w))
# Вывод результата
#df3.show(1000)
#exit()

df11 = df1.toPandas()
df22 = df2.toPandas()
df33 = df3.toPandas()

# Get current axis 
ax = plt.gca()
ax.ticklabel_format(style='plain')
# bar plot
df11.plot(kind='line', 
        x='№', 
        y='долг', 
        color='green', ax=ax)
df11.plot(kind='line', 
        x='№', 
        y='проценты', 
        color='red', ax=ax)

df22.plot(kind='line', 
        x='№', 
        y='Долг120', 
        color='blue', ax=ax)
df22.plot(kind='line', 
        x='№', 
        y='Проценты120', 
        color='orange', ax=ax)
df33.plot(kind='line', 
        x='№', 
        y='Долг250', 
        color='purple', ax=ax)
df33.plot(kind='line', 
        x='№', 
        y='Проценты250', 
        color='black', ax=ax)


# set the title 
plt.title('Выплаты')
plt.grid ( True )
ax.set(xlabel=None)
# show the plot 
plt.show() 
spark.stop()
t1=time.time()
print('finished',time.strftime('%H:%M:%S',time.gmtime(round(t1-t0))))

