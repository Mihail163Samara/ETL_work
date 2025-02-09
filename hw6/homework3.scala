/*
Задание 3 с семинара
Объединить время, статус, группу в один столбец

chcp 65001 && spark-shell -i "/home/spark/file_home_worke/homework3.scala" --conf "spark.driver.extraJavaOptions=-Dfile.encoding=utf-8"
*/

val t1 = System.currentTimeMillis()
var df1 = spark.read.format("com.crealytics.spark.excel")
        .option("sheetName", "Sheet1")
        .option("useHeader", "false")
        .option("treatEmptyValuesAsNulls", "false")
        .option("inferSchema", "true").option("addColorColumns", "true")
		.option("usePlainNumberFormat","true")
        .option("startColumn", 0)
        .option("endColumn", 99)
        .option("timestampFormat", "MM-dd-yyyy HH:mm:ss")
        .option("maxRowsInMemory", 20)
        .option("excerptSize", 10)
        .option("header", "true")
        .format("excel")
        .load("/home/spark/file_home_worke/s3.xlsx")
		df1.write.format("jdbc").option("url","jdbc:mysql://localhost:33061/spark?user=root&password=1")
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "sem3tab_01")
        .mode("overwrite").save()
val q = """
		SELECT ID_тикета, FROM_UNIXTIME(Status_time) AS Status_time,
		(LEAD(Status_time) OVER(PARTITION BY ID_тикета ORDER BY Status_time) - Status_time)/3600 AS Длительность,
		(CASE 
			WHEN Статус IS NULL THEN @PREV1 ELSE @PREV1:= Статус END) AS Статус,
		(CASE 
			WHEN Группа IS NULL THEN @PREV2 ELSE @PREV2:= Группа END) AS Группа, 
		Назначение
		FROM (SELECT ID_тикета, Status_time, Статус, IF (ROW_NUMBER() OVER (PARTITION BY ID_тикета ORDER BY Status_time) = 1 AND Назначение IS NULL, '', Группа) AS Группа, Назначение 
		FROM (SELECT DISTINCT tab1.objectid AS ID_тикета,tab1.restime AS Status_time, tab2.Статус, tab3.Группа, tab3.Назначение,
		(SELECT @PREV1:=''), (SELECT @PREV2:='')
		FROM (SELECT DISTINCT objectid, restime FROM spark.sem3tab_01
		WHERE fieldname IN ('gname2', 'status')) AS tab1
		LEFT JOIN (SELECT DISTINCT objectid, restime, fieldvalue AS Статус FROM spark.sem3tab_01
		WHERE fieldname IN ('status')) AS tab2 ON tab1.objectid = tab2.objectid AND tab1.restime = tab2.restime
		LEFT JOIN (SELECT DISTINCT objectid, restime, fieldvalue AS Группа, 1 AS Назначение FROM spark.sem3tab_01 
		WHERE fieldname IN ('gname2')) AS tab3 ON tab1.objectid = tab3.objectid AND tab1.restime = tab3.restime) AS tab4) AS tab5
		"""
		spark.read.format("jdbc").option("url","jdbc:mysql://localhost:33061/spark?user=root&password=1")
        .option("driver", "com.mysql.cj.jdbc.Driver").option("query", q).load()
		.write.format("jdbc").option("url","jdbc:mysql://localhost:33061/spark?user=root&password=1")
        .option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "sem3tab_02")
        .mode("overwrite").save()
val query = """
    SELECT ID_тикета, 
    GROUP_CONCAT(CONCAT(Status_time, " " , Статус, " ", Группа) ORDER BY status_time SEPARATOR '; ') Назначение
    FROM sem3tab_02
    GROUP BY ID_тикета
    """

 df1 = spark.read.format("jdbc").option("url","jdbc:mysql://localhost:33061/spark?user=root&password=1")
.option("driver", "com.mysql.cj.jdbc.Driver").option("query", query)
.load()

df1.show()

df1.write.format("jdbc").option("url","jdbc:mysql://localhost:33061/spark?user=root&password=1")
.option("driver", "com.mysql.cj.jdbc.Driver").option("dbtable", "hwtab3")
.mode("overwrite").save()

println("homework 3 complete")

spark.stop()

val s0 = (System.currentTimeMillis() - t1)/1000
val s = s0 % 60
val m = (s0/60) % 60
val h = (s0/60/60) % 24
println("%02d:%02d:%02d".format(h, m, s))
System.exit(0)
