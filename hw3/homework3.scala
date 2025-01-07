/*
Задание 3 с семинара
Объединить время, статус, группу в один столбец

chcp 65001 && spark-shell -i "\spark\file_home_worke\homework3.scala" --conf "spark.driver.extraJavaOptions=-Dfile.encoding=utf-8"
*/

val t1 = System.currentTimeMillis()

val query = """
    SELECT ID_тикета, 
    GROUP_CONCAT(CONCAT(Status_time, " " , Статус, " ", Группа) ORDER BY status_time SEPARATOR '; ') Назначение
    FROM sem3tab_02
    GROUP BY ID_тикета
    """

var df1 = spark.read.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=qwaszx123!Q")
.option("driver", "com.mysql.cj.jdbc.Driver").option("query", query)
.load()

df1.show()

df1.write.format("jdbc").option("url","jdbc:mysql://localhost:3306/spark?user=root&password=qwaszx123!Q")
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
