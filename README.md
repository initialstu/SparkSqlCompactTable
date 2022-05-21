# SparkSqlCompactTable

## 题目：实现 Compact table command

### 添加 compact table 命令，用于合并小文件，例如表 test1 总共有 50000 个文件，每个 1MB，通过该命令，合成为 500 个文件，每个约 100MB

* 语法：COMPACT TABLE table_identify [partitionSpec] [INTO fileNum FILES]

核心代码

* SqlBase.g4

```
statement
    | COMPACT TABLE target=tableIdentifier partitionSpec?
(INTO fileNum=INTEGER_VALUE FILES)?                           #compactTable

nonReserved
    | FILES

ansiNonReserved
    | FILES

//============================
// Start of the keywords list
//============================
FILES: 'FILES';
```

* 使用antlr4编译生成SqlBaseParser代码

```
# idea执行路径
maven -> spark project calalyst -> plugins -> antlr4 -> antlr4:antlr4(双击执行)
```

* 增加CompactTableCommand类

```scala
case class CompactTableCommand(table: TableIdentifier,fileNum: Option[Int]) extends LeafRunnableCommand {
    override def output: Seq[Attribute] = Seq(AttributeReference("compact_table", StringType, false)())
    
    override def run(sparkSession: SparkSession): Seq[Row] = {
      val dataDF: DataFrame = spark.table(table.identifier)
      val num: Int = fileNum match {
        case Some(i) => i
        case _ =>
          (spark
            .sessionState
            .executePlan(dataDF.queryExecution.logical)
            .optimizedPlan
            .stats.sizeInBytes / (1024L * 1024L * 128L)).toInt
      }
      val tmpTableName = table.identifier + "_tmp"
      dataDF.repartition(num).write.mode(SaveMode.Overwrite).saveAsTable(tmpTableName)
      spark.sql("drop table if exists %s".format(table.identifier))
      spark.sql("alter table %s rename to %s".format(tmpTableName, table.identifier))
      val output = "Compact Table %s Into %d Files Completed.".format(table.identifier, num)
      Seq(Row(output))
  }
}
```

* SparkSqlParser.scala增加visitCompactTable方法

```scala
override def visitCompactTable(ctx: CompactTableContext): LogicalPlan = withOrigin(ctx) {
    val table: TableIdentifier = visitTableIdentifier(ctx.tableIdentifier())
    val fileNum: Option[Int] = ctx.INTEGER_VALUE().getText.toInt
    CompactTableCommand(table, fileNum)
}
```

执行结果

```
spark-sql> compact table chaicq_compact into 1 files;
Compact Table chaicq_compact Into 1 Files Completed.
Time taken: 26.342 seconds, Fetched 1 row(s)
```

