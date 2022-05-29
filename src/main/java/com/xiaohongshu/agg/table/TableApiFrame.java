package com.xiaohongshu.agg.table;

public class TableApiFrame {
    /**
     * // 创建表环境
     * TableEnvironment tableEnv = ...;
     *
     * // 创建输入表，连接外部系统读取数据
     * tableEnv.executeSql("CREATE TEMPORARY TABLE inputTable ... WITH ( 'connector' = ... )");
     *
     * // 注册一个表，连接到外部系统，用于输出
     * tableEnv.executeSql("CREATE TEMPORARY TABLE outputTable ... WITH ( 'connector' = ... )");
     *
     * // 执行 SQL 对表进行查询转换，得到一个新的表
     * Table table1 = tableEnv.sqlQuery("SELECT ... FROM inputTable... ");
     *
     * // 使用 Table API 对表进行查询转换，得到一个新的表
     * Table table2 = tableEnv.from("inputTable").select(...);
     *
     * // 将得到的结果写入输出表
     * TableResult tableResult = table1.executeInsert("outputTable");
     */
}
