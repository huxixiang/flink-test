package com.xiaohongshu.common.flink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

public class ExecutionEnvironmentClient {


    public static StreamExecutionEnvironment getStreamExecEnv(){
        Configuration configuration = new Configuration();
        // 指定本地WEB-UI端口号
        configuration.setInteger(RestOptions.PORT, 8082);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment(configuration);
        return env;
    }



    public static StreamTableEnvironment getStreamTableEnvironment(StreamExecutionEnvironment env){
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().useBlinkPlanner().build();
        //table api的执行环境
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env,settings);
        return tableEnv;
    }
}
