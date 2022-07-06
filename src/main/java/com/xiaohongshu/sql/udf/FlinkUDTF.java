package com.xiaohongshu.sql.udf;

import org.apache.flink.table.annotation.DataTypeHint;
import org.apache.flink.table.annotation.FunctionHint;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.types.Row;




@FunctionHint(
        input = @DataTypeHint("ARRAY<ROW < " +
                " user_id string," +
                " note_id string," +
                " ads_id string," +
                " advertiser_id string," +
                " campaign_id string," +
                " unit_id string," +
                " creative_id string," +
                " creativity_type string," +
                " track_type string," +
                " track_id string," +
                " request_time BIGINT," +
                " bidword string," +
                " matchtype int," +
                " matchscore double," +
                " bidword_source int," +
                " submatch_type int," +
                " bidword_flag int," +
                " duration_score double," +
                " bid int," +
                " given_cpa int," +
                " rough_bid double," +
                " pctr double," +
                " pcvr double," +
                " material string," +
                " ocpc_stage int," +
                " log_json string," +
                " filter_reason string," +
                " relevance_detail_info string," +
                " retrieval_strategy string," +
                " rough_filter_flag int, " +
                "relevance_filter_flag int >>"),
        output = @DataTypeHint(value = "ROW < " +
                " user_id string," +
                " note_id string," +
                " ads_id string," +
                " advertiser_id string," +
                " campaign_id string," +
                " unit_id string," +
                " creative_id string," +
                " creativity_type string," +
                " track_type string," +
                " track_id string," +
                " request_time BIGINT," +
                " bidword string," +
                " matchtype int," +
                " matchscore double," +
                " bidword_source int," +
                " submatch_type int," +
                " bidword_flag int," +
                " duration_score double," +
                " bid int," +
                " given_cpa int," +
                " rough_bid double," +
                " pctr double," +
                " pcvr double," +
                " material string," +
                " ocpc_stage int," +
                " log_json string," +
                " filter_reason string," +
                " relevance_detail_info string," +
                " retrieval_strategy string, " +
                "rough_filter_flag int, " +
                "relevance_filter_flag int >")
)
public class FlinkUDTF extends TableFunction<Row> {

    public void eval(Row[] adsInfos) {
        if(adsInfos!=null && adsInfos.length>0){
            for(Row adsInfo:adsInfos){
                collect(adsInfo);
            }
        }
    }

}
