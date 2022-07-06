package com.xiaohongshu.lucky;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.red.search.niumowang.lucky.LuckyParseConfigGenerator;
import com.red.search.niumowang.lucky.LuckyParseUtil;
import com.red.search.niumowang.lucky.dto.IndexConvertConfig;
import org.junit.Test;

import java.util.*;

public class LucyTest {

    @Test
    public void lucyTest(){
        String luckyJson = "{\n" +
                "  \"fields\": [\n" +
                "    {\n" +
                "      \"field_name\": \"id\",\n" +
                "      \"field_type\": \"INT64\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"field_name\": \"campaign_type\",\n" +
                "      \"field_type\": \"INT32\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"field_name\": \"state\",\n" +
                "      \"field_type\": \"INT32\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"field_name\": \"enable\",\n" +
                "      \"field_type\": \"INT32\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"field_name\": \"advertiser_id\",\n" +
                "      \"field_type\": \"INT64\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"field_name\": \"budget_state\",\n" +
                "      \"field_type\": \"INT32\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"field_name\": \"uac_budget_state\",\n" +
                "      \"field_type\": \"INT32\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"field_name\": \"marketing_target\",\n" +
                "      \"field_type\": \"INT32\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"field_name\": \"campaign_day_budget\",\n" +
                "      \"field_type\": \"INT32\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"field_name\": \"limit_day_budget\",\n" +
                "      \"field_type\": \"INT32\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"field_name\": \"constraint_type\",\n" +
                "      \"field_type\": \"INT32\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"field_name\": \"constraint_value\",\n" +
                "      \"field_type\": \"INT32\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"field_name\": \"pacing_mode\",\n" +
                "      \"field_type\": \"INT32\"\n" +
                "    },\n" +
                "    {\n" +
                "      \"field_name\": \"platform\",\n" +
                "      \"field_type\": \"INT32\"\n" +
                "    }\n" +
                "  ]\n" +
                "}";
        String jsonContent = "{\"id\":2254890,\"enable\":0,\"state\":0,\"advertiser_id\":118162,\"campaign_name\":\"5.6搜索\",\"limit_day_budget\":1,\"campaign_day_budget\":40000,\"budget_state\":1,\"balance_state\":1,\"create_audit\":\"{\\\"id\\\":\\\"623d6bdec434a40001a9dd46\\\",\\\"name\\\":\\\"云中寻翠_孔灵灵\\\",\\\"account_number\\\":\\\"5077347028\\\",\\\"account_type\\\":0,\\\"channel\\\":9}\",\"update_audit\":\"{\\\"id\\\":\\\"623d6bdec434a40001a9dd46\\\",\\\"name\\\":\\\"云中寻翠_孔灵灵\\\",\\\"account_number\\\":\\\"5077347028\\\",\\\"account_type\\\":0,\\\"channel\\\":9}\",\"create_time\":1651830549000,\"modify_time\":1652151760000,\"v_seller_id\":\"623d6bde0eee1100012bb3fa\",\"uac_budget_state\":0,\"marketing_target\":9,\"mode_type\":0,\"campaign_type\":1,\"start_time_test\":null,\"expire_time_test\":null,\"time_period_type\":1,\"time_period\":\"111000000000000011111111111000000000000011111111111000000000000011111111111000000000000011111111111000000000000011111111111000000000000011111111111000000000000011111111\",\"time_state\":2,\"placement\":2,\"optimize_target\":5,\"promotion_target\":1,\"bidding_strategy\":4,\"bid_type\":3,\"constraint_type\":5,\"constraint_value\":31,\"platform\":1,\"build_type\":0,\"origin_campaign_day_budget\":40000,\"campaign_smart_switch\":0,\"adv_budget_state\":1,\"start_time\":1651830549000,\"expire_time\":29947507200000,\"pacing_mode\":0,\"parent_id\":0,\"feed_flag\":0,\"hidden_flag\":0,\"accumulation_status\":0,\"migration_status\":0,\"budget_allocation_ratio\":\"0.0000000\",\"dtm\":\"20220627\"}";
        //通过lucky schema生成
        List<IndexConvertConfig> indexConvertConfigList =
                LuckyParseConfigGenerator.parseFromLuckyConfig(luckyJson);
        //parseTestInput 为Map<String,Object>
        Map<String,Object>parseTestInput = new HashMap<>();
        JSONObject jsonObject = JSONObject.parseObject(jsonContent);
        JSONObject fields = JSONObject.parseObject(luckyJson);
        JSONArray fieldsArr = fields.getJSONArray("fields");
        fieldsArr.stream().forEach(field->{
            parseTestInput.put(((JSONObject)field).get("field_name").toString(),jsonObject.get(((JSONObject)field).get("field_name")));
        });




        String parseResult = LuckyParseUtil.parseFromMap(parseTestInput, indexConvertConfigList);
        System.out.println(parseResult);




    }

    @Test
    public void uuid(){
        System.out.println(UUID.randomUUID().toString().replaceAll("-","").toLowerCase());
    }





    static String buildHead(String tableName, Object slotColumn, Object key, int slot) {
        StringBuilder builder = new StringBuilder();
        int slotIndex;
        if (slotColumn != null) {
            slotIndex = calculateSlotIndex(slotColumn, slot);
        } else {
            slotIndex = calculateSlotIndex(String.valueOf(key), slot);
        }

        builder.append("{").append(tableName).append(':').append(slotIndex).append("}").append(key);
        return builder.toString();
    }

    public static int calculateSlotIndex(Object slotColumn, int totalSlot) {
        return Math.abs(String.valueOf(slotColumn).hashCode()) % totalSlot;
    }

    @Test
    public void head(){
        System.out.println(buildHead("t_landing_page", null,"5ee9c5e643f6f000010bd032", 32));
    }
}
