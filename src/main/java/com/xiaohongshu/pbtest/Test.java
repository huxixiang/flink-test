package com.xiaohongshu.pbtest;


import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang3.StringUtils;

public class Test {
    public static void main(String[] args) {
        String s = "12@34@56@78@";
        System.out.println(StringUtils.split(s, "@", 2)[1]);
        System.out.println(JSONObject.toJSONString(StringUtils.split(s, "@", 2)));
        System.out.println(JSONObject.toJSONString(s.split("@")));

    }
}
