package com.xiaohongshu.common.utils;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @desc java集合中的初始化方式
 */
public class CollectionInitUtils {

    //java自带的初始化方式
    private Map<String, String> mMap = new HashMap<String, String>() {{
        put("k1", "v1");
        put("k2", "v2");
    }};


    //guava中的map初始化
    public static Map<String, String> getMap() {
        Map<String, String> mMap = ImmutableMap.of("k1", "v1", "k2", "v2");
        return mMap;
    }


    /**
     * set 初始化方式
     */

    Set<String> set = new HashSet<String>() {
        {
            add("morning");
            add("afternoon");
        }
    };

    //使用List初始化set
    String s = "a,b,c,d,e,f";
    Set<String> actionSet = new HashSet<>(Arrays.asList(s.split(",")));

    //stream 初始化set
    Set<String> streamSet = Stream.of("", "", "").collect(Collectors.toSet());

    public static void main(String[] args) {

//        // todo 构造需要写入kafka的数据结构
//        Map<String, String> sinkKafkaBidwords = response.getValue().stream().collect(
//                Collectors
//                        .toMap(i -> buildKafkaField(mt, i.getKey()), i -> i.getValue(),
//                                (a, b) -> a));

        List<String> list = Arrays.asList("1", "2", "1");
        Map<String, String> collect = list.stream().collect(Collectors.toMap(
                i->i,
                i->i,
                (v1,v2)-> v1
        ));

        System.out.println(collect.toString());

    }



    public static boolean getMergeList(Object v1,Object v2){
        if(v1 instanceof List && v2 instanceof List){
            ((List<?>) v1).addAll((List)v2);
            return true;
        }else if(v1 instanceof List){
            ((List<String>) v1).add((String) v2);
            return true;
        }else if(v2 instanceof List){
            ((List<String>) v2).add((String)v1);
            return true;
        }else{
            List<String> merge =  Arrays.asList((String)v1, (String) v2);
            return true;
        }

    }


}
