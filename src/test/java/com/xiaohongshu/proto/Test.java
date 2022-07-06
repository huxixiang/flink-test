package com.xiaohongshu.proto;

public class Test {
    @org.junit.Test
    public void test(){
        Long start  = 1654681502000l;
        Long end = 1654681503049l;
        System.out.println((long)(end-start));
    }

    @org.junit.Test
    public void interfaceTest(){
        InterfaceClass imp1 = new InterfaceImp1();
        imp1.init();
    }
}
