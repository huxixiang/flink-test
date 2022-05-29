package com.xiaohongshu.kafka;


public class KafkaUtils {


    /**
     * hash 函数
     * @param data
     * @return
     */
    public static int murmur2(byte[] data) {
        int length = data.length;
        int seed = -1756908916;
        int h = seed ^ length;
        int length4 = length / 4;

        for(int i = 0; i < length4; ++i) {
            int i4 = i * 4;
            int k = (data[i4 + 0] & 255) + ((data[i4 + 1] & 255) << 8) + ((data[i4 + 2] & 255) << 16) + ((data[i4 + 3] & 255) << 24);
            k *= 1540483477;
            k ^= k >>> 24;
            k *= 1540483477;
            h *= 1540483477;
            h ^= k;
        }

        switch(length % 4) {
            case 3:
                h ^= (data[(length & -4) + 2] & 255) << 16;
            case 2:
                h ^= (data[(length & -4) + 1] & 255) << 8;
            case 1:
                h ^= data[length & -4] & 255;
                h *= 1540483477;
            default:
                h ^= h >>> 13;
                h *= 1540483477;
                h ^= h >>> 15;
                return h;
        }
    }


}
