package day03.io;

import java.io.*;
import java.util.*;

public class WordCount {
    public static void main(String[] args) throws IOException {
        //1.定义输入 输出流
        String path = "E:\\Program Files\\JetBrains\\JAVA_Project\\hadoop\\src\\main\\resources\\wc.txt";
        String out = "E:\\Program Files\\JetBrains\\JAVA_Project\\hadoop\\src\\main\\resources\\out.txt";
        BufferedReader br = new BufferedReader(new InputStreamReader(new FileInputStream(path), "utf-8"));
        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(out), "utf-8"));


        String s = null;
        HashMap<String, Long> map = new HashMap<>();
        //2.读入数据，并进行操作
        while ((s = br.readLine()) != null) {
            String[] words = s.split(",");
            //将每个单词出现的次数统计到map集合中
            //HashMap的主干是一个Entry数组。Entry是HashMap的基本组成单元，
            // 每一个Entry包含一个key-value键值对。（其实所谓Map其实就是保存了两个对象之间的映射关系的一种集合）
            //链表则是主要为了解决哈希冲突而存在的，如果定位到的数组位置不含链表（当前entry的next指向null）,那么查找，
            // 添加等操作很快，仅需一次寻址即可；如果定位到的数组包含链表，对于添加操作，其时间复杂度为O(n)，首先遍历链表，
            // 存在即覆盖，否则新增；对于查找操作来讲，仍需遍历链表，然后通过key对象的equals方法逐一比对查找。所以，性能考虑，
            // HashMap中的链表出现越少，性能才会越好。
            for (String word : words) {
//                if(map.containsKey(word)){
//                    map.put(word,map.get(word)+1);
//                }else {
//                    map.put(word,1L);
//                }
                Long count = map.getOrDefault(word, 0L);
                count++;
                map.put(word,count);
            }
        }


        Set<Map.Entry<String, Long>> entries = map.entrySet();
        ArrayList<Map.Entry<String, Long>> list = new ArrayList<>(entries);

        //排序
        Collections.sort(list, new Comparator<Map.Entry<String, Long>>() {
            @Override
            public int compare(Map.Entry<String, Long> o1, Map.Entry<String, Long> o2) {
                return (int)(o2.getValue() - o1.getValue());
            }
        });

        //3.写入数据1
//        for (Map.Entry<String, Long> entry : map.entrySet()) {
//            System.out.println(entry.getKey()+":"+entry.getValue());
//            bw.write(entry.getKey()+":"+entry.getValue()+"\r\n");
//            bw.flush();
//        }
        //3.写入数据2

        for (int i = 0; i < 3; i++) {
            System.out.println(list.get(i));
            bw.write(String.valueOf(list.get(i))+"\r\n");
            bw.flush();
        }
        //4.关流
        br.close();
        bw.close();

    }
}
