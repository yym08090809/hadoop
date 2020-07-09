package day05.join;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class JoinDemo01 {
    //map
    //18 - 40 岁(age)的男性(sex)年轻人 最喜欢看的10部电影(movieId)

    public static class MapTask extends Mapper<LongWritable,Text,Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //1::F::1::1907::4
            String[] splits = value.toString().split("::");
            String sex = splits[1];
            int age = Integer.parseInt(splits[2]);
            String movieId = splits[3];
            if(age>=18 && age<=40 && sex.equals("M")){
                //符合条件的写出去
                context.write(new Text(movieId),new IntWritable(1));
            }
        }
    }

    public static class ReduceTask extends Reducer<Text,IntWritable,Text,NullPointerException>{
        //reduce
        //将movieId 对应的观看数存入map中
        HashMap<String,Integer> map = new HashMap<>();

        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            //1907 {1，1，1，1，1}
            int count = 0;
            for (IntWritable value : values) {
                count++;
            }
            map.put(key.toString(),count);
        }

        @Override
        protected void cleanup(Context context) throws IOException, InterruptedException {
            //把map中的所有数据存入list中
            Set<Map.Entry<String, Integer>> entries = map.entrySet();
            ArrayList<Map.Entry<String,Integer>> list = new ArrayList<>(entries);
            list.sort(new Comparator<Map.Entry<String, Integer>>() {
                @Override
                public int compare(Map.Entry<String, Integer> o1, Map.Entry<String, Integer> o2) {
                    return o2.getValue() - o1.getValue();
                }
            });
            for (int i = 0; i < 10; i++) {
                context.write(new Text(list.get(i).toString()),null);
            }
        }
    }

    //main
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //  job 提交内部类
        job.setMapperClass(MapTask.class);
        job.setReducerClass(ReduceTask.class);
        job.setJarByClass(JoinDemo01.class);
        //四个输入参数校验
        // map:Text IntWritable
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // reduce:Text IntWritable
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);


        //如果输出文件存在，就删除原文件
        String outputPath = "F:\\SGKJ\\大数据\\练习\\joinOut1";
        File file = new File(outputPath);
        if (file.exists()) {
            FileUtils.deleteDirectory(file);
        }

        //设置输入、输出路径
        String inputPath = "F:\\SGKJ\\大数据\\练习\\joinOut\\part-r-00000";
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        boolean b = job.waitForCompletion(true);
        System.out.println(b ? "正确" : "有问题");
    }
}
