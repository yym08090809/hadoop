package day03.mapReduce;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

public class WordCount3 {
    //map 静态 内部类 输入（Long,String） 输出（String,int） 对输入文本进行分割，输出。
    public static class MapTask extends Mapper<LongWritable, Text,Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] words = value.toString().split(",");
            for (String word : words) {
                context.write(new Text(word),new IntWritable(1));
            }
        }
    }
    //reduce 静态 内部类  输入（hadoop，1） 输出（hadoop，10086）
    public static class ReduceTask extends Reducer<Text,IntWritable,Text,IntWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (IntWritable value : values) {
                count++;
            }
            context.write(key,new IntWritable(count));
        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        // job
        System.setProperty("HADOOP_USER_NAME","root");
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS","hdfs://192.168.88.110:9000");
        Job job = Job.getInstance(conf);
        //  job 提交内部类
        job.setMapperClass(WordCount2.MapTask.class);
        job.setReducerClass(WordCount2.ReduceTask.class);
        job.setJarByClass(WordCount2.class);
        //四个输入参数校验
        // map:Text IntWritable
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // reduce:Text IntWritable
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);



        //如果输出文件存在，就删除原文件
        String outputPath = "/bigdata/output/wordcount1";
        File file = new File(outputPath);
        if(file.exists()){

        }

        //设置输入、输出路径
        String inputPath = "/bigdata/input/wc.txt";
        FileInputFormat.addInputPath(job,new Path(inputPath));
        FileOutputFormat.setOutputPath(job,new Path(outputPath));

        //提示
        boolean b = job.waitForCompletion(true);
        System.out.println(b?"正确":"有问题");
    }
}
