package day04.commonFriend;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

public class Friend2 {
    public static class MapTask extends Mapper<LongWritable, Text,Text,Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            //A	IKCBGFHOD => I-K  A
            String[] splits = value.toString().toString().split("\t");
            String values = splits[0];
            String[] friends = splits[1].toString().split("");
            for (int i = 0; i < friends.length-1; i++) {
                for (int j = i+1; j < friends.length; j++) {
                    context.write(new Text(friends[i]+"-"+friends[j]),new Text(values));
                }
            }
        }
    }

    public static class ReduceTask extends Reducer<Text,Text,Text,Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            //I-K  A I-K  B => I-K A,B
            StringBuffer sb = new StringBuffer();
            boolean flag = true;
            for (Text value : values) {
                if(flag){
                    sb.append(value);
                    flag =false;
                }else {
                    sb.append(",").append(value);
                }
            }
            context.write(key,new Text(sb.toString()));

        }
    }

    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //  job 提交内部类
        job.setMapperClass(MapTask.class);
        job.setReducerClass(ReduceTask.class);
        job.setJarByClass(Friend2.class);
        //四个输入参数校验
        // map:Text IntWritable
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        // reduce:Text IntWritable
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);


        //如果输出文件存在，就删除原文件
        String outputPath = "E:\\Program Files\\JetBrains\\JAVA_Project\\hadoop\\src\\main\\resources\\friend2";
        File file = new File(outputPath);
        if (file.exists()) {
            FileUtils.deleteDirectory(file);
        }

        //设置输入、输出路径
        String inputPath = "E:\\Program Files\\JetBrains\\JAVA_Project\\hadoop\\src\\main\\resources\\friend\\part-r-00000";
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        //提示
        boolean b = job.waitForCompletion(true);
        System.out.println(b ? "正确" : "有问题");
    }
}
