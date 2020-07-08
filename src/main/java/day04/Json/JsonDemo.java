package day04.Json;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.IOException;

public class JsonDemo {
    //map
    public static class MapTask extends Mapper<LongWritable, Text, Text, IntWritable> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

            ObjectMapper mapper = new ObjectMapper();
            MovieBean bean = mapper.readValue(value.toString(), MovieBean.class);
            String movie = bean.getMovie();
            Integer rate = bean.getRate();
            context.write(new Text(movie), new IntWritable(rate));
        }
    }

    //reduce
    public static class ReduceTask extends Reducer<Text, IntWritable, Text, DoubleWritable> {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            double sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
                //sum += (double) Integer.valueOf(value.toString()).intValue();
                count++;
            }
            context.write(key,new DoubleWritable(1.0f*sum/count));
        }
    }

    //main
    public static void main(String[] args) throws InterruptedException, IOException, ClassNotFoundException {
        // job
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);
        //  job 提交内部类
        job.setMapperClass(MapTask.class);
        job.setReducerClass(ReduceTask.class);
        job.setJarByClass(JsonDemo.class);
        //四个输入参数校验
        // map:Text IntWritable
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);
        // reduce:Text IntWritable
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);


        //如果输出文件存在，就删除原文件
        String outputPath = "E:\\Program Files\\JetBrains\\JAVA_Project\\hadoop\\src\\main\\resources\\movie";
        File file = new File(outputPath);
        if (file.exists()) {
            FileUtils.deleteDirectory(file);
        }

        //设置输入、输出路径
        String inputPath = "E:\\Program Files\\JetBrains\\JAVA_Project\\hadoop\\src\\main\\resources\\movie.json";
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        //提示
        boolean b = job.waitForCompletion(true);
        System.out.println(b ? "正确" : "有问题");
    }
}
