package day04.Alltop;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class AllTop {
    //map
    public static class MapTask extends Mapper<LongWritable, Text, Text, MovieBean> {
        ObjectMapper mapper = new ObjectMapper();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            MovieBean movieBean = mapper.readValue(value.toString(), MovieBean.class);
            context.write(new Text(), movieBean);
        }
    }

    //reduce
    public static class ReduceTask extends Reducer<Text, MovieBean, MovieBean, NullWritable> {
        ArrayList<MovieBean> movieBeans = new ArrayList<>();
        @Override
        protected void reduce(Text key, Iterable<MovieBean> values, Context context) throws IOException, InterruptedException {

            for (MovieBean value : values) {
                MovieBean movieBean = new MovieBean();
                movieBean.set(value);
                movieBeans.add(movieBean);
            }
            Collections.sort(movieBeans, new Comparator<MovieBean>() {
                @Override
                public int compare(MovieBean o1, MovieBean o2) {
                    return o2.getRate() - o1.getRate();
                }
            });
            for (int i = 0; i < 10; i++) {
                context.write(movieBeans.get(i), null);
            }

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
        job.setJarByClass(AllTop.class);
        //四个输入参数校验
        // map:Text IntWritable
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MovieBean.class);
        // reduce:Text IntWritable
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MovieBean.class);


        //如果输出文件存在，就删除原文件
        String outputPath = "E:\\Program Files\\JetBrains\\JAVA_Project\\hadoop\\src\\main\\resources\\movieAllTop10";
        File file = new File(outputPath);
        if (file.exists()) {
            FileUtils.deleteDirectory(file);
        }

        //设置输入、输出路径
        String inputPath = "E:\\Program Files\\JetBrains\\JAVA_Project\\hadoop\\src\\main\\resources\\movie.json";
        FileInputFormat.addInputPath(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        boolean b = job.waitForCompletion(true);
        System.out.println(b ? "正确" : "有问题");
    }
}
