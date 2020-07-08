package day04.topn;

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

public class LocakTop {
    //map  输入 {"movie":"1193","rate":"5","timeStamp":"978300760","uid":"1"}   输出  （1193，{"movie":"1193","rate":"5","timeStamp":"978300760","uid":"1"}）
    public static class MapTask extends Mapper<LongWritable, Text, Text, MovieBean> {
        ObjectMapper mapper = new ObjectMapper();

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            MovieBean movieBean = mapper.readValue(value.toString(), MovieBean.class);
            context.write(new Text(movieBean.getMovie()), movieBean);
        }
    }

    //reduce
    public static class ReduceTask extends Reducer<Text, MovieBean, MovieBean, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<MovieBean> values, Context context) throws IOException, InterruptedException {
            ArrayList<MovieBean> movieBeans = new ArrayList<>();
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
            for (int i = 0; i < 5; i++) {
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
        job.setJarByClass(LocakTop.class);
        //四个输入参数校验
        // map:Text IntWritable
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MovieBean.class);
        // reduce:Text IntWritable
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(MovieBean.class);


        //如果输出文件存在，就删除原文件
        String outputPath = "E:\\Program Files\\JetBrains\\JAVA_Project\\hadoop\\src\\main\\resources\\movieLocalTop5";
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
