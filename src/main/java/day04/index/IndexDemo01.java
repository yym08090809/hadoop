package day04.index;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;

public class IndexDemo01 {

    //    map
    public static class MapTask extends Mapper<LongWritable, Text, Text, Text> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            FileSplit filesplit = (FileSplit) context.getInputSplit();
            String fileName = filesplit.getPath().getName();
            String[] split = value.toString().split(",");
            for (String s : split) {
//              输出数据格式： a.txt => hadoop 1
                context.write(new Text(fileName + "=>" + s), new Text("1"));
            }
        }
    }

    //    commbiner
    public static class CombinerTask extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            int count = 0;
            for (Text value : values) {
                count++;
            }
            String[] keySplit = key.toString().split("=>");
            String fileName = keySplit[0];
            String wordName = keySplit[1];
//          输出格式：hadoop a.txt=>200
            context.write(new Text(wordName), new Text(fileName+"=>"+count));
        }
    }

    //    reduce
    public static class ReduceTask extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            StringBuffer sb=new StringBuffer();
            boolean flag=true;
            for (Text value : values) {
                if(flag){
                    sb.append("{").append(value);
                    flag =false;
                }else{
                    sb.append(",").append(value);
                }
            }
            sb.append("}");
            context.write(key,new Text(sb.toString()));
        }
    }

    //    main
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

//        提交内部类
        job.setMapperClass(MapTask.class);
        job.setCombinerClass(CombinerTask.class);
        job.setReducerClass(ReduceTask.class);
        job.setJarByClass(IndexDemo02.class);

//        设置参数类型
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(Text.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

//        设置输入、输出路径
        String input = "F:\\SGKJ\\大数据\\练习\\index";
        String output = "F:\\SGKJ\\大数据\\练习\\indexout";

//        判断输出文件夹是否存在
        File file = new File(output);
        if (file.exists()) {
            FileUtils.deleteDirectory(file);
        }

//       提交
        FileInputFormat.setInputPaths(job, new Path(input));
        FileOutputFormat.setOutputPath(job, new Path(output));

//        提示
        boolean tip = job.waitForCompletion(true);
        System.out.println(tip ? "牛逼兄弟！" : "不太行啊，有bug！");
    }

}
