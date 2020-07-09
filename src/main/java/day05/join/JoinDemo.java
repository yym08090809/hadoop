package day05.join;

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
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

public class JoinDemo {
    //map
    public static class MapTask extends Mapper<LongWritable, Text,Text,UserBean>{
        FileSplit fileSplit = null;
        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            fileSplit = (FileSplit)context.getInputSplit();
        }

        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String fileName = fileSplit.getPath().getName();
            //切分数据
            String[] splits = value.toString().split("::");
            UserBean userBean = new UserBean();
            //判断来自那个文件
            if(fileName.equals("users.dat")){
                //文件中的数据及对应的字段
                //1::F::1::10::48067
                //String userId String sex Integer age String movieId Integer rate String tableName
                userBean.set(splits[0],splits[1],Integer.parseInt(splits[2]),null+"",0,"users");
            }else {
                //1::1193::5::978300760
                //String userId String sex Integer age String movieId Integer rate String tableName
                userBean.set(splits[0],null+"",0,splits[1],Integer.parseInt(splits[2]),"ratings");
            }
            //uid，String userId String sex Integer age String movieId Integer rate String tableName
            context.write(new Text(userBean.getUserId()),userBean);
        }
    }

    //reduce
    public static class  ReduceTask extends Reducer<Text,UserBean,UserBean,NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<UserBean> values, Context context) throws IOException, InterruptedException {
            ArrayList<UserBean> list = new ArrayList<>();
            //拆分
                //user表的对象
            UserBean bean1 = new UserBean();
            for (UserBean value : values) {
                if(value.getTableName().equals("users")){
                    bean1.setSex(value.getSex());
                    bean1.setAge(value.getAge());
                }else {
                    //ratings表的对象
                    UserBean bean2 = new UserBean();
                    bean2.setUserId(value.getUserId());
                    bean2.setMovieId(value.getMovieId());
                    bean2.setRate(value.getRate());
                    list.add(bean2);
                }
            }

            //合并
            for (UserBean userBean : list) {
                userBean.setSex(bean1.getSex());
                userBean.setAge(bean1.getAge());
                context.write(userBean,null);
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
        job.setJarByClass(JoinDemo.class);
        //四个输入参数校验
        // map:Text IntWritable
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(UserBean.class);
        // reduce:Text IntWritable
        job.setOutputKeyClass(UserBean.class);
        job.setOutputValueClass(NullWritable.class);


        //如果输出文件存在，就删除原文件
        String outputPath = "F:\\SGKJ\\大数据\\练习\\joinOut";
        File file = new File(outputPath);
        if (file.exists()) {
            FileUtils.deleteDirectory(file);
        }

        //设置输入、输出路径
        String inputPath = "F:\\SGKJ\\大数据\\练习\\join";
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        boolean b = job.waitForCompletion(true);
        System.out.println(b ? "正确" : "有问题");
    }
}
