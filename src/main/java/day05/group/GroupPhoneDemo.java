package day05.group;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;

public class GroupPhoneDemo {
    //map
    public static class MapTask extends Mapper<LongWritable, Text, Text, PhoneBean> {
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] splits = value.toString().split("\t");
            String phoneNumber = splits[1];
            Integer upData = Integer.parseInt(splits[8]);
            Integer downData = Integer.parseInt(splits[9]);
            PhoneBean bean = new PhoneBean(phoneNumber, upData, downData);
            context.write(new Text(phoneNumber), bean);
        }
    }

    //Partitioner
    public static class PartitionerTask extends Partitioner<Text, PhoneBean> {
        static HashMap<String, Integer> map = null;

        static {
            map = new HashMap<>();
            map.put("13", 0);
            map.put("15", 1);
            map.put("18", 2);
        }

        @Override
        public int getPartition(Text text, PhoneBean bean, int i) {
            String phone = text.toString().substring(0, 2);
            Integer value = map.get(phone);
            return value != null ? value : 3;
        }
    }

    //reduce
    public static class ReduceTask extends Reducer<Text, PhoneBean, PhoneBean, NullWritable> {
        @Override
        protected void reduce(Text key, Iterable<PhoneBean> values, Context context) throws IOException, InterruptedException {
            int upDataToatl = 0;
            int downDataTotal = 0;
            PhoneBean bean = new PhoneBean();
            for (PhoneBean value : values) {
                upDataToatl += value.getUpData();
                downDataTotal += value.getDownData();
            }
            bean.setPhoneNumber(key.toString());
            bean.setUpData(upDataToatl);
            bean.setDownData(downDataTotal);
            context.write(bean, null);
        }
    }

    //main
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf);

        job.setMapperClass(MapTask.class);
        job.setReducerClass(ReduceTask.class);
        job.setJarByClass(GroupPhoneDemo.class);
        job.setPartitionerClass(PartitionerTask.class);
       job.setNumReduceTasks(4);
        //四个输入参数校验
        // map:Text IntWritable
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PhoneBean.class);
        // reduce:Text IntWritable
        job.setOutputKeyClass(PhoneBean.class);
        job.setOutputValueClass(NullWritable.class);

        String outputPath = "F:\\SGKJ\\大数据\\练习\\PhoneData";
        File file = new File(outputPath);
        if (file.exists()) {
            FileUtils.deleteDirectory(file);
        }

        //设置输入、输出路径
        String inputPath = "F:\\SGKJ\\大数据\\练习\\http.txt";
        FileInputFormat.setInputPaths(job, new Path(inputPath));
        FileOutputFormat.setOutputPath(job, new Path(outputPath));

        boolean b = job.waitForCompletion(true);
        System.out.println(b ? "正确" : "有问题");
    }
}
