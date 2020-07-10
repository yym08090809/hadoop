package day06;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.db.DBConfiguration;
import org.apache.hadoop.mapreduce.lib.db.DBOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

public class DBLocakTop {
    private static final String driverClass = "com.mysql.jdbc.Driver";
    private static final String dbUrl = "jdbc:mysql://127.0.0.1:3306/hadoop?useUnicode=true&amp;characterEncoding=uft8&amp;serverTimezone=GMT";
    private static final String userName ="root";
    private static final String passWord = "123456";
    private static final String tableName = "movies";
    private static final String[] fieldNames = {"movie","rate","ts","uid"};
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
        //Configuration conf, String driverClass,
        //      String dbUrl, String userName, String passwd
        //连接数据库
        DBConfiguration.configureDB(conf,driverClass,dbUrl,userName,passWord);

        Job job = Job.getInstance(conf);
        //  job 提交内部类
        job.setMapperClass(MapTask.class);
        job.setReducerClass(ReduceTask.class);
        job.setJarByClass(DBLocakTop.class);
        //四个输入参数校验
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(MovieBean.class);
        job.setOutputKeyClass(MovieBean.class);
        job.setOutputValueClass(NullWritable.class);


        //设置输入、输出路径
        String inputPath = "E:\\Program Files\\JetBrains\\JAVA_Project\\hadoop\\src\\main\\resources\\movie.json";
        FileInputFormat.addInputPath(job, new Path(inputPath));
        //输出到数据库
        //Job job, String tableName,
        //      String... fieldNames
        DBOutputFormat.setOutput(job,tableName,fieldNames);


        boolean b = job.waitForCompletion(true);
        System.out.println(b ? "正确" : "有问题");
    }
}
