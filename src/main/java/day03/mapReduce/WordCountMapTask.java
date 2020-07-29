package day03.mapReduce;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

/**
 * @author 李松柏
 * @createTime 2020/7/28 18:26
 * @description WordCount
 */
public class WordCountMapTask extends Mapper<LongWritable, Text,Text, IntWritable> {
    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String[] words = value.toString().split(",");
        for (String word : words) {
            context.write(new Text(word),new IntWritable(1));
        }
    }
}
