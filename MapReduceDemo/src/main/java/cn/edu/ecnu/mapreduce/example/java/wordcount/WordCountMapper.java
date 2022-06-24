package cn.edu.ecnu.mapreduce.example.java.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class WordCountMapper extends Mapper<LongWritable,Text,Text,IntWritable>{
    @Override
    protected void map(LongWritable key, Text value, Context context)
        throws IOException, InterruptedException{
        String[] datas = value.toString().split(" ");
        for (String data : datas){
            context.write(new Text(data), new IntWritable(1));
        }
    }
}
