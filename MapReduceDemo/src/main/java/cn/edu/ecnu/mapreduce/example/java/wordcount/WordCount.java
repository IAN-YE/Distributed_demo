package cn.edu.ecnu.mapreduce.example.java.wordcount;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;

public class WordCount extends Configured implements Tool{
    @Override
    public int run(String[] args) throws Exception{
        Job job = Job.getInstance(getConf(),getClass().getSimpleName());
        job.setJarByClass(getClass());

        FileInputFormat.addInputPath(job,new Path(args[0]));
        FileOutputFormat.setOutputPath(job,new Path(args[1]));

        job.setMapperClass(WordCountMapper.class);
        job.setReducerClass(WordCountReducer.class);

        // job.setCombinerClass(WordCountCombiner.class);

        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main (String[] args) throws Exception{
        System.out.println(args[0]);
        int exitCode = ToolRunner.run(new WordCount(), args);
        System.exit(exitCode);
    }
}
