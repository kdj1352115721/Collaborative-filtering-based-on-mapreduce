package com.briup;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class Step3_sort extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Step3_sort(),args);
    }

    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "sortJob");
        job.setJarByClass(this.getClass());

        job.setMapperClass(sortMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(sortReducer.class);
        job.setOutputKeyClass(IntWritable.class);
        job.setOutputValueClass(Text.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path(conf.get("inpath")));

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path(conf.get("outpath")));

        job.waitForCompletion(true);
        return 0;
    }

    public static class sortMapper extends Mapper<LongWritable, Text, Text,IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",");
            context.write(new Text(split[0]),new IntWritable(1));
        }
    }



    public static  class sortReducer extends Reducer<Text,IntWritable,IntWritable,Text>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            int index = 1;
            //按number个数排升序输出
         /*   for (IntWritable value : values) {
                context.write(new IntWritable(index++),key);
            }*/
         context.write(new IntWritable(index),key);
        }
    }
}
