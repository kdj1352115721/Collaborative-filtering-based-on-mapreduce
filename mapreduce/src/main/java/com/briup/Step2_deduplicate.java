package com.briup;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;

public class Step2_deduplicate extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Step2_deduplicate(),args);
        //实际上是调用了三参的，最后返回实现了hadoop.util.Tool 中的run(String[]  toolArgs)
        //传入实现类tool（new Step2_deduplicate()）是tool.getconf()为了拿到conf，
        // 其中conf,args 是为了拿到解析器paser，解析器是为了能够拿到上面的参数 toolArgs以实现接口Tool中的run方法。
    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "dupjob");
        job.setJarByClass(this.getClass());

        job.setMapperClass(dupMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(IntWritable.class);

        job.setReducerClass(dupReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(NullWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path(conf.get("inpath")));

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path(conf.get("outpath")));

        job.waitForCompletion(true);
        return 0;
    }

    public static class dupMapper extends Mapper<LongWritable, Text,Text, IntWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split(",");
            context.write(new Text(split[0]),new IntWritable(1));
        }
    }
    public static class dupReducer extends Reducer<Text,IntWritable,Text, NullWritable>{
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            context.write(key,NullWritable.get());
        }
    }
}
