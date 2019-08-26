package com.briup;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
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

public class step4_studentavg extends Configured implements Tool {
    public static void main(String[] args) throws Exception {
        ToolRunner.run(new step4_studentavg(),args);
    }
    @Override
    public int run(String[] strings) throws Exception {
        Configuration conf = getConf();
        Job job = Job.getInstance(conf, "stuavgJob");
        job.setJarByClass(this.getClass());

        job.setMapperClass(studentavgMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(DoubleWritable.class);

        job.setReducerClass(studentavgReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(DoubleWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        TextInputFormat.addInputPath(job,new Path(conf.get("inpath")));

        job.setOutputFormatClass(TextOutputFormat.class);
        TextOutputFormat.setOutputPath(job,new Path(conf.get("outpath")));

        job.waitForCompletion(true);
        return 0;
    }
    public static class studentavgMapper extends Mapper<LongWritable, Text,Text, DoubleWritable>{
        @Override
        protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
            String[] split = value.toString().split("\t");
                context.write(new Text(split[0]),new DoubleWritable(Double.valueOf(split[1])));
        }
    }
    public static class studentavgReducer extends Reducer<Text,DoubleWritable,Text,DoubleWritable>{
        @Override
        protected void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
            int num=0;
            double sum =0.0;
            for (DoubleWritable value : values) {
                num++;
                sum+=value.get();
            }
            double avg = sum /num;
            context.write(key,new DoubleWritable(avg));
        }
    }
}
