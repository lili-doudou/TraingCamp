package com.hengdou.week2;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.mapreduce.lib.input.*;


/**
 * Created by hengdou on 2022/3/11.
 */
public class FlowDriver extends Configured implements Tool{

    public int run(String[] args) throws Exception {


        Configuration conf = new Configuration();

        Job job = Job.getInstance(conf);

        job.setJarByClass(FlowDriver.class);

        job.setMapperClass(FlowMapper.class);
        job.setReducerClass(FlowReducer.class);

        //设置map程序的输出key、value
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(PhoneFlowBean.class);

        //设置   输出 key、value
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(PhoneFlowBean.class);

        //输入数据路径
        FixedLengthInputFormat.setInputPaths(job, new Path(args[0]));
        //输出数据路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        return job.waitForCompletion(true)?0:1;

    }

    public static void main(String[] args) throws Exception {
        int  status = ToolRunner.run(new Configuration(), new FlowDriver(), args);
        System.exit(status);
    }

}
