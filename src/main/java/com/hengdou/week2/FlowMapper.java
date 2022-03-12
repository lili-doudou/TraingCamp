package com.hengdou.week2;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapreduce.Mapper;
import sun.security.util.Length;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by hegndou on 2022/3/11.
 */
public class FlowMapper extends Mapper<LongWritable,Text,Text,PhoneFlowBean>{
        /**
         * 每一行执行一次map函数
         * @param key 表示字节在源文件中偏移量
         * @param value 行文本内容
         */
    protected  void map(LongWritable key, Text value,Context context) throws IOException,InterruptedException {
        final String[] splited = value.toString().split("\t");

        String phone = splited[1];
        long up_flow = Long.parseLong(splited[(splited.length)-3]);
        long dw_flow = Long.parseLong(splited[(splited.length)-2]);

        //封装数据为kv并输出        <phone:flow>
        context.write(new Text(phone),new PhoneFlowBean(phone,up_flow,dw_flow));

        }

    }
