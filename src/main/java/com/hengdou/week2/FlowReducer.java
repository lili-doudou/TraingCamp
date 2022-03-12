package com.hengdou.week2;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

/**
 * Created by hengdou on 2022/3/11.
 */
public class FlowReducer extends Reducer<Text,PhoneFlowBean,Text,PhoneFlowBean>{
    protected void reduce(Text key, Iterable<PhoneFlowBean> values,
                          Context context) throws IOException,InterruptedException{
        // reduce中的业务逻辑就是遍历values，然后进行累加求和再输出
        PhoneFlowBean phoneFlow = new PhoneFlowBean();
        long up = phoneFlow.getUpflow();
        long down = phoneFlow.getDwflow();
        long sum = phoneFlow.getSumFlow();

        for (PhoneFlowBean flow : values) {
            up += flow.getUpflow();
            down += flow.getDwflow();
            sum += flow.getSumFlow();
        }
        phoneFlow.setSumFlow(sum);
        phoneFlow.setUpflow(up);
        phoneFlow.setDwflow(down);
        phoneFlow.setPhone(key.toString());

        context.write(key,phoneFlow);


    }

}
