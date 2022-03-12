package com.hengdou.week2;


import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

/**
 * Created by hengdou on 2022/3/10.
 */
public class PhoneFlowBean implements Writable{
    private long upflow ;
    private long dwflow;
    private long sumFlow;
    private String phone;

    public PhoneFlowBean(){}

    public PhoneFlowBean(String phone, long upflow, long dwflow) {
        this.phone = phone;
        this.upflow = upflow;
        this.dwflow = dwflow;
        this.sumFlow=this.upflow + this.dwflow;
    }

    public long getUpflow() {
        return upflow;
    }

    public void setUpflow(long upflow) {
        this.upflow = upflow;
    }

    public long getDwflow() {
        return dwflow;
    }

    public void setDwflow(long dwflow) {
        this.dwflow = dwflow;
    }

    public long getSumFlow() {
        return sumFlow;
    }

    public void setSumFlow(long sumFlow) {
        this.sumFlow = sumFlow;
    }


    public String getPhone() {
        return phone;
    }

    public void setPhone(String phone) {
        this.phone = phone;
    }

    public void write(DataOutput dataOutput) throws IOException {
        dataOutput.writeUTF(this.phone);
        dataOutput.writeLong(this.upflow);
        dataOutput.writeLong(this.dwflow);
        dataOutput.writeLong(this.sumFlow);
    }

    public void readFields(DataInput dataInput) throws IOException {
        this.phone = dataInput.readUTF();
        this.upflow = dataInput.readLong();
        this.dwflow = dataInput.readLong();
        this.sumFlow = dataInput.readLong();

    }

    public String toString() {
        return upflow+","+dwflow+","+sumFlow;
    }
}
