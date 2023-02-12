package cn.ityege.hadoop.mapreduce.beans;

import org.apache.hadoop.io.Writable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CovidCountBean implements Writable {
    private long cases;//确诊病例数
    private long deaths;//死亡病例数

    public CovidCountBean() {
    }

    //自己封装对象的set方法 用于对象属性赋值
    public void set(long cases, long deaths) {
        this.cases = cases;
        this.deaths = deaths;
    }

    public long getCases() {
        return cases;
    }

    public void setCases(long cases) {
        this.cases = cases;
    }

    public long getDeaths() {
        return deaths;
    }

    public void setDeaths(long deaths) {
        this.deaths = deaths;
    }

    /**
     *  序列化方法
     */
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeLong(cases);
        out.writeLong(deaths);
    }

    /**
     * 反序列化方法 注意顺序
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        this.cases = in.readLong();
        this.deaths =in.readLong();
    }

    @Override
    public String toString() {
        return cases+"\t"+deaths;
    }
}
