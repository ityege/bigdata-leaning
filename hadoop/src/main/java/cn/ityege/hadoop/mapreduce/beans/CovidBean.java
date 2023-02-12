package cn.ityege.hadoop.mapreduce.beans;

import org.apache.hadoop.io.WritableComparable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

public class CovidBean implements WritableComparable<CovidBean> {
    private String state;//州
    private String county;//县
    private long cases;//确诊病例数

    public CovidBean() {
    }
    public void set(String state, String county, long cases) {
        this.state = state;
        this.county = county;
        this.cases = cases;
    }
    //序列化方法
    @Override
    public void write(DataOutput out) throws IOException {
        out.writeUTF(state);
        out.writeUTF(county);
        out.writeLong(cases);
    }

    public String getState() {
        return state;
    }

    public void setState(String state) {
        this.state = state;
    }

    public String getCounty() {
        return county;
    }

    public void setCounty(String county) {
        this.county = county;
    }

    public long getCases() {
        return cases;
    }

    public void setCases(long cases) {
        this.cases = cases;
    }

    //反序列化方法 注意顺序
    @Override
    public void readFields(DataInput in) throws IOException {
        this.state = in.readUTF();
        this.county = in.readUTF();
        this.cases = in.readLong();
    }


    @Override
    public String toString() {
        return "CovidBean{" +
                "state='" + state + '\'' +
                ", county='" + county + '\'' +
                ", cases=" + cases +
                '}';
    }
    /**
     * todo 排序方法 规则：首先根据州进行排序，字典序；如果州一样，根据病例数的倒序排序
     * @param o
     * @return
     */
    @Override
    public int compareTo(CovidBean o) {

        int result;

        //首先比较州
        int i = state.compareTo(o.getState());

        if (i > 0){
            result=1;
        }else if(i < 0){
            result =-1;
        }else{
            //如果进入到这里 意味着两个州一样 此时根据病例数的倒序进行排序
            result = cases > o.getCases() ? -1:(cases < o.getCases() ? 1:0);
        }

        return result;
    }

}
