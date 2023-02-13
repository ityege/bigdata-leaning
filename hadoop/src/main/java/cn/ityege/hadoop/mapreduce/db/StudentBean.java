package cn.ityege.hadoop.mapreduce.db;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.lib.db.DBWritable;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * CREATE TABLE `student` (
 * `sid` int NOT NULL,
 * `sname` varchar(255) DEFAULT NULL,
 * `ssex` int DEFAULT NULL,
 * `chinese` int DEFAULT NULL,
 * `math` int DEFAULT NULL,
 * `english` int DEFAULT NULL,
 * `score` double DEFAULT NULL,
 * PRIMARY KEY (`sid`)
 * ) ENGINE=InnoDB DEFAULT CHARSET=utf8;
 */
public class StudentBean implements Writable, DBWritable {
    private int sid;
    private String sname;
    private int ssex;
    private int chinese;
    private int math;
    private int english;
    private double score;


    public StudentBean() {
    }

    public StudentBean(int sid, String sname, int ssex, int chinese, int math, int english, double score) {
        this.sid = sid;
        this.sname = sname;
        this.ssex = ssex;
        this.chinese = chinese;
        this.math = math;
        this.english = english;
        this.score = score;
    }

    /**
     * set方法 用于对象赋值
     */
    public void set(int sid, String sname, int ssex, int chinese, int math, int english, double score) {
        this.sid = sid;
        this.sname = sname;
        this.ssex = ssex;
        this.chinese = chinese;
        this.math = math;
        this.english = english;
        this.score = score;
    }

    public int getSid() {
        return sid;
    }

    public void setSid(int sid) {
        this.sid = sid;
    }

    public String getSname() {
        return sname;
    }

    public void setSname(String sname) {
        this.sname = sname;
    }

    public int getSsex() {
        return ssex;
    }

    public void setSsex(int ssex) {
        this.ssex = ssex;
    }

    public int getChinese() {
        return chinese;
    }

    public void setChinese(int chinese) {
        this.chinese = chinese;
    }

    public int getMath() {
        return math;
    }

    public void setMath(int math) {
        this.math = math;
    }

    public int getEnglish() {
        return english;
    }

    public void setEnglish(int english) {
        this.english = english;
    }

    public double getScore() {
        return score;
    }

    public void setScore(double score) {
        this.score = score;
    }

    /**
     * 序列化方法
     */
    @Override
    public void write(DataOutput out) throws IOException {

        out.writeInt(sid);
        out.writeUTF(sname);
        out.writeInt(ssex);
        out.writeInt(chinese);
        out.writeInt(math);
        out.writeInt(english);
        out.writeDouble(score);
    }

    /**
     * 反序列化方法
     */
    @Override
    public void readFields(DataInput in) throws IOException {
        this.sid = in.readInt();
        this.sname = in.readUTF();
        this.ssex = in.readInt();
        this.chinese = in.readInt();
        this.math = in.readInt();
        this.english = in.readInt();
        this.score = in.readDouble();
    }

    /**
     * 在PreparedStatement中设置对象的字段。写数据库
     */
    @Override
    public void write(PreparedStatement ps) throws SQLException {

        ps.setInt(1, sid);
        ps.setString(2, sname);
        ps.setInt(3, ssex);
        ps.setInt(4, chinese);
        ps.setInt(5, math);
        ps.setInt(6, english);
        ps.setDouble(7, score);
    }

    /**
     * 从ResultSet中读取对象的字段。 读数据库
     */
    @Override
    public void readFields(ResultSet rs) throws SQLException {
        this.sid = rs.getInt(1);
        this.sname = rs.getString(2);
        this.ssex = rs.getInt(3);
        this.chinese = rs.getInt(4);
        this.math = rs.getInt(5);
        this.english = rs.getInt(6);
        this.score = rs.getDouble(7);
    }

    @Override
    public String toString() {
        return sid + "\t" + sname + "\t" + ssex + "\t" + chinese + "\t" + math + "\t" + english + "\t" + score;
    }
}
