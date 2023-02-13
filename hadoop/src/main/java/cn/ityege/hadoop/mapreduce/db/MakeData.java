package cn.ityege.hadoop.mapreduce.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.util.Random;

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
public class MakeData {
    public static void main(String[] args) throws Exception {
        Random random = new Random();
        //注册驱动
        Class.forName("com.mysql.cj.jdbc.Driver");//MySQL5以后可直接省略
        //获取数据库连接
        Connection con = DriverManager.getConnection("jdbc:mysql://localhost:3306/test?useSSL=false&useUnicode=true&characterEncoding=UTF8&serverTimezone=GMT&allowPublicKeyRetrieval=true", "root", "123456");


        String sql = "insert into test.student(sid, sname,ssex,chinese,math,english,score) VALUES (?,?,?,?,?,?,?)";
        PreparedStatement ps = con.prepareStatement(sql);
        try {

            for (int i = 1; i <= 1_0000; i++) {
                ps.setInt(1, i);
                ps.setString(2, "张三" + i);
                ps.setObject(3, random.nextInt(2));
                ps.setObject(4, random.nextInt(100));
                ps.setObject(5, random.nextInt(100));
                ps.setObject(6, random.nextInt(100));
                ps.setObject(7, Math.random()*100);
                ps.addBatch();
            }
            ps.executeBatch();//将容器中的sql语句提交
            ps.clearBatch();//清空容器

        } catch (Exception e) {
        } finally {
            con.close();
        }
    }
}
