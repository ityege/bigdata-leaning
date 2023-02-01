package example.stormsql;

import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * 整了半天报了一个错不知道为什么
 *Exception in thread "main" java.lang.RuntimeException: Failed to find data source for STUDENT URI: kafka://test?bootstrap-servers=bigdata1:9092,bigdata2:9092,bigdata3:9092
 */
public class GetTime {
    public static String evaluate(Long date, String dateFormat) {
        SimpleDateFormat simpleDateFormat1 = new SimpleDateFormat(dateFormat);
        String format = simpleDateFormat1.format(date);
        return format;
    }

    public static void main(String[] args) {
        System.out.println(evaluate(new Date().getTime(),"yyyy年MM月dd日 HH:mm:ss"));;
    }
}
