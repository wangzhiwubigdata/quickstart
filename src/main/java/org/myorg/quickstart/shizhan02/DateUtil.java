package org.myorg.quickstart.shizhan02;

import java.text.SimpleDateFormat;
import java.util.Date;

public class DateUtil {


    public static String timeStampToDate(Long timestamp){

        ThreadLocal<SimpleDateFormat> threadLocal
                = ThreadLocal.withInitial(() -> new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
        String format = threadLocal.get().format(new Date(timestamp));
        return format.substring(0,10);
    }

    public static void main(String[] args) {
        System.out.println(timeStampToDate(System.currentTimeMillis()));
    }
}
