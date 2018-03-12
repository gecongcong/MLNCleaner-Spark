package spark;


import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.Serializable;

/**
 * Created by gcc on 17-7-20.
 */
public class SparkConfig implements Serializable{
    public static String masterName = "spark://10.214.147.224:7077";
    public static String appName = "cleaner-spark";
    public static SparkConf conf = null;

    public static JavaSparkContext start(){
        // 1. 创建SparkConf配置信息
        conf = new SparkConf()
//                .setMaster("local[1]")
                .setAppName(appName);
        // 2. 创建SparkContext对象，在java编程中，该对象叫做JavaSparkContext
        JavaSparkContext sc = new JavaSparkContext(conf);
        return sc ;
    }

    public static void stop(JavaSparkContext sc){
        sc.stop();
    }
}
