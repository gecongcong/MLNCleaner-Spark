package main;

import data.DataFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import spark.SparkConfig;

import java.net.URI;

import static data.DataFormat.baseURL;

/**
 * Created by gcc on 18-3-12.
 */
public class SparkTest {
    static final Logger log = LogManager.getLogger(Main.class);

    public static void main(String[] args) {
        try {
//            System.out.println("baseURL=" + baseURL);
            String outfile1 = DataFormat.baseHDFS + "dataSet/" + args[0] + "/" + "sparktest";
            JavaSparkContext sc = SparkConfig.start();
            log.info(DataFormat.baseHDFS + "dataSet/" + args[0] + "/" + args[1]);
            JavaRDD s_trainRDD = sc.textFile(DataFormat.baseHDFS + "dataSet/" + args[0] + "/" + args[1]);//导入待清洗的数据集

            log.info( s_trainRDD.count());
            Configuration conf = new Configuration();
            FileSystem fs1 = FileSystem.get(URI.create(outfile1), conf);
            Path path1 = new Path(outfile1);
            if (fs1.exists(path1)) {
                fs1.delete(path1, true);
            }
            s_trainRDD.saveAsTextFile(outfile1);
        }catch (Exception e){
            e.printStackTrace();
        }
    }
}
