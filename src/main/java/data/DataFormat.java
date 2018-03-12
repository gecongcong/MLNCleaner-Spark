package data;

/**
 * Created by gcc on 17-7-22.
 */
public class DataFormat {
    public static String baseURL = "/home/gcc/experiment/dataSet"; //Project BaseURL
    public static String rulesFirstOrder = "dataSet/syn-car/rules-first-order.txt";
    public static String rulesURL = baseURL + "/syn-car/rules.txt";
    //public static String dataURL = baseURL + "dataSet/synthetic-car/3q/fulldb-3q-hasID-5%error.csv";
    public static String dataURL ="dataSet/syn-car/3q/fulldb-3q-hasID-5%error.csv";
    public static String evidence_outFile = baseURL + "dataSet/syn-car/evidence.db";
    public static String splitString = ",";
    public static String baseHDFS = "hdfs://10.214.147.224:9000/";
    //public static String cleanedFileURL = baseURL+ "/dataSet/HAI/RDBSCleaner_cleaned.txt";//存放清洗后的数据集
    public static String cleanedPath = "RDBSCleaner_cleaned";
    public static String groundFile = "ground_truth-hasID.csv";
    public static boolean ifHeader = true;
}
