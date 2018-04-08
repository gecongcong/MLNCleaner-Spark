package util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;

/**
 * Created by zju on 18-2-20.
 */
public class Log {
    public String logFile = "log.txt";
    static final String baseURL = "/home/hadoop/log_cleanerSpark/";

    public Log(String logFile){
        this.logFile = logFile;
    }

    public void write(String str){
        String fileURL = baseURL+logFile;
        File file = new File(fileURL);
        FileWriter fw = null;
        BufferedWriter bw = null;
        try {
            if (file.exists()) {// 判断文件是否存在
//                System.out.println("文件已存在: " + cleanedFileURL);
            } else if (!file.getParentFile().exists()) {// 判断目标文件所在的目录是否存在
                // 如果目标文件所在的文件夹不存在，则创建父文件夹
                System.out.println("目标文件所在目录不存在，准备创建它！");
                if (!file.getParentFile().mkdirs()) {// 判断创建目录是否成功
                    System.out.println("创建目标文件所在的目录失败！");
                }
            } else {
                file.createNewFile();
            }
            fw = new FileWriter(file,true);
            bw = new BufferedWriter(fw);

            bw.write(str);
            bw.newLine();
            bw.flush();
            bw.close();
            fw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
