package util;

import data.DataFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.util.Progressable;

import java.io.*;
import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.URI;
import java.util.Enumeration;

/**
 * Created by gcc on 17-7-23.
 */
public class Util {
    public static void setLineID(String readURL, String writeURL) {
        // read file content from file
        FileReader reader = null;
        try {
            reader = new FileReader(readURL);
            BufferedReader br = new BufferedReader(reader);

            // write string to file

            FileWriter writer = new FileWriter(writeURL);
            BufferedWriter bw = new BufferedWriter(writer);

            String str = "";
            int index = 0;
            while ((str = br.readLine()) != null) {
                StringBuffer sb = new StringBuffer(str);
                if (index == 0) {
                    sb.insert(0, "ID,");
                    bw.write(sb.toString() + "\n");
                } else {
                    sb.insert(0, index + ",");
                    bw.write(sb.toString() + "\n");
                }
                index++;
                System.out.println(sb.toString());
            }
            br.close();
            reader.close();
            bw.close();
            writer.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void deleteFile() throws FileNotFoundException, IOException {
        String dst = "hdfs://master:9000/RDBSCleaner_DB/HAI/RDBSCleaner_cleaned.txt";
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dst), conf);
        fs.delete(new Path(dst), true);
    }

    /**
     * 上传文件到HDFS上去
     */
    public static void uploadToHdfs() throws FileNotFoundException, IOException {
        String localSrc = DataFormat.dataURL;
        String dst = "hdfs://10.214.147.224:9000/dataSet/syn-car/fulldb-3q-hasID-5%error.csv";
        InputStream in = new BufferedInputStream(new FileInputStream(localSrc));
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(dst), conf);
        OutputStream out = fs.create(new Path(dst), new Progressable() {
            public void progress() {
                System.out.print(".");
            }
        });
        IOUtils.copyBytes(in, out, 4096, true);
    }

    /**
     * 查询本机IP
     */
    public static String findLocalIP() throws Exception {
        Enumeration allNetInterfaces = NetworkInterface.getNetworkInterfaces();
        InetAddress ip = null;
        while (allNetInterfaces.hasMoreElements()) {
            NetworkInterface netInterface = (NetworkInterface) allNetInterfaces.nextElement();
            //System.out.println(netInterface.getName());
            Enumeration addresses = netInterface.getInetAddresses();
            while (addresses.hasMoreElements()) {
                ip = (InetAddress) addresses.nextElement();
                if (ip != null && ip instanceof Inet4Address) {
                    //System.out.println("local IP = " + ip.getHostAddress());
                    if (netInterface.getName().equals("eth0")) {
                        return ip.getHostAddress();
                    }
                }
            }
        }
        return "";
    }

    /**
     * 从文件读取本机IP
     */
    public static String getLocalIP(String url) throws Exception {
        String str = "";
        try {
            // read file content from file
            StringBuffer sb = new StringBuffer(url);

            FileReader reader = new FileReader(url);
            BufferedReader br = new BufferedReader(reader);

            if ((str = br.readLine()) != null) {
                //System.out.println(str);
            }
            br.close();
            reader.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return str;
    }

    public static void main(String[] args) throws Exception {
//        String readURL = "/home/gcc/IdeaProjects/Cleaner-Spark/dataSet/HAI/HAI-20.csv";
//        String writeURL = "/home/gcc/IdeaProjects/Cleaner-Spark/dataSet/HAI/HAI-20(hasID).csv";
//        setLineID(readURL,writeURL);
        uploadToHdfs();
        //System.out.println(findLocalIP());
        //deleteFile();
//        String url = "/home/gcc/IP/localIP.txt";
//        getLocalIP(url);
    }
}
