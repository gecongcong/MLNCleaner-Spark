package main;

import data.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.util.hash.Hash;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple1;
import scala.Tuple2;
import spark.SparkConfig;
import org.apache.spark.api.java.JavaRDD;
import tuffy.main.MLNmain;
import util.Log;
import util.TokenUtil;

import java.io.*;
import java.net.URI;
import java.sql.Array;
import java.sql.SQLException;
import java.text.DecimalFormat;
import java.util.*;

import static data.DataFormat.baseURL;


/**
 * Created by hadoop on 17-7-20.
 */
public class Main implements Serializable {
    static final Logger log = LogManager.getLogger(Main.class);

    //根据sample的tupleID,从ground truth File中提取对应的数据，用于evaluate()
    public static ArrayList<String> pickData(String cleanedURL, String groundURL) {

        ArrayList<String> ground_data = readHDFS(groundURL);
        ArrayList<String> dataSet = readHDFS(cleanedURL);
        ArrayList<String> sample_ground_data = new ArrayList<>(dataSet.size());
        for (int i = 0; i < dataSet.size(); i++) {
            String line = dataSet.get(i);
            String tupleID = line.substring(0, line.indexOf(","));
            sample_ground_data.add(ground_data.get(Integer.parseInt(tupleID) - 1));
        }
        return sample_ground_data;
    }

    public static ArrayList<String> pickData(ArrayList<String> dataSet, String groundURL) {

        ArrayList<String> ground_data = readHDFS(groundURL);
        ArrayList<String> sample_ground_data = new ArrayList<>(dataSet.size());
        for (int i = 0; i < dataSet.size(); i++) {
            String line = dataSet.get(i);
            String tupleID = line.substring(0, line.indexOf(","));
            sample_ground_data.add(ground_data.get(Integer.parseInt(tupleID) - 1));
        }
        return sample_ground_data;
    }

    public static ArrayList<String> read(String URL) {
        ArrayList<String> context = new ArrayList<String>();

        FileReader reader;
        try {
            reader = new FileReader(URL);
            BufferedReader br = new BufferedReader(reader);
            String line = null;
            String current = "";
            int key = 0; //tuple index
            br.readLine();
            while ((line = br.readLine()) != null && line.length() != 0) {
                context.add(line.replaceAll(" ", ""));
            }
            br.close();

        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return context;
    }

    public static ArrayList<String> readHDFS(String URL) {
        ArrayList<String> context = new ArrayList<>();
        try {
            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(URI.create(URL), conf);

            FSDataInputStream is = fs.open(new Path(URL));
            BufferedReader br = new BufferedReader(new InputStreamReader(is));
            String line;
            br.readLine();
            while ((line = br.readLine()) != null && line.length() != 0) {
                context.add(line); //.replaceAll(" ", "")
            }
            br.close();
            is.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return context;
    }

    /**
     * Recall is the ratio of correctly updated attributes to the total number of errors.
     * Precision is the ratio of correctly updated attributes (exact matches) to the total number of updates
     */
    public static void evaluate(ArrayList<String> ground_data, ArrayList<String> cleaned_data, String dirtyURL, Log localLog) {
        ArrayList<String> dirty_data = readHDFS(dirtyURL);
        int correct_update_num = 0;
        int total_error_num = 0;
        int total_update_num = 0;
        int over_correct_num = 0;
        double recall;
        double precision;

//        System.err.print("no cleaned Tuple: \n[");
        for (int i = 0; i < ground_data.size(); i++) {
            String current_ground = ground_data.get(i);
            String current_dirty = dirty_data.get(i);
            String current_clean = cleaned_data.get(i);
            if (!current_ground.equals(current_dirty)) {
                total_error_num++;
                /*if (!current_clean.equals(current_ground)) {
                    System.err.print(current_clean.substring(0, current_clean.indexOf(",")) + " ");  //no cleaned tuple:
                    System.out.println("current_ground = " + current_ground);
                    System.out.println("current_dirty = " + current_dirty);
                    System.out.println("current_clean = " + current_clean);

                }*/
            }
            if (!current_clean.equals(current_dirty)) {
                total_update_num++;
                if (current_clean.equals(current_ground)) {
                    correct_update_num++;
                } else {
                    over_correct_num++;
//                    System.err.println("over correct id = " + (i + 1));
                }
            }
        }
//        System.err.println("]");
//        System.err.println("over correct number = " + over_correct_num);
        System.out.println("\ntotal error number = " + total_error_num);
        localLog.write("\ntotal error number = " + total_error_num);
        recall = (double) correct_update_num / total_error_num;
        precision = (double) correct_update_num / total_update_num;
        System.out.println("\nRecall = " + recall);
        localLog.write("\nRecall = " + recall);
        System.out.println("\nPrecision = " + precision);
        localLog.write("\nPrecision = " + precision);
        System.out.println("\nF1 = " + 2 * (precision * recall) / (precision + recall));
        localLog.write("\nF1 = " + 2 * (precision * recall) / (precision + recall));
    }

    public static void evaluate(ArrayList<String> ground_data, ArrayList<String> cleaned_data, ArrayList<String> dirty_data) {
        int correct_update_num = 0;
        int total_error_num = 0;
        int total_update_num = 0;
        int over_correct_num = 0;
        double recall;
        double precision;

//        System.err.print("no cleaned Tuple: \n[");
        for (int i = 0; i < ground_data.size(); i++) {
            String current_ground = ground_data.get(i);
            String current_dirty = dirty_data.get(i);
            String current_clean = cleaned_data.get(i);
            if (!current_ground.equals(current_dirty)) {
                total_error_num++;
                /*if (!current_clean.equals(current_ground)) {
                    System.err.print(current_clean.substring(0, current_clean.indexOf(",")) + " ");  //no cleaned tuple:
                    System.out.println("current_ground = " + current_ground);
                    System.out.println("current_dirty = " + current_dirty);
                    System.out.println("current_clean = " + current_clean);

                }*/
            }
            if (!current_clean.equals(current_dirty)) {
                total_update_num++;
                if (current_clean.equals(current_ground)) {
                    correct_update_num++;
                } else {
                    over_correct_num++;
//                    System.err.println("over correct id = " + (i + 1));
                }
            }
        }
//        System.err.println("]");
//        System.err.println("over correct number = " + over_correct_num);
        System.out.println("\ntotal error number = " + total_error_num);
        recall = (double) correct_update_num / total_error_num;
        precision = (double) correct_update_num / total_update_num;
        System.out.println("\nRecall = " + recall);
        System.out.println("\nPrecision = " + precision);
        System.out.println("\nF1 = " + 2 * (precision * recall) / (precision + recall));
    }

    public static void evaluate(ArrayList<String> ground_data, String cleanedURL, String dirtyURL) {
        ArrayList<String> cleaned_data = readHDFS(cleanedURL);
        ArrayList<String> dirty_data = readHDFS(dirtyURL);
        int correct_update_num = 0;
        int total_error_num = 0;
        int total_update_num = 0;
        int over_correct_num = 0;
        double recall;
        double precision;

        System.err.print("no cleaned Tuple: \n[");
        for (int i = 0; i < ground_data.size(); i++) {
            String current_ground = ground_data.get(i);
            String current_dirty = dirty_data.get(i);
            String current_clean = cleaned_data.get(i);
            if (!current_ground.equals(current_dirty)) {
                total_error_num++;
                if (!current_clean.equals(current_ground)) {
                    System.err.print(current_clean.substring(0, current_clean.indexOf(",")) + " ");  //no cleaned tuple:
//                    System.out.println("current_ground = " + current_ground);
//                    System.out.println("current_dirty = " + current_dirty);
//                    System.out.println("current_clean = " + current_clean);

                }
            }
            if (!current_clean.equals(current_dirty)) {
                total_update_num++;
                if (current_clean.equals(current_ground)) {
                    correct_update_num++;
                } else {
                    over_correct_num++;
//                    System.err.println("over correct id = " + (i + 1));
                }
            }
        }
        System.err.println("]");
//        System.err.println("over correct number = " + over_correct_num);
        System.out.println("\ntotal error number = " + total_error_num);
        recall = (double) correct_update_num / total_error_num;
        precision = (double) correct_update_num / total_update_num;
        System.out.println("\nRecall = " + recall);
        System.out.println("\nPrecision = " + precision);
        System.out.println("\nF1 = " + 2 * (precision * recall) / (precision + recall));
    }

    public static HashMap<String, String> readMLNFile(String mlnFile) {

        HashMap<String, String> result = new HashMap<String, String>();
        try {
            FileReader reader = new FileReader(mlnFile);
            BufferedReader br = new BufferedReader(reader);
            String line = null;
            //escape predicate
//            while((line = br.readLine()) != null) {
//                if(line.length()==0)break;
//            }
            while ((line = br.readLine()) != null && line.length() != 0) {
                String rule_noWeight = line.substring(line.indexOf(",") + 1).trim();
                String weight = line.substring(0, line.indexOf(","));
                result.put(rule_noWeight, weight);
            }
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public static ArrayList<data.Clause> learnwt(ArrayList<String> data_parts, String[] header,
                                                 String[] args, ArrayList<Integer> newHeaderList) throws SQLException, IOException {
        Rule rule = new Rule();
        //Domain domain = new Domain();
//        String evidence_outFile = baseURL + "/" + args[0] + "/evidence.db";

        String cleanedFileURL = baseURL + "/" + args[0] + "RDBSCleaner_cleaned.txt";//存放清洗后的数据集
        String splitString = ",";

        boolean ifHeader = true;
        ArrayList<Tuple> tupleList = rule.initData(data_parts, splitString);//生成TupleList 供formatEvidence()使用

        //调用MLN相关的命令参数
        ArrayList<String> list = new ArrayList<>();
        String marginal_args = "-marginal";
        //list.add(marginal_args);
        String learnwt_args = "-learnwt";
        list.add(learnwt_args);
        String nopart_args = "-nopart";
        //list.add(nopart_args);
        String mln_args = "-i";
        list.add(mln_args);
//        String mlnFileURL = baseURL + "/HAI/prog-new.mln";//prog.mln
        String mlnFileURL = args[2];
        // 根据输入的参数来跑
        list.add(mlnFileURL);
        String evidence_args = "-e";
        list.add(evidence_args);
        String evidenceFileURL = args[4];
        list.add(evidenceFileURL);
        String queryFile_args = "-queryFile";
        list.add(queryFile_args);
        String queryFileURL = baseURL + "/" + args[0] + "/query.db";
        System.out.println("queryFileURL = " + queryFileURL);
//        String queryFileURL = DataFormat.baseHDFS + "dataSet/" + args[0] + "/query.db";
        list.add(queryFileURL);
        String outFile_args = "-r";
        list.add(outFile_args);
//        String weightFileURL = baseURL + "/HAI/out.txt";
        String weightFileURL = args[3];
        list.add(weightFileURL);
        String noDropDB = "-keepData";
        list.add(noDropDB);
        String maxIter_args = "-dMaxIter";
        list.add(maxIter_args);
        String maxIter = "15";
        list.add(maxIter);
        String mcsatSamples_args = "-mcsatSamples";
        //list.add(mcsatSamples_args);
        String mcsatSamples = "100";
        //list.add(mcsatSamples);
        String[] learnwt = list.toArray(new String[list.size()]);

        /*
        * 训练阶段
        * */
        int batch = 1; // 可调节
        int sampleSize = 1000; //可调节
        ArrayList<Clause> attributesPROB = null;
        for (int i = 0; i < batch; i++) {
            rule.formatEvidence(tupleList, newHeaderList, header, args[4]);
            //入口：参数学习 weight learning――using 'Diagonal Newton discriminative learning'
            attributesPROB = MLNmain.main(learnwt);
        }
        return attributesPROB;
    }

    public static HashMap<String, GroundRule> calTupleNum(ArrayList<data.Clause> clauseWeight, ArrayList<String> dataset) {//计算每条规则对应的tuple数量
//        ArrayList<String> list = Rule.readFileNoHeader(dataURL);
        HashMap<String, GroundRule> map = new HashMap<>();
        try {
            for (data.Clause clause : clauseWeight) {
                String prob = clause.getWeight();
                String rawMLN = clause.getContent();
                String mln = rawMLN.replaceAll("\"", "");
                String[] values = mln.split(" v ");
                for (int i = 0; i < values.length; i++) {
                    values[i] = values[i].replaceAll(" ", "")
                            .replaceAll(".*\\(", "")
                            .replaceAll("\\)", "");
                }
                Arrays.sort(values);//排序 方便比较
                int num = 0;
                for (int i = 0; i < dataset.size(); i++) {
                    String[] tuple = dataset.get(i).replaceAll(" ", "").split(",");
                    Arrays.sort(tuple);//排序 方便比较

                    int count = 0;
                    int key = 0;
                    for (int k = 0; k < values.length; k++) {//检验ifContains
                        while (key < tuple.length) {
                            if (values[k].equals(tuple[key])) {
                                count++;
                                key++;
                                break;
                            } else {
                                key++;
                            }
                        }
                    }
                    boolean flag = false;
                    if (count == values.length)
                        flag = true;
                    if (flag) {
                        num++;
                    }
                }
                map.put(rawMLN, new GroundRule(prob, num));//储存规则与它对应的Tuple的数量
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return map;
    }

    public static HashMap<String, GroundRule> calTupleNum(String mlnURL, String dataURL) {//计算每条规则对应的tuple数量
        ArrayList<String> list = Rule.readFileNoHeader(dataURL);
        HashMap<String, GroundRule> map = new HashMap<>();
        try {
            FileReader reader;
            reader = new FileReader(mlnURL);
            BufferedReader br = new BufferedReader(reader);
            String line = null;

            while ((line = br.readLine()) != null && line.length() != 0) {
                int index = line.indexOf(",");
                String prob = line.substring(0, index);
                String rawMLN = line.substring(index + 1);
                String mln = rawMLN.replaceAll("\"", "");
//                            .replaceAll(".*\\(","")
//                            .replaceAll("\\)","");
                String[] values = mln.split(" v ");
                for (int i = 0; i < values.length; i++) {
                    values[i] = values[i].replaceAll(" ", "")
                            .replaceAll(".*\\(", "")
                            .replaceAll("\\)", "");
                }
                Arrays.sort(values);//排序 方便比较
                int num = 0;
                for (int i = 0; i < list.size(); i++) {
                    String[] tuple = list.get(i).replaceAll(" ", "").split(",");
                    Arrays.sort(tuple);//排序 方便比较

                    int count = 0;
                    int key = 0;
                    for (int k = 0; k < values.length; k++) {//检验ifContains
                        while (key < tuple.length) {
                            if (values[k].equals(tuple[key])) {
                                count++;
                                key++;
                                break;
                            } else {
                                key++;
                            }
                        }
                    }
                    boolean flag = false;
                    if (count == values.length)
                        flag = true;
                    if (flag) {
                        num++;
                    }
                }
                map.put(rawMLN, new GroundRule(prob, num));//储存规则与它对应的Tuple的数量
            }
            br.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return map;
    }

    public static ArrayList<Tuple2<Integer, Clause>> normalizationMLN(ArrayList<ArrayList<data.Clause>> clauseWeight_list, ArrayList<ArrayList<String>> dataset_list, Integer keyID) {
        ArrayList<HashMap<String, GroundRule>> mapList = new ArrayList<>();

        ArrayList<Tuple2<Integer, Clause>> result = new ArrayList<>();

        for (int i = 0; i < dataset_list.size(); i++) {
            ArrayList<data.Clause> clauseWeight = clauseWeight_list.get(i);
            ArrayList<String> curr_dataset = dataset_list.get(i);
            HashMap<String, GroundRule> map = calTupleNum(clauseWeight, curr_dataset);
            mapList.add(map);
        }
        HashMap<String, Clause> avgMAP = new HashMap<>(mapList.get(0).size());
        for (int k = 0; k < mapList.size(); k++) {
            HashMap<String, GroundRule> map = mapList.get(k);

            Iterator<Map.Entry<String, GroundRule>> iter = map.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<String, GroundRule> entry = iter.next();
                String clause = entry.getKey();
                GroundRule gr = entry.getValue();
                Double prob = Double.parseDouble(gr.weight);
                int num = gr.number;
                if (num == 0) {
                    Clause c = new Clause(clause, gr.weight, 1);
                    avgMAP.put(clause, c);
                } else {
                    int count = num;
                    double avgPROG = prob * num;
                    for (int i = k + 1; i < mapList.size(); i++) {
                        GroundRule gr2 = mapList.get(i).get(clause);

                        if (gr2 != null) {
                            Double prob2 = Double.parseDouble(gr2.weight);
                            int num2 = gr2.number;
                            avgPROG += prob2 * num2;
                            count += num2;
                            mapList.get(i).remove(clause);
                        }
                    }
                    avgPROG = avgPROG / count;
                    DecimalFormat decimalFormat = new DecimalFormat("######0.0000");
                    Clause c = new Clause(clause, decimalFormat.format(avgPROG), count);
                    avgMAP.put(clause, c);
                }
            }
        }


        //write updated clauses to file
        try {
//            File writefile = new File(writeURL);
//            FileWriter fw = new FileWriter(writefile);
//            BufferedWriter bw = new BufferedWriter(fw);

            Iterator<Map.Entry<String, Clause>> new_iter = avgMAP.entrySet().iterator();
            while (new_iter.hasNext()) {
                Map.Entry<String, Clause> entry = new_iter.next();
                Clause c = entry.getValue();
                result.add(new Tuple2<>(keyID, c));
//                bw.write(content);
//                bw.newLine();
            }
//            bw.flush();
//            bw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }

    public static void normalizationMLN(ArrayList<String> newMLNs, ArrayList<String> dataURLs, String writeURL) {
        ArrayList<HashMap<String, GroundRule>> mapList = new ArrayList<>();
        for (int i = 0; i < dataURLs.size(); i++) {
            String newMLN = newMLNs.get(i);
            String dataURL = dataURLs.get(i);
            HashMap<String, GroundRule> map = calTupleNum(newMLN, dataURL);
            mapList.add(map);
        }
        HashMap<String, Double> avgMAP = new HashMap<>(mapList.get(0).size());
        for (int k = 0; k < mapList.size(); k++) {
            HashMap<String, GroundRule> map = mapList.get(k);

            Iterator<Map.Entry<String, GroundRule>> iter = map.entrySet().iterator();
            while (iter.hasNext()) {
                Map.Entry<String, GroundRule> entry = iter.next();
                String clause = entry.getKey();
                GroundRule gr = entry.getValue();
                Double prob = Double.parseDouble(gr.weight);
                int num = gr.number;
                if (num == 0) {
                    avgMAP.put(clause, prob);
                } else {
                    int count = num;
                    double avgPROG = prob * num;
                    for (int i = k + 1; i < mapList.size(); i++) {
                        GroundRule gr2 = mapList.get(i).get(clause);

                        if (gr2 != null) {
                            Double prob2 = Double.parseDouble(gr2.weight);
                            int num2 = gr2.number;
                            avgPROG += prob2 * num2;
                            count += num2;
                            mapList.get(i).remove(clause);
                        }
                    }
                    avgPROG = avgPROG / count;
                    avgMAP.put(clause, avgPROG);
                }
            }
        }


        //write updated clauses to file
        try {
            File writefile = new File(writeURL);
            FileWriter fw = new FileWriter(writefile);
            BufferedWriter bw = new BufferedWriter(fw);

            Iterator<Map.Entry<String, Double>> new_iter = avgMAP.entrySet().iterator();
            while (new_iter.hasNext()) {
                Map.Entry<String, Double> entry = new_iter.next();
                String clause = entry.getKey();
                Double prob = entry.getValue();
                DecimalFormat format = new DecimalFormat("#0.0000");
                String content = format.format(prob) + ",\t" + clause;
                bw.write(content);
                bw.newLine();
            }
            bw.flush();
            bw.close();
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    public static HashMap<Integer, String[]> clean(HashMap<String, Clause> attributesPROB, ArrayList<String> dataset_list, String[] args, String[] header, ArrayList<Integer> ignoredIDs) throws SQLException, IOException {

        String dataURL = DataFormat.baseHDFS + "dataSet/" + args[0] + "/" + args[1];
        String rulesURL = DataFormat.baseHDFS + "dataSet/" + args[0] + "/rules.txt";
//        String tmp_dataURL = dataURL;
        Rule rule = new Rule();
        Domain domain = new Domain();

        String splitString = ",";
        boolean ifHeader = true;
        List<Tuple> rules = rule.loadRules(rulesURL, splitString);
//        ArrayList<Tuple> tupleList = rule.initData(dataset_list, splitString);
        domain.header = header;

        /*ignoredIDs = rule.findIgnoredTuples(rules);
        domain.header = rule.header;
        header = rule.header;*/


        /*
        * 读取训练阶段的clauses权重结果
        * */
        /*
        int batch = 1; // 可调节
        List<HashMap<String, Double>> attributesPROBList = new ArrayList<>();
        for (int i = 0; i < batch; i++) { //懒得改了，没有for循环，这里batch=1
            //读取参数学习得到的团权重，存入HashMap
            System.out.println(">>> load Clauses Weight from MLNs out.txt");
            HashMap<String, Double> attributesPROB = Rule.loadRulesFromFile(baseURL + "/" + args[0] + "/out.txt");
            attributesPROBList.add(attributesPROB);
            System.out.println(">>> completed!");
        }*/

        /*
        * 清洗阶段
        * */
        //区域划分 形成Domains
        System.out.println(">>> Partition dataset into Domains...");
        List<HashMap<String, ArrayList<Integer>>> convert_domains = new ArrayList<>(rules.size());
        domain.init(dataset_list, splitString, rules, convert_domains, ignoredIDs); //初始化dataSet,dataSet_noIgnor,domains. convert_domains里面存了<Tuple,List<tupleID>>
        System.out.println(">>> Completed!");
        //domain.printDomainContent(domain.domains);

        //对每个Domain执行group by key操作,返回不被group到的outlier tuples
        System.out.println(">>> Do groupByKey process for each Domain...");
        List<List<Tuple>> domain_outlier = domain.groupByKey(domain.domains, rules, convert_domains);
        System.out.println(">>> Completed!");
        System.out.println(">>> Smooth outliers to matched Groups...");
        domain.smoothOutlierToGroup(domain_outlier, domain.Domain_to_Groups, domain.dataSet, convert_domains, domain.dataSet_noIgnor);
        System.out.println(">>> Completed!");

        //根据MLN的概率修正错误数据
        System.out.println(">>> Correct error Data By MLN probs...");
        domain.correctByMLN(domain.Domain_to_Groups, attributesPROB, domain.header, domain.domains);
        System.out.println(">>> Completed!");
        //打印修正后的Domain
//        domain.printDomainContent(domain.domains);

        System.out.println(">>> Combine Domains...");
        List<List<Integer>> keysList = domain.combineDomain(domain.Domain_to_Groups);    //返回所有重复数组的tupleID,并记录重复元组
        //打印重复数据的Tuple ID
        if (null == keysList || keysList.isEmpty()) System.out.println("\tNo duplicate exists.");
        else {
            System.out.println("\n>>> Delete duplicate tuples");
            // 根据keysList 保存的对于domain1 中每个group 保存的key值（这些是有可能会重复的）来去重
            // domain.printDataSet(domain.dataSet);
            // domain.deleteDuplicate(keysList, domain.dataSet);	//执行去重操作
            // domain.printDataSet(domain.dataSet);
            System.out.println(">>> completed!");
        }

//        domain.printConflicts(domain.conflicts);
        domain.findCandidate(domain.conflicts, domain.domains, attributesPROB, ignoredIDs);

        //print dataset after cleaning
        //domain.printDataSet(domain.dataSet);

//        writeToFile(DataFormat.cleanedFile, domain.dataSet, domain.header);

        return domain.dataSet;
    }

    public static void main(String[] args) {
        try {
            String logFile = args[0] + args[1].replaceAll("trainData", "cleanerspark_log").replaceAll("\\.csv", ".txt");
            Log localLog = new Log(logFile);
            System.out.println("Local Log URL=" + logFile);
            localLog.write("testData = " + args[2]);
            localLog.write("trainData = " + args[1]);

            System.out.println("baseURL=" + baseURL);

            JavaSparkContext sc = SparkConfig.start();
            log.info("baseURL=" + baseURL);

            JavaRDD s_trainRDD = sc.textFile(DataFormat.baseHDFS + "dataSet/" + args[0] + "/" + args[1]);//导入待清洗的数据集
            JavaRDD s_testRDD = sc.textFile(DataFormat.baseHDFS + "dataSet/" + args[0] + "/" + args[2]);//导入待清洗的数据集
            //ifHeader = sc.broadcast(DataFormat.ifHeader);
            double startTime = System.currentTimeMillis();    //获取开始时间

            //广播rules: 格式化Rules, 分为Reason和Result两部分
            Rule rule = new Rule();
            final Broadcast<List<Tuple>> ruleList = sc.broadcast(rule.loadRules(DataFormat.baseHDFS + "dataSet/" + args[0] + "/rules.txt", DataFormat.splitString));

            //广播rules-first-order-logic
            JavaRDD<String> ruleFOLRDD = sc.textFile(DataFormat.baseHDFS + "dataSet/" + args[0] + DataFormat.rulesFirstOrder);//导入first-order-logic.txt
            final Broadcast<ArrayList<String>> bc_rules = sc.broadcast((ArrayList<String>) ruleFOLRDD.collect());

            log.info("\nrules first order logic:");
            for (int i = 0; i < bc_rules.getValue().size(); i++) {
                log.info(bc_rules.getValue().get(i));
            }

            String tmp_header1 = s_trainRDD.top(1).get(0).toString(); //ID,model,make,type,year,condition,wheelDrive,doors,engine
            final Broadcast<String[]> header = sc.broadcast(tmp_header1.substring(3).split(DataFormat.splitString));//广播header
            String[] str_header = header.getValue();
            System.out.println("\nheader = " + Arrays.toString(str_header) + "\n");
            //log.info("\nheader = " + Arrays.toString(str_header)+"\n");
            //广播ignoredIDs
            ArrayList<Integer> ignoredIDs = new Rule().findIgnoredIDs(ruleList.getValue(), str_header);
            final Broadcast<ArrayList<Integer>> bc_ignoredIDs = sc.broadcast(ignoredIDs);

            ArrayList<Integer> newHeaderList = new ArrayList<>(str_header.length);
            for (int i = 0; i < str_header.length; i++) {
                newHeaderList.add(i);
            }
            for (int i = 0; i < ignoredIDs.size(); i++) {
                newHeaderList.remove(ignoredIDs.get(i));
            }
            String newHeader = "";
            for (int i = 0; i < newHeaderList.size(); i++) {
                newHeader += str_header[newHeaderList.get(i)];
                if (i != newHeaderList.size() - 1) {
                    newHeader += ",";
                }
            }

            //广播newHeader
            final Broadcast<String> bc_newHeader = sc.broadcast(newHeader);
            final Broadcast<ArrayList<Integer>> bc_newHeaderList = sc.broadcast(newHeaderList);

            //累加器, 保存节点数Slaves number
            //final Accumulator<Integer> PGnodeNum = sc.accumulator(0);

            JavaRDD trainRDD = s_trainRDD.filter(new Function<String, Boolean>() {
                int count = 0;

                @Override
                public Boolean call(String tupleLine) throws Exception {
                    boolean result = true;
                    if (count != 0) return result;
                    count++;
//                System.out.println("header.value() = "+Arrays.toString(header.value()));
                    String tmp[] = new String[header.value().length];
                    System.arraycopy(header.value(), 0, tmp, 0, tmp.length);
                    String header_value = "";
                    for (int i = 0; i < tmp.length; i++) {
                        header_value += tmp[i];
                        if (i != tmp.length - 1) {
                            header_value += DataFormat.splitString;
                        }
                    }
                    //String header_value = Arrays.toString(tmp).replaceAll(" ", "").replaceAll("\\[", "").replaceAll("\\]", "");
                    if (count == 1 && tupleLine.substring(3).equals(header_value)) {
                        result = false;
                    }
                    return result;
                }
            });

            JavaRDD testRDD = s_testRDD.filter(new Function<String, Boolean>() {
                int count = 0;

                @Override
                public Boolean call(String tupleLine) throws Exception {
                    boolean result = true;
                    if (count != 0) return result;
                    count++;
//                System.out.println("header.value() = "+Arrays.toString(header.value()));
                    String tmp[] = new String[header.value().length];
                    System.arraycopy(header.value(), 0, tmp, 0, tmp.length);
                    String header_value = "";
                    for (int i = 0; i < tmp.length; i++) {
                        header_value += tmp[i];
                        if (i != tmp.length - 1) {
                            header_value += DataFormat.splitString;
                        }
                    }
                    //String header_value = Arrays.toString(tmp).replaceAll(" ", "").replaceAll("\\[", "").replaceAll("\\]", "");
                    if (count == 1 && tupleLine.substring(3).equals(header_value)) {
                        result = false;
                    }
                    return result;
                }
            });

            //广播partitionNum
            int partitionNum = Integer.parseInt(args[3]);
            final Broadcast<Integer> bc_partitionNum = sc.broadcast(partitionNum);

            //train
            JavaPairRDD trainRDD_grouped = trainRDD.groupBy(new Function<String, Integer>() {//JavaPairRDD<Integer,Iterable<String>>
                private static final long serialVersionUID = 1L;

                @Override
                public Integer call(String tuple) throws Exception {
                    int partition_index = DataSet.partition(11);
                    return partition_index;
                }
            }).repartition(11);

            //<String, Double>
            JavaPairRDD trainResult = trainRDD_grouped.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, Iterable<String>>, Integer, Clause>() {
                private static final long serialVersionUID = 1L;

                @Override
                public Iterable<Tuple2<Integer, Clause>> call(Tuple2<Integer, Iterable<String>> t) throws Exception {
                    Integer keyID = t._1();
                    ArrayList<String> dataset_list = DataSet.init(t);
                    log.info("\n>>> Begin MLN Weight Learning...\n");

                    log.info("\n>>> Partition Data from testData.csv...\n");
                    int size = bc_partitionNum.getValue();
                    ArrayList<ArrayList<String>> data_parts = new ArrayList<>(size);
                    for (int i = 0; i < size; i++) {
                        data_parts.add(new ArrayList<>());
                    }
                    log.info("\nDataset List Size = " + dataset_list.size() + "\n");
                    ArrayList<String> token_list = Rule.partitionData(dataset_list, bc_partitionNum.getValue(), args[0], bc_rules.getValue(),
                            bc_newHeader.getValue(), header.getValue(), data_parts);

                    log.info("Begin Partition MLNs into '" + bc_partitionNum.getValue() + "' parts.");

                    ArrayList<String> dataURLs = new ArrayList<>();
                    ArrayList<ArrayList<Clause>> clauseWeight_list = new ArrayList<>();
                    for (int i = 0; i < bc_partitionNum.getValue(); i++) {
                        System.out.println("************ PARTITION" + i + " ************");
                        String dataWriteFile = "/home/hadoop/experiment/dataSet/" + args[0] + "/data-new" + i + ".txt";
                        String rulesWriteFile = "/home/hadoop/experiment/dataSet/" + args[0] + "/rules-new" + token_list.get(i) + ".txt";
                        String outFile = "/home/hadoop/experiment/dataSet/" + args[0] + "/out-" + token_list.get(i) + ".txt";
                        String evidenceFile = "/home/hadoop/experiment/dataSet/" + args[0] + "/evidence-" + token_list.get(i) + ".txt";
                        String mlnArgs[] = {args[0], dataWriteFile, rulesWriteFile, outFile, evidenceFile};

                        ArrayList<Clause> attributesPROB = Main.learnwt(data_parts.get(i), header.getValue(), mlnArgs, bc_newHeaderList.getValue()); //参数训练，最后生成[n=partitionNum]个out.txt文件

                        clauseWeight_list.add(attributesPROB);
                        dataURLs.add(dataWriteFile);
                    }
                    System.out.println(">>> Weight Learning Finished!");

                    ArrayList<Tuple2<Integer, Clause>> result = normalizationMLN(clauseWeight_list, data_parts, keyID);

                    return result;
                }
            });


            ArrayList<Tuple2<Integer, Clause>> list = (ArrayList<Tuple2<Integer, Clause>>) trainResult.collect();
            HashMap<String, Clause> clauseMap = new HashMap<>();
            for (int i = 0; i < list.size(); i++) {
                Tuple2<Integer, Clause> current = list.get(i);
                Clause clause = current._2();
                String clause_content = clause.getContent();
                String[] line = clause_content.split(" v ");
                String value = "";
                for (int k = 0; k < line.length; k++) {
                    String v = line[k];
                    v = v.replaceAll(".*\\(\"", "")
                            .replaceAll("\"\\).*", "");
                    value += v;
                    if (k != line.length - 1) {
                        value += ",";
                    }
                }
                String[] value_array = value.split(",");
                Arrays.sort(value_array);
                String clause_str = Arrays.toString(value_array);
                if (!clauseMap.containsKey(clause_str)) {
                    clauseMap.put(clause_str, clause);
                } else {
                    Clause old_clause = clauseMap.get(clause_str);
                    int old_num = old_clause.getNumber();
                    double old_prob = Double.parseDouble(old_clause.getWeight());
                    int curr_num = clause.getNumber();
                    double curr_prob = Double.parseDouble(clause.getWeight());
                    int new_num = old_num + curr_num;
                    double avgPROB = (old_prob * old_num + curr_prob * curr_num) / new_num;
                    DecimalFormat decimalFormat = new DecimalFormat("######0.0000");
                    Clause new_Clause = new Clause(clause_content, decimalFormat.format(avgPROB), new_num);
                    clauseMap.put(clause_str, new_Clause);
                }
            }

            final Broadcast<HashMap<String, Clause>> clauseWeightResult = sc.broadcast(clauseMap);

            //===========================================================================
            //transform dataset to tuple format
            /*JavaRDD testTupleRDD = testRDD.flatMap(new FlatMapFunction<String, Tuple>() {
                @Override
                public Iterable<Tuple> call(String str) throws Exception {
                    ArrayList<Tuple> tuples = new Domain().transformToTuple(str, ruleList.value(), bc_ignoredIDs.value(),header.value());
                    return tuples;
                }
            });

            JavaPairRDD domainRDD = testTupleRDD.groupBy(new Function<Tuple, Integer>() {//JavaPairRDD<Integer, Iterable<Tuple>>
                private static final long serialVersionUID = 1L;

                @Override
                public Integer call(Tuple tuple) throws Exception {
                    int domain_index = new Domain().init(tuple,ruleList.value());
                    return domain_index;
                }

            });*/

            /* count 测试用
            ArrayList<Tuple2<Integer, Iterable<Tuple>>> testList = (ArrayList<Tuple2<Integer, Iterable<Tuple>>>) domainRDD.collect();
            testList.size();
            int count = 0;
            for (Tuple2<Integer, Iterable<Tuple>> entity : testList) {
                Integer partition_i = entity._1();
                log.info("\npartition_i = "+partition_i);
                Iterable<Tuple> partition_list = entity._2();
                Iterator<Tuple> iter = partition_list.iterator();
                while (iter.hasNext()) {
                    count++;
                    Tuple tuple = iter.next();
                    System.out.println(tuple.getContext().toString());
                    log.info("tuple = "+tuple);
                }
            }
            log.info(count);*/

            /*domainRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Tuple2<Integer, Iterable<String>>>, Object, Object>() {
                @Override
                public Iterable<Tuple2<Object, Object>> call(Iterator<Tuple2<Integer, Iterable<String>>> iter) throws Exception {
                    List<Tuple> ruleslist = ruleList.value();
                    Tuple2<Integer, Iterable<String>> line = iter.next();
                    Integer partition_i = line._1();
                    Iterable<String> tuple = line._2();
                    ArrayList<Tuple2<Object, Object>> list = new ArrayList<>();
                    return list;
                }
            }).collect();*/

            //===========================================================================


            final JavaPairRDD testRDD_grouped = testRDD
                    .groupBy(new Function<String, Integer>() {//JavaPairRDD<Integer,Iterable<String>>
                        private static final long serialVersionUID = 1L;

                        @Override
                        public Integer call(String tuple) throws Exception {
                            int partition_index = DataSet.partition(1);
                            return partition_index;
                        }
                    });//.repartition(2)

            /*int count = 0;
            List<Tuple2<Integer, Iterable<String>>> list1 = testRDD_grouped.collect();
            for (int i = 0; i < list1.size(); i++) {
                Tuple2<Integer, Iterable<String>> curr = list1.get(i);
                Iterable<String> values = curr._2();
                Iterator<String> iter = values.iterator();
                while (iter.hasNext()){
                    count++;
                    String tuple = iter.next();
                    System.out.println(tuple);
                }
            }
            System.out.println(count);*/

            //test
            JavaPairRDD testResult = testRDD_grouped.flatMapToPair(new PairFlatMapFunction<Tuple2<Integer, Iterable<String>>, Integer, String>() {
                private static final long serialVersionUID = 1L;

                @Override
                public Iterable<Tuple2<Integer, String>> call(Tuple2<Integer, Iterable<String>> t) throws Exception {
                    log.info(">>> Loading data...");
                    HashMap<String, Clause> clauseWeight = clauseWeightResult.getValue();
                    ArrayList<String> dataset_list = DataSet.init(t);

                    System.out.println(">>> Begin Cleaning...");
                    ArrayList<Tuple2<Integer, String>> result = new ArrayList<>();
                    //清洗阶段
                    String mlnArgs[] = {args[0], args[2]};
                    HashMap<Integer, String[]> dataSet = clean(clauseWeight, dataset_list, mlnArgs, header.getValue(), bc_ignoredIDs.getValue());

                    //Main.writeToFile(DataFormat.cleanedFile, dataSet, header.getValue());
                    ArrayList<String> out = new ArrayList<>();
                    /*out.add("ID," + Arrays.toString(header.value())
                            .replaceAll("[\\[\\]]", "")
                            .replaceAll(" ", ""));*/
                    Iterator<Map.Entry<Integer, String[]>> dataset_iter = dataSet.entrySet().iterator();
                    while (dataset_iter.hasNext()) {
                        Map.Entry<Integer, String[]> entry = dataset_iter.next();
                        Integer ID = entry.getKey();
                        String[] tuple_array = entry.getValue();
                        String tuple = "";
                        for (int i = 0; i < tuple_array.length; i++) {
                            tuple += tuple_array[i];
                            if (i != tuple_array.length - 1) {
                                tuple += ",";
                            }
                        }

                        /*String tuple = Arrays.toString(entry.getValue())
                                .replaceAll("\\[", "")
                                .replace("]", "")
                                .replace(" ", "");*/

                        out.add("" + ID.toString() + "," + tuple);
                        result.add(new Tuple2<>(ID, tuple));
                    }
                    out.sort(new Comparator<String>() {
                        @Override
                        public int compare(String o1, String o2) {
                            int index1 = o1.indexOf(",");
                            int index2 = o2.indexOf(",");
                            int id1 = Integer.parseInt(o1.substring(0, index1));
                            int id2 = Integer.parseInt(o2.substring(0, index2));
                            if (id1 > id2) {
                                return 1;
                            } else {
                                return -1;
                            }
                        }
                    });

                    /*ArrayList<String> ground_data = pickData(out, DataFormat.baseHDFS + "dataSet/" + "HAI/rawData" + "/" + DataFormat.groundFile);
                    ground_data.sort(new Comparator<String>() {
                        @Override
                        public int compare(String o1, String o2) {
                            int index1 = o1.indexOf(",");
                            int index2 = o2.indexOf(",");
                            int id1 = Integer.parseInt(o1.substring(0, index1));
                            int id2 = Integer.parseInt(o2.substring(0, index2));
                            if (id1 > id2) {
                                return 1;
                            } else {
                                return -1;
                            }
                        }
                    });
                    evaluate(ground_data, out, dataset_list);*/


                    return result;
                }
            });

            double endTime = System.currentTimeMillis();    //获取结束时间
            double totalTime = (endTime - startTime) / 1000;
            DecimalFormat df = new DecimalFormat("#.00");
            localLog.write("Total Time: " + df.format(totalTime) + "s");
            testResult = testResult.sortByKey();
            try {
                String outfile = DataFormat.baseHDFS + "dataSet/" + args[0] + "/" + DataFormat.cleanedPath;
                localLog.write("cleanedDataSet.txt stored in=" + outfile);
                Configuration conf = new Configuration();
                FileSystem fs = FileSystem.get(URI.create(outfile), conf);
                Path path = new Path(outfile);
                if (fs.exists(path)) {
                    fs.delete(path, true);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

            String cleanedResultFile = "/home/hadoop/experiment/dataSet/" + args[0] + "/" + DataFormat.cleanedPath + ".csv";
            File file = new File(cleanedResultFile);
            FileWriter fw = null;
            BufferedWriter writer = null;
            ArrayList<String> cleanedData = new ArrayList<>();
            try {
                if (file.exists()) {// 判断文件是否存在
                    System.out.println("File exists");
                } else if (!file.getParentFile().exists()) {// 判断目标文件所在的目录是否存在
                    // 如果目标文件所在的文件夹不存在，则创建父文件夹
                    System.out.println("目标文件所在目录不存在，准备创建它！");
                    if (!file.getParentFile().mkdirs()) {// 判断创建目录是否成功
                        System.out.println("创建目标文件所在的目录失败！");
                    }
                } else {
                    file.createNewFile();
                }
                fw = new FileWriter(file);
                writer = new BufferedWriter(fw);

                writer.write("ID," + Arrays.toString(header.value())
                        .replaceAll("[\\[\\]]", "")
                        .replaceAll(" ", ""));
                writer.newLine();//换行

                List<Tuple2<Integer, String>> list1 = testResult.collect();
                for (int i = 0; i < list1.size(); i++) {
                    Tuple2<Integer, String> content = list1.get(i);
                    writer.write(content._1() + "," + content._2());
                    cleanedData.add(content._1() + "," + content._2());
                    writer.newLine();
                }
                writer.flush();
                writer.close();
            } catch (Exception e) {
                e.printStackTrace();
            }

//            testResult.saveAsTextFile(DataFormat.baseHDFS + args[0] + "/" + DataFormat.cleanedPath + ".csv");
            ArrayList<String> ground_data = pickData(cleanedData, DataFormat.baseHDFS + "dataSet/HAI/rawData/" + DataFormat.groundFile);
            ground_data.sort(new Comparator<String>() {
                @Override
                public int compare(String o1, String o2) {
                    int index1 = o1.indexOf(",");
                    int index2 = o2.indexOf(",");
                    int id1 = Integer.parseInt(o1.substring(0, index1));
                    int id2 = Integer.parseInt(o2.substring(0, index2));
                    if (id1 > id2) {
                        return 1;
                    } else {
                        return -1;
                    }
                }
            });

            //for debug test
            /*String groundDataURL = "/home/hadoop/experiment/dataSet/" + args[0] + "/groundData-3q.csv";
            System.out.println(">>>test groundDataURL = " + groundDataURL);
            writeToFile(ground_data, groundDataURL);*/

            evaluate(ground_data, cleanedData, DataFormat.baseHDFS + "dataSet/" + args[0] + "/" + args[2],localLog);
//            evaluate(ground_data, cleanedResultFile, DataFormat.baseHDFS + "dataSet/" + args[0] + "/" + args[2]);
            System.exit(0);
        } catch (Exception e) {
            //log.error(e.getMessage());
            e.printStackTrace();
        }

    }

    public static void writeToFile(ArrayList<String> list, String outFileURL) {
        File file = new File(outFileURL);
        FileWriter fw = null;
        BufferedWriter writer = null;
        try {
            if (file.exists()) {// 判断文件是否存在
                System.out.println("File already exists. " + outFileURL);
            } else if (!file.getParentFile().exists()) {// 判断目标文件所在的目录是否存在
                // 如果目标文件所在的文件夹不存在，则创建父文件夹
                System.out.println("mkdir！");
                if (!file.getParentFile().mkdirs()) {// 判断创建目录是否成功
                    System.out.println("failed mkdir！");
                }
            } else {
                file.createNewFile();
            }
            fw = new FileWriter(file);
            writer = new BufferedWriter(fw);


            for (String s : list) {
                writer.write(s);
                writer.newLine();//换行
            }
            writer.flush();
            writer.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void writeToFile(String cleanedFileURL, HashMap<Integer, String[]> dataSet, String[] header) {
        List<Map.Entry<Integer, String[]>> list = new ArrayList<>(dataSet.entrySet());

        Collections.sort(list, new Comparator<Map.Entry<Integer, String[]>>() {
            @Override
            public int compare(Map.Entry<Integer, String[]> o1, Map.Entry<Integer, String[]> o2) {
                return o1.getKey().compareTo(o2.getKey());
            }
        });

        File file = new File(cleanedFileURL);
        FileWriter fw = null;
        BufferedWriter writer = null;
        try {
            if (file.exists()) {// 判断文件是否存在
                System.out.println("文件已存在: " + cleanedFileURL);
            } else if (!file.getParentFile().exists()) {// 判断目标文件所在的目录是否存在
                // 如果目标文件所在的文件夹不存在，则创建父文件夹
                System.out.println("目标文件所在目录不存在，准备创建它！");
                if (!file.getParentFile().mkdirs()) {// 判断创建目录是否成功
                    System.out.println("创建目标文件所在的目录失败！");
                }
            } else {
                file.createNewFile();
            }
            fw = new FileWriter(file);
            writer = new BufferedWriter(fw);


            writer.write("ID," + Arrays.toString(header)
                    .replaceAll("[\\[\\]]", "")
                    .replaceAll(" ", ""));
            writer.newLine();//换行

            for (Map.Entry<Integer, String[]> map : list) {
                String line = Arrays.toString(map.getValue()).replaceAll("[\\[\\]]", "").replaceAll(" ", "");
                writer.write(map.getKey() + "," + line);
                writer.newLine();//换行
            }
            writer.flush();
            writer.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 为数据集设置TupleID
     */
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
                str = str.replaceAll(" ", "");
                StringBuffer sb = new StringBuffer(str);
                if (index == 0) {
                    sb.insert(0, "ID,");
                    bw.write(sb.toString() + "\n");
                } else {
                    sb.insert(0, index + ",");
                    bw.write(sb.toString() + "\n");
                }
                index++;
                //System.out.println(sb.toString());
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
}
