package main;

import data.*;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFlatMapFunction;
import org.apache.spark.broadcast.Broadcast;
import scala.Int;
import scala.Tuple2;
import spark.SparkConfig;
import org.apache.spark.api.java.JavaRDD;
import tuffy.main.MLNmain;

import java.io.*;
import java.net.URI;
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
                context.add(line.replaceAll("\\(", "")
                        .replaceAll("\\)", "")
                        .replaceAll(" ", ""));
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

    public static ArrayList<Tuple2<String, Double>> learnwt(String[] args, ArrayList<Integer> newHeaderList) throws SQLException, IOException {
        String dataURL = args[1];

        double startTime = System.currentTimeMillis();    //获取开始时间

        Rule rule = new Rule();
        //Domain domain = new Domain();
        String evidence_outFile = baseURL + "/" + args[0] + "/evidence.db";

        //System.out.println("rootURL=" + rootURL);
        String cleanedFileURL = baseURL + "/" + args[0] + "RDBSCleaner_cleaned.txt";//存放清洗后的数据集
        System.out.println("dataURL = " + dataURL);
        String splitString = ",";

        boolean ifHeader = true;
        //List<Tuple> rules = rule.loadRules(dataURL, rulesURL, splitString);
        rule.initData(dataURL, splitString, ifHeader);//生成TupleList 供formatEvidence()使用

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
        String evidenceFileURL = baseURL + "/" + args[0] + "/evidence.db"; //samples/smoke/
        list.add(evidenceFileURL);
        String queryFile_args = "-queryFile";
        list.add(queryFile_args);
        String queryFileURL = baseURL + "/" + args[0] + "/query.db";
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
        String maxIter = "400";
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
        ArrayList<Tuple2<String, Double>> attributesPROB = null;
        for (int i = 0; i < batch; i++) {
            rule.formatEvidence(evidence_outFile, newHeaderList);
            //入口：参数学习 weight learning――using 'Diagonal Newton discriminative learning'
            attributesPROB = MLNmain.main(learnwt);
        }
        return attributesPROB;
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

    public static HashMap<Integer, String[]> clean(String[] args, String[] header, ArrayList<Integer> ignoredIDs) throws SQLException, IOException {

        String dataURL = DataFormat.baseHDFS + "dataSet/" + args[0] + "/" + args[1];
        String rulesURL = DataFormat.baseHDFS + "dataSet/" + args[0] + "/rules.txt";
        String tmp_dataURL = dataURL;
        Rule rule = new Rule();
        Domain domain = new Domain();

        System.out.println("dataURL = " + tmp_dataURL);

        String splitString = ",";
        boolean ifHeader = true;
        List<Tuple> rules = rule.loadRules(rulesURL, splitString);
        rule.initData(tmp_dataURL, splitString, ifHeader);
        domain.header = header;
        /*ignoredIDs = rule.findIgnoredTuples(rules);
        domain.header = rule.header;
        header = rule.header;*/


        /*
        * 读取训练阶段的clauses权重结果
        * */
        int batch = 1; // 可调节
        List<HashMap<String, Double>> attributesPROBList = new ArrayList<>();
        for (int i = 0; i < batch; i++) {
            //读取参数学习得到的团权重，存入HashMap
            System.out.println(">>> load Clauses Weight from MLNs out.txt");
            HashMap<String, Double> attributesPROB = Rule.loadRulesFromFile(baseURL + "/" + args[0] + "/out.txt");
            attributesPROBList.add(attributesPROB);
            System.out.println(">>> completed!");
        }

        /*
        * 清洗阶段
        * */
        //区域划分 形成Domains
        System.out.println(">>> Partition dataset into Domains...");
        List<HashMap<String, ArrayList<Integer>>> convert_domains = new ArrayList<>(rules.size());
        domain.init(tmp_dataURL, splitString, ifHeader, rules, convert_domains, ignoredIDs); //初始化dataSet,dataSet_noIgnor,domains. convert_domains里面存了<Tuple,List<tupleID>>
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
        domain.correctByMLN(domain.Domain_to_Groups, attributesPROBList, domain.header, domain.domains);
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

        domain.printConflicts(domain.conflicts);
        domain.findCandidate(domain.conflicts, domain.domains, attributesPROBList.get(0), ignoredIDs);

        //print dataset after cleaning
        //domain.printDataSet(domain.dataSet);

//        writeToFile(DataFormat.cleanedFile, domain.dataSet, domain.header);

        return domain.dataSet;
    }

    public static void main(String[] args) {
        try {
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
            JavaRDD<String> ruleFOLRDD = sc.textFile(DataFormat.baseHDFS + DataFormat.rulesFirstOrder);//导入first-order-logic.txt
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
            JavaPairRDD trainResult = trainRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<String>, String, Double>() {
                @Override
                public Iterable<Tuple2<String, Double>> call(Iterator<String> iter) throws Exception {
                    log.info("\n>>> Begin MLN Weight Learning...\n");

                    log.info("\n>>> Partition Data from testData.csv...\n");
                    log.info("URL = " + DataFormat.baseHDFS + "dataSet/" + args[0] + "/" + args[1]);
                    Rule.partitionData(DataFormat.baseHDFS + "dataSet/" + args[0] + "/" + args[1],
                            bc_partitionNum.getValue(), args[0], bc_rules.getValue(), bc_newHeader.getValue());

                    log.info("Begin Partition MLNs into '" + bc_partitionNum.getValue() + "' parts.");

                    ArrayList<String> newMLNs = new ArrayList<>();
                    ArrayList<String> dataURLs = new ArrayList<>();
                    ArrayList<Tuple2<String, Double>> result = new ArrayList<>();
                    for (int i = 0; i < bc_partitionNum.getValue(); i++) {
                        System.out.println("************ PARTITION" + i + " ************");
                        String dataWriteFile = "/home/hadoop/experiment/dataSet/" + args[0] + "/data-new" + i + ".txt";
                        String rulesWriteFile = "/home/hadoop/experiment/dataSet/" + args[0] + "/rules-new" + i + ".txt";
                        String outFile = "/home/hadoop/experiment/dataSet/" + args[0] + "/out-" + i + ".txt";
                        String mlnArgs[] = {args[0], dataWriteFile, rulesWriteFile, outFile};
                        ArrayList<Tuple2<String, Double>> attributesPROB = Main.learnwt(mlnArgs, bc_newHeaderList.getValue()); //参数训练，最后生成[n=partitionNum]个out.txt文件
                        result.addAll(attributesPROB);
                        newMLNs.add(outFile);
                        dataURLs.add(dataWriteFile);
                    }
                    System.out.println(">>> Weight Learning Finished!");

                    normalizationMLN(newMLNs, dataURLs, "/home/hadoop/experiment/dataSet/" + args[0] + "/out.txt");

                    return result;
                }
            });

            String outfile1 = DataFormat.baseHDFS + "dataSet/" + args[0] + "/" + DataFormat.cleanedPath;
            System.out.println("Weight learning outfile stored in=" + outfile1);
            Configuration conf = new Configuration();
            FileSystem fs1 = FileSystem.get(URI.create(outfile1), conf);
            Path path1 = new Path(outfile1);
            if (fs1.exists(path1)) {
                fs1.delete(path1, true);
            }
            trainResult.saveAsTextFile(outfile1);

            //test
            JavaPairRDD testResult = testRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<String>, Integer, String>() {
                @Override
                public Iterable<Tuple2<Integer, String>> call(Iterator<String> iter) throws Exception {
                    System.out.println(">>> Begin Cleaning...");
                    ArrayList<Tuple2<Integer, String>> result = new ArrayList<>();
                    //清洗阶段
                    String mlnArgs[] = {args[0], args[2]};
                    HashMap<Integer, String[]> dataSet = clean(mlnArgs, header.getValue(), bc_ignoredIDs.getValue());

                    //Main.writeToFile(DataFormat.cleanedFile, dataSet, header.getValue());

                    Iterator<Map.Entry<Integer, String[]>> dataset_iter = dataSet.entrySet().iterator();
                    while (dataset_iter.hasNext()) {
                        Map.Entry<Integer, String[]> entry = dataset_iter.next();
                        Integer ID = entry.getKey();
                        String tuple = Arrays.toString(entry.getValue())
                                .replaceAll("\\[", "")
                                .replace("]", "")
                                .replace(" ", "");
                        result.add(new Tuple2<>(ID, tuple));
                    }
                    double endTime = System.currentTimeMillis();    //获取结束时间
                    double totalTime = (endTime - startTime) / 1000;
                    System.out.println("Rounding Time = " + totalTime);
                    log.info("Rounding Time = " + totalTime);
                    return result;
                }
            });

            String outfile = DataFormat.baseHDFS + "dataSet/" + args[0] + "/" + DataFormat.cleanedPath;
            System.out.println("cleanedDataSet.txt stored in=" + outfile);
            FileSystem fs = FileSystem.get(URI.create(outfile), conf);
            Path path = new Path(outfile);
            if (fs.exists(path)) {
                fs.delete(path, true);
            }
            testResult.saveAsTextFile(outfile);

            ArrayList<String> ground_data = pickData(outfile.concat("/part-00000"), DataFormat.baseHDFS + "dataSet/" + args[0] + "/" + DataFormat.groundFile);
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

            evaluate(ground_data, outfile.concat("/part-00000"), DataFormat.baseHDFS + "dataSet/" + args[0] + "/" + args[2]);
        } catch (Exception e) {
            //log.error(e.getMessage());
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
