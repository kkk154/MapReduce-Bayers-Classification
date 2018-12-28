package Classfication;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.hdfs.util.ByteArrayManager;
import org.apache.hadoop.io.Text;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;

public class WordPerProb {

    /**
     * 计算每个单词的先验概率
     * remoteFilePath 输入文件路径
     * remoteFilePath1 输出文件路径
     */
    public static void WordProb(Configuration conf, String remoteFilePath) throws IOException {
        int sum = 0;
        int wordsum = Wordsum();
        Map<String, Integer> map = new HashMap<>();
        Map<String, Double> map1 = new HashMap<>();
        Path remotePath = new Path(remoteFilePath);
        try (FileSystem fs = FileSystem.get(conf);
             FSDataInputStream in = fs.open(remotePath);
             BufferedReader d = new BufferedReader(new InputStreamReader(in));) {
            String line;
            while ((line = d.readLine()) != null) {
                String[] strings = line.split("\t");
                map.put(strings[0], Integer.valueOf(strings[1]));
                System.out.println(line);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        sum = map.get("_word_sum_");
        for (Map.Entry<String, Integer> entry : map.entrySet()) {
            String key = entry.getKey();
            int value = entry.getValue();
            map1.put(key, (value+1) / (double) (sum+ wordsum));
        }
        //写入文件
        String outpath1 = "./output" + Path.SEPARATOR + remotePath.getParent().getName() + Path.SEPARATOR + "prob" + Path.SEPARATOR + "result1.txt";
        FileSystem fs1 = FileSystem.get(conf);
        FSDataOutputStream outputStream1 = fs1.create(new Path(outpath1));
        outputStream1.write((remotePath.getParent().getName() + "\t" + sum + "\n").getBytes());
        outputStream1.flush();
        outputStream1.close();
        //写入文件
        String outpath = "./output" + Path.SEPARATOR + remotePath.getParent().getName() + Path.SEPARATOR + "prob" + Path.SEPARATOR + "result.txt";
        System.out.println(outpath);
        FileSystem fs = FileSystem.get(conf);
        FSDataOutputStream outputStream = fs.create(new Path(outpath));
        for (Map.Entry<String, Double> entry : map1.entrySet()) {
            outputStream.write((entry.getKey() + "\t" + String.valueOf(entry.getValue()) + "\n").getBytes());
            outputStream.flush();
        }
        outputStream.close();
    }

    public static int Wordsum() throws IOException {
        Map<String, Integer> wordsumap = new HashMap<>();
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        Path bayers = new Path("./output");
        FileStatus[] fileStatuses1 = fs.listStatus(bayers);
        for (int i = 0; i < fileStatuses1.length; i++) {
            Map<String, Double> map1 = new HashMap<>();
            Text classname;
            //类别名称
            if (!fileStatuses1[i].isDirectory())
                continue;
            classname = new Text(fileStatuses1[i].getPath().getName());
            FSDataInputStream in = fs.open(new Path("./output/" + classname.toString() + "/part-r-00000")
            );
            BufferedReader d = new BufferedReader(new InputStreamReader(in));
            String line;
            while ((line = d.readLine()) != null) {
                String[] strings = line.split("\t");
                wordsumap.put(strings[0], 1);
            }
        }
        return wordsumap.size();
    }
}