package Classfication;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import utils.Stopwords;
import utils.Tools;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class WordPerProb {

    /**
     * 计算每个单词的先验概率
     * remoteFilePath 输入文件路径
     * remoteFilePath1 输出文件路径
     */
    public static void WordProb(Configuration conf, String remoteFilePath) throws IOException {
        int sum = 0;
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
            map1.put(key, value / (double) sum);
        }

        //写入文件
        String outpath = "./output"+ Path.SEPARATOR+remotePath.getParent().getName()+Path.SEPARATOR+"prob"+Path.SEPARATOR+"result.txt";
        System.out.println(outpath);
        FileSystem fs = FileSystem.get(conf);
        FSDataOutputStream outputStream=fs.create(new Path(outpath));
        for (Map.Entry<String,Double> entry:map1.entrySet()){
            outputStream.writeUTF(entry.getKey()+"\t"+String.valueOf(entry.getValue())+"\n");
            outputStream.flush();
        }
        outputStream.close();
    }

    /*
     *Use Mapreduce
     *
     * */
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private final static IntWritable zero = new IntWritable(0);
        //private final static IntWritable zero = new IntWritable(0);
        private Text word = new Text();
        private String s1 = "_word_sum_";
        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            super.map(key, value, context);
            StringTokenizer itr = new StringTokenizer(value.toString(),"\n");  // 将TextInputFormat生成的键值对转换成字符串类型
            while (itr.hasMoreTokens())
            {
                String[] s = itr.nextToken().split("\t");
                IntWritable Max = new IntWritable();
                word.set(s[0]);
                if(s[0].equals(s1))
                {
                    Max.set(Integer.valueOf(s[1]));
                    context.write(word,Max);
                }

                //每个词 key，1 value
                context.write(word, zero);
                //_word_sum_ key,1 value

            }
        }
    }

    public static class MaxReducer extends Reducer<Text , IntWritable , Text, IntWritable>
    {
        @Override
        protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
            super.reduce(key, values, context);
            if(key.toString().equals("_word_sum_"))
            {

            }
         //   context.write(key,values.);
        }
    }
}