package WordCount;

import com.sun.jersey.client.impl.CopyOnWriteHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import utils.Stopwords;
import utils.Tools;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import java.io.IOException;

//***
// 基于贝叶斯训练数据进行预测
// ***/
public class Predict {
    /**
     *
     * MapReduce 前向计算
     *
     * **/
   /* public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>
    {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Text _word_sum_ =new Text("_word_sum_");
        *//*
         * LongWritable 为输入的key的类型
         * Text 为输入value的类型
         * Text-IntWritable 为输出key-value键值对的类型
         *//*
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            StringTokenizer itr = new StringTokenizer(value.toString());  // 将TextInputFormat生成的键值对转换成字符串类型
            //获取文件的绝对路径
            String filepath = ((FileSplit)context.getInputSplit()).getPath().toString();
            while (itr.hasMoreTokens())
            {
                String s = itr.nextToken();
                //去除停用词
                if(Stopwords.isStopword(s))
                    continue;
                //去除数值
                if(Tools.isFloat(s))
                    continue;
                word.set(s);
                //每个词 key，1 value
                context.write(word, one);
                //_word_sum_ key,1 value
                context.write(_word_sum_,one);
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable>
    {
        private IntWritable result = new IntWritable();
        *//*
         * Text-IntWritable 来自map的输入key-value键值对的类型
         * Text-IntWritable 输出key-value 单词-词频键值对
         *//*
        public void reduce(Text key, Iterable<IntWritable> values,Context context ) throws IOException, InterruptedException
        {
            int sum = 0;
            for (IntWritable val : values)
            {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }*/

    //<文件路径，文件真实类别:文件预测类别>
    private static CopyOnWriteHashMap<String ,String> dict = new CopyOnWriteHashMap<>();
    //列表，分别存储每类的训练数据
    private static List<Map<String,Double>> bayersnet =new ArrayList<>();

    //
    // ****   线程池方法
    //
    static class MyRunnable implements Runnable {

        public CopyOnWriteHashMap.Entry entry;
        public MyRunnable(CopyOnWriteHashMap.Entry entry)
        {
            this.entry = entry;
        }
        @Override
        public void run() {
            //记录所有的词
            List<String> words = new ArrayList<>();
            //记录每个文档属于每个类的概率
            Map<String,Double> map2 = new HashMap<>();
            Configuration conf = new Configuration();
            FileSystem fs=null;
            //对每个文件进行操作
            String key =(String)entry.getKey();
            String value = (String)entry.getValue();
            String[] values = value.split(":");
            try {
                fs = FileSystem.get(conf);
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println("正在处理文档:"+key);
            try (
                    FSDataInputStream in = fs.open(new Path(key));
                    BufferedReader d = new BufferedReader(new InputStreamReader(in)))
            {
                    String line;
                    while ((line = d.readLine()) != null) {
                        words.add(line);
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
            System.out.println("words:");
            System.out.println(words);
            double prob =1;
            int flag = 0;
            String classname=null;
            int num;
            for (String s:words) {
                for (Map<String,Double> map:bayersnet) {
                    num = map.size();
                    for (Map.Entry<String,Double> entry1:map.entrySet()) {
                        String[] s1 = entry1.getKey().split(":");
              /*          System.out.println(" String[] s1 = entry1.getKey().split(\":\");");
                        System.out.println(s1[0]+" "+s1[1]);*/
                        classname = s1[0];
                        String word = s1[1];
                       // System.out.println(word);
                        if (s.equals(word)) {
                            System.out.println("找到相等的词");
                            flag = 1;
                            prob = prob * Double.valueOf(entry1.getValue());
                            map2.put(classname,prob);
                            break;
                        }
                    }
                    if(flag == 1)
                    {
                        System.out.println("没找到相等的词");
                        prob = prob*(1/(double)num);
                        map2.put(classname,prob);
                    }
                    flag = 0;
                }
                System.out.println(map2);
            }

            System.out.println(map2);

        }
    }

    public static void main(String[] args) throws IOException {

        Configuration conf = new Configuration();  // job的配置
        FileSystem fs = FileSystem.get(conf);
        Path bayers = new Path("./output");
        Path predir = new Path("./predict");
        FileStatus[] fileStatuses = fs.listStatus(predir);
        for (int i = 0; i < fileStatuses.length; i++) {
            Text classname ;
            //类别名称
            if (!fileStatuses[i].isDirectory())
                continue;
            classname = new Text(fileStatuses[i].getPath().getName());
            FileStatus[] fileStatusess = fs.listStatus(fileStatuses[i].getPath());
            for (int j = 0; j < fileStatusess.length; j++) {
                Path path = fileStatusess[j].getPath();
                CopyOnWriteHashMap<String,String> map = new CopyOnWriteHashMap<>();
                dict.put(path.toString(), classname.toString()+":"+"unknow");
            }
        }

        FileStatus[] fileStatuses1 = fs.listStatus(bayers);
        for (int i = 0; i < fileStatuses1.length; i++) {
            //<词类别:词，prob>
            Map<String,Double> map1 = new HashMap<>();
            Text classname ;
            //类别名称
            if (!fileStatuses1[i].isDirectory())
                continue;
            classname = new Text(fileStatuses1[i].getPath().getName());
            try (
                 FSDataInputStream in = fs.open(new Path("./output/"+classname.toString()+"/prob/result.txt")
                 );
                 BufferedReader d = new BufferedReader(new InputStreamReader(in));) {
                String line;
                while ((line = d.readLine()) != null) {
                    String[] strings = line.split("\t");
                    map1.put(classname.toString()+":"+strings[0],Double.valueOf(strings[1]));
                }
                bayersnet.add(map1);
               // System.out.println("*****map1:"+map1);
               // System.out.println("****bayersnet:"+bayersnet);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        //打印将要预测的文件列表
        System.out.println(dict);
        //打印训练好的数据
        System.out.println(bayersnet);
        // 创建一个线程池对象，控制要创建几个线程对象。
        // public static ExecutorService newFixedThreadPool(int nThreads)
        ExecutorService pool = Executors.newFixedThreadPool(20);

            // 可以执行Runnable对象或者Callable对象代表的线程
        for (CopyOnWriteHashMap.Entry entry:dict.entrySet()) {
            pool.submit(new MyRunnable(entry));
        }

        //pool.submit(new MyRunnable());

            //结束线程池
        pool.shutdown();
    }


}
