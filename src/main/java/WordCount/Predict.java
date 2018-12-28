package WordCount;

import com.sun.jersey.client.impl.CopyOnWriteHashMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.Text;
import utils.Stopwords;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.io.IOException;
import static utils.Tools.isFloat;

//***
// 基于贝叶斯训练数据进行预测
// ***/
public class Predict {

    //<文件路径，文件真实类别:文件预测类别>
    private static CopyOnWriteHashMap<String ,String> dict = new CopyOnWriteHashMap<>();
    //列表，分别存储每类的训练数据
    private static List<Map<String,Double>> bayersnet =new ArrayList<>();
    //
    private static Map<String, Double> textprob = new HashMap<>();
    //单词总数
    private static int wordsum = 0;
    //所有类别，单词个数
    private static Map<String,Integer> classwordsum = new HashMap<>();
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
            double prob =0;
            int flag = 0;
            String classname=null;
            int num=1;
            for (String s:words) {
                if(Stopwords.isStopword(s)||isFloat(s))
                    continue;
                for (Map<String,Double> map:bayersnet) {
                    for (Map.Entry<String,Double> entry1:map.entrySet()) {
                        String[] s1 = entry1.getKey().split(":");
                        classname = s1[0];
                        String word = s1[1];
                        num = classwordsum.get(classname);
                        if (s.equals(word)) {
                            flag = 1;
                            prob = prob+Math.log(Double.valueOf(entry1.getValue()));
                            map2.put(classname,prob);
                            break;
                        }
                    }
                    if(flag == 0)
                    {
                        prob = prob+Math.log((1/(double)(num+wordsum)));
                        map2.put(classname,prob);
                    }
                    flag = 0;
                }
            }
            for(Map.Entry<String,Double> entry3:map2.entrySet())
            {
                for (Map.Entry<String,Double> entry4:textprob.entrySet()) {
                    if(entry3.getKey().equals(entry4.getKey()))
                    {
                        Double val = entry3.getValue()+Math.log(entry4.getValue());
                        map2.put(entry3.getKey(),val);
                    }
                }
            }
            System.out.println(map2);
            int count = 0;
            double maxprob =-100000000;
            String probclass = "";
            for (Map.Entry<String,Double> entry5:map2.entrySet()) {
                if (maxprob<entry5.getValue())
                {
                    maxprob = entry5.getValue();
                    probclass = entry5.getKey();
                }
            }
            dict.put(key,values[0]+":"+probclass);
        }
    }


    public static void main(String[] args) throws IOException, InterruptedException {

        Configuration conf = new Configuration();  // job的配置
        FileSystem fs = FileSystem.get(conf);
        Path bayers = new Path("./output");
        Path textprobdir = new Path("./output1/textprob/result.txt");
        Path predir = new Path("./predict");
        FileStatus[] fileStatuses = fs.listStatus(predir);
        Map<String,Integer> wordsumap = new HashMap<>();
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
                    wordsumap.put(strings[0],1);
                }
                bayersnet.add(map1);
            } catch (IOException e) {
                e.printStackTrace();
            }
            try (
                    FSDataInputStream in = fs.open(new Path("./output/"+classname.toString()+"/prob/result1.txt")
                    );
                    BufferedReader d = new BufferedReader(new InputStreamReader(in));) {
                String line;
                while ((line = d.readLine()) != null) {
                    String[] strings = line.split("\t");
                    classwordsum.put(strings[0],Integer.valueOf(strings[1]));
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        System.out.println("classwordsum:"+classwordsum);
        wordsum = wordsumap.size();

        FSDataInputStream in = fs.open(textprobdir);
        BufferedReader d = new BufferedReader(new InputStreamReader(in));
        String line;
        while ((line = d.readLine()) != null) {
            String[] strings = line.split("\t");
            textprob.put(strings[0],Double.valueOf(strings[1]));
        }
        //打印将要预测的文件列表
        System.out.println(dict);
        //打印训练好的数据
        System.out.println(bayersnet);
        //打印文本先验概率
        System.out.println(textprob);
        // 创建一个线程池对象，控制要创建几个线程对象。
      //   public static ExecutorService newFixedThreadPool(int nThreads)
        ExecutorService pool = Executors.newFixedThreadPool(20);

            // 可以执行Runnable对象或者Callable对象代表的线程
        for (CopyOnWriteHashMap.Entry entry:dict.entrySet()) {
            pool.submit(new MyRunnable(entry));
        }
        //结束线程池
        pool.shutdown();
        while(true){
            if(pool.isTerminated()){
                System.out.println("所有的子线程都结束了！");
                break;
            }
            Thread.sleep(1000);
        }
        FSDataOutputStream outputStream= null;
        try {
            outputStream = fs.create(new Path("./output/result/result.txt"));
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(dict);
        for (Map.Entry<String,String> entry6:dict.entrySet()){
            String[] strings = entry6.getValue().split(":");
            System.out.println(entry6.getValue());
            System.out.println(strings.length);
            try {
                System.out.println("Path:"+entry6.getKey()+",真实类别:"+strings[0]+",预测类别:"+strings[1]);
                outputStream.write((entry6.getKey()+"\t"+strings[0]+"\t"+strings[1]+"\n").getBytes());
            } catch (IOException e) {
                e.printStackTrace();
            }
            try {
                outputStream.flush();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            outputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

    }


}
