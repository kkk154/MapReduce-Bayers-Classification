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
           // System.out.println("words:");
           // System.out.println(words);
            double prob =Math.E;
            int flag = 0;
            String classname=null;
            int num=1;
            for (String s:words) {
                if(Stopwords.isStopword(s)||isFloat(s))
                    continue;
                for (Map<String,Double> map:bayersnet) {
                    if(map.size()!=0)
                        num = map.size();
                    for (Map.Entry<String,Double> entry1:map.entrySet()) {
                        String[] s1 = entry1.getKey().split(":");
              /*          System.out.println(" String[] s1 = entry1.getKey().split(\":\");");
                        System.out.println(s1[0]+" "+s1[1]);*/
                        classname = s1[0];
                        String word = s1[1];
                  //      System.out.println("测试文档单词："+s+",训练文档单词："+word+",相等?"+s.equals(word));
                        if (s.equals(word)) {

                           // System.out.println("找到相等的词");
                            flag = 1;
                            prob = Math.abs(Math.log(prob))*Math.abs(Math.log(Double.valueOf(entry1.getValue())));
                        //    System.out.println("prob:"+prob);
                            map2.put(classname,prob);
                            break;
                        }
                    }
                    if(flag == 0)
                    {
                       // System.out.println("没找到相等的词");
                        prob = Math.abs(Math.log(prob))*Math.abs(Math.log((1/(double)num)));
                      //  System.out.println("prob:"+prob);
                        map2.put(classname,prob);
                    }
                    flag = 0;
                }
               // System.out.println(map2);
            }
            for(Map.Entry<String,Double> entry3:map2.entrySet())
            {
                for (Map.Entry<String,Double> entry4:textprob.entrySet()) {
                    if(entry3.getKey().equals(entry4.getKey()))
                    {
                        Double val = entry3.getValue()*entry4.getValue();
                        map2.put(entry3.getKey(),val);
                    }
                }
            }
            double maxprob = 0;
            String probclass = "";
            for (Map.Entry<String,Double> entry5:map2.entrySet()) {
                if (maxprob<entry5.getValue())
                {
                    maxprob = entry5.getValue();
                    probclass = entry5.getKey();
                }

            }
            dict.put(key,values[0]+":"+probclass);
            //System.out.println(map2);
            //System.out.println(dict);

        }
    }


    public static void main(String[] args) throws IOException, InterruptedException {

        Configuration conf = new Configuration();  // job的配置
        FileSystem fs = FileSystem.get(conf);
        Path bayers = new Path("./output");
        Path textprobdir = new Path("./output/textprob/result.txt");
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


        FSDataInputStream in = fs.open(textprobdir);
        BufferedReader d = new BufferedReader(new InputStreamReader(in));
        String line;
        while ((line = d.readLine()) != null) {
            String[] strings = line.split("\t");
            textprob.put(strings[0],Double.valueOf(strings[1]));
        }
            // System.out.println("*****map1:"+map1);
            // System.out.println("****bayersnet:"+bayersnet);

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
           // new Thread(new MyRunnable(entry)).start();
        }

        //pool.submit(new MyRunnable());
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
        for (Map.Entry<String,String> entry6:dict.entrySet()){
            String[] strings = entry6.getValue().split(":");
            try {
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
