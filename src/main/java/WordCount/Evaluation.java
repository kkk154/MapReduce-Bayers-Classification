package WordCount;


import org.apache.commons.math3.util.Precision;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;


//**
// 评估贝叶斯分类器的算法性能
// *//
public class Evaluation {
    //计算精确度
    public static double Precision(List<String> trueclass,List<String> predclass)
    {
        int count =0;
        int AUSTR = 0;
        int CANA = 0;
        for (int i = 0 ;i<trueclass.size();i++)
        {
            if (trueclass.get(i).equals(predclass.get(i)))
            {
                count++;
            }
        }

        for (int i = 0 ;i<trueclass.size();i++)
        {
            if (trueclass.get(i).equals(predclass.get(i))&&trueclass.get(i).equals("AUSTR"))
            {
                AUSTR++;
            }
            if (trueclass.get(i).equals(predclass.get(i))&&trueclass.get(i).equals("CANA"))
            {
                CANA++;
            }
        }
        System.out.println("AUSTR:"+AUSTR+"个被正确检索");
        System.out.println("CANA:"+CANA+"个被正确检索");
        return count/(double)predclass.size();
    }
    //计算查全率
    public static double Recall(List<String> trueclass,List<String> predclass)
    {
        int count =0;
        for (int i = 0 ;i<trueclass.size();i++)
        {
            if (trueclass.get(i).equals(predclass.get(i)))
            {
                count++;
            }
        }
        return count/(double)trueclass.size();
    }
    //计算F1
    public static double F1(List<String> trueclass,List<String> predclass)
    {
        double p = Precision(trueclass,predclass);
        double r = Recall(trueclass,predclass);

        return 2*p*r/(p+r);
    }

    public static void main(String[] args) throws IOException {
        List<String> trueclass = new ArrayList<>();
        List<String> predclass = new ArrayList<>();
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream in = fs.open(new Path("./output/result/result.txt"));
        BufferedReader d = new BufferedReader(new InputStreamReader(in));
        String line;
        while ((line = d.readLine()) != null) {
                String[] strings = line.split("\t");
                System.out.println(strings.length);
                trueclass.add(strings[1]);
                predclass.add(strings[2]);
        }
        System.out.println("Precision="+Precision(trueclass,predclass));
        System.out.println("Recall="+Recall(trueclass,predclass));
        System.out.println("F1="+F1(trueclass,predclass));
    }
}
