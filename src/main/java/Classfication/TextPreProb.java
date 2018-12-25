package Classfication;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TextPreProb {

    /**
     * 计算类别的先验概率
     * @param fs
     * @param folderPath
     * @return
     * @throws IOException
     */
    public static Map<Text,DoubleWritable> CalPreProb1(FileSystem fs, Path folderPath) throws IOException {
        int sum=0;
        Map<Text,DoubleWritable> map = new HashMap<Text, DoubleWritable>();
        List<Path> paths = new ArrayList<Path>();
        if (fs.exists(folderPath)) {
            Text classname ;
            FileStatus[] fileStatus = fs.listStatus(folderPath);
            for (int i = 0; i < fileStatus.length; i++) {
                FileSystem fileSystem=null;
                    FileStatus fileStatu = fileStatus[i];
                    if(fileStatu.isDirectory()) {
                        Configuration conf = new Configuration();
                        FileSystem fs1 = FileSystem.get(conf);
                        FileStatus[] fileStatus1 = fs1.listStatus(fileStatu.getPath());
                        sum += fileStatus1.length;
                        classname = new Text(fileStatu.getPath().getName());
                        map.put(classname, new DoubleWritable(fileStatus1.length));
                    }

            }
            for (Map.Entry<Text,DoubleWritable> entry:map.entrySet()) {
                Text key = entry.getKey();
                DoubleWritable val = entry.getValue();
                double v = val.get()/(double)sum;
           //     System.out.println(v);
                DoubleWritable prob = new DoubleWritable(val.get()/(double)sum);
                map.put(key,prob);
            }
        }
        return map;
    }

}
