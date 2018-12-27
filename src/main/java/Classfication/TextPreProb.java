package Classfication;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
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
    public static Map<String,Double> CalPreProb1(FileSystem fs, Path folderPath) throws IOException {
        int sum=0;
        Map<String,Double> map = new HashMap<String, Double>();
        List<Path> paths = new ArrayList<Path>();
        Path path1 = new Path("./output/textprob/result.txt");
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
                        map.put(classname.toString(), new Double(fileStatus1.length));
                    }

            }
            for (Map.Entry<String,Double> entry:map.entrySet()) {
                String key = entry.getKey();
                Double val = entry.getValue();
                double v = val/(double)sum;
           //     System.out.println(v);
                Double prob = val/(double)sum;
                map.put(key,prob);
            }
        }
        FSDataOutputStream outputStream=fs.create(path1);
        for (Map.Entry<String,Double> entry:map.entrySet()){
            outputStream.write((entry.getKey()+"\t"+String.valueOf(entry.getValue())+"\n").getBytes());
            outputStream.flush();
        }
        outputStream.close();
        return map;
    }

}
