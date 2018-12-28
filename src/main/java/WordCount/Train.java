package WordCount;
//#WorkCount.java
import java.util.Map;
import Classfication.TextPreProb;
import Classfication.WordCount;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import static Classfication.WordPerProb.WordProb;
import static utils.ClearOutputResult.clear;

//***
// 贝叶斯方法训练文本分类
//
// ****/
public class Train {
    /**
     * args[0] input dir
     * args[1] output dir
     **/
    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();  // job的配置
        FileSystem fs = FileSystem.get(conf);
        //自动清理运行结果
        clear(conf);
        //计算每个类的先验概率
        Map<String,Double> textprob = TextPreProb.CalPreProb1(fs,new Path("./input"));
        System.out.println("文本的先验概率：");
        for (Map.Entry<String,Double> entry:textprob.entrySet()) {
            System.out.println("Class:"+entry.getKey()+",Prob:"+entry.getValue().toString());
        }

        FileStatus[] fileStatus = fs.listStatus(new Path(args[0]));
        /***
         * 为每个类别提交一个wordcount, mapreduce任务
         * 后期可优化
         * **/
        for (int i = 0; i < fileStatus.length; i++) {
            //类别名称
            Text classname = new Text(fileStatus[i].getPath().getName());
            Path in = fileStatus[i].getPath();
            Path out = new Path(args[1]+Path.SEPARATOR+classname.toString());
            //提交mapreduce任务
            Job job = Job.getInstance(conf,classname.toString()+"word count");  // 初始化Job
            job.setJarByClass(WordCount.class);
            job.setMapperClass(WordCount.TokenizerMapper.class);
            job.setCombinerClass(WordCount.IntSumReducer.class);
            job.setReducerClass(WordCount.IntSumReducer.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(IntWritable.class);
            FileInputFormat.addInputPath(job,in );  // 设置输入路径
            FileOutputFormat.setOutputPath(job, out);  // 设置输出路径
            job.waitForCompletion(true);
            //计算每个类中每个词的先验概率
        }
        for(int i = 0; i<fileStatus.length;i++)
        {
            Text classname = new Text(fileStatus[i].getPath().getName());
            Path out = new Path(args[1]+Path.SEPARATOR+classname.toString());
            WordProb(conf,out.toString()+Path.SEPARATOR+"part-r-00000");
        }



     /*   Job job = Job.getInstance(conf, "word count");  // 初始化Job
        job.setJarByClass(WordCount.class);
        job.setMapperClass(WordCount.TokenizerMapper.class);
        job.setCombinerClass(WordCount.IntSumReducer.class);
        job.setReducerClass(WordCount.IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));  // 设置输入路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));  // 设置输出路径
        System.exit(job.waitForCompletion(true) ? 0 : 1);*/
    }
}
