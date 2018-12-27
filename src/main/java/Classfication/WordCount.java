package Classfication;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import utils.Stopwords;
import utils.Tools;
import java.io.IOException;
import java.util.StringTokenizer;

public class WordCount
{
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>
    {

        private final static IntWritable one = new IntWritable(1);
        private Text word = new Text();
        private Text _word_sum_ =new Text("_word_sum_");
        /*
         * LongWritable 为输入的key的类型
         * Text 为输入value的类型
         * Text-IntWritable 为输出key-value键值对的类型
         */
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            StringTokenizer itr = new StringTokenizer(value.toString());  // 将TextInputFormat生成的键值对转换成字符串类型
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
        /*
         * Text-IntWritable 来自map的输入key-value键值对的类型
         * Text-IntWritable 输出key-value 单词-词频键值对
         */
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
    }
}
