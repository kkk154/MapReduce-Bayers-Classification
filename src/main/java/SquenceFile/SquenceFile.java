package SquenceFile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.ReflectionUtils;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

public class SquenceFile {

    private static final String[] DATA = {
            "One, two, buckle my shoe",
            "Three, four, shut the door",
            "Five, six, pick up sticks",
            "Seven, eight, lay them straight",
            "Nine, ten, a big fat hen"
    };

    public static void SFileWrite(String inPath,String outPath) throws IOException {
        String inpath = inPath;
        File file = new File(inpath);
        File[]  files = file.listFiles();
        String outpath = outPath;
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(outpath), conf);
        Path path = new Path(outpath);

        Text key = new Text();
        Text value = new Text();
        SequenceFile.Writer writer = null;
        try {
            writer = SequenceFile.createWriter(fs, conf, path, key.getClass(), value.getClass());

           /* for (int i = 0; i < 100; i++) {
                key.set(100 - i);
                value.set(DATA[i % DATA.length]);
                System.out.printf("[%s]\t%s\t%s\n", writer.getLength(), key, value);
                writer.append(key, value);
            }*/
            for (File f:files) {
                key.set(f.getPath());
                InputStream is = new FileInputStream(f);
                byte[] b=new byte[is.available()];
                is.read(b);
                String s = new String(b);
                value.set(s);
                System.out.printf("%s\t%s\n", key, value);
                writer.append(key, value);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            IOUtils.closeStream(writer);
        }
    }

    public static void SFileRead(String inPath) throws IOException {
        String uri = inPath;
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(URI.create(uri), conf);
        Path path = new Path(uri);

        SequenceFile.Reader reader = null;
        try {
            reader = new SequenceFile.Reader(fs, path, conf);
            Writable key = (Writable)
                    ReflectionUtils.newInstance(reader.getKeyClass(), conf);
            Writable value = (Writable)
                    ReflectionUtils.newInstance(reader.getValueClass(), conf);
            long position = reader.getPosition();
            while (reader.next(key, value)) {
        //同步记录的边界
                String syncSeen = reader.syncSeen() ? "*" : "";
                System.out.printf("%s\t%s\n", key, value);
                position = reader.getPosition(); // beginning of next record
            }
        } finally {
            IOUtils.closeStream(reader);
        }
    }

    public static void main(String args[])
    {
        try {
            SFileWrite(args[0],args[1]);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
