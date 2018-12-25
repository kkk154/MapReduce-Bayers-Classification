package utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;


public class ClearOutputResult {
    public static void clear(Configuration conf) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        String s = "./output";
        Path path = new Path(s);
        fs.delete(path, true);
    }
}
