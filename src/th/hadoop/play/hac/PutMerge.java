package th.hadoop.play.hac;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

/**
 * Merging files while putting them into Hadoop File System 
 * Adapting for Hadoop 0.2x from Chuck Lam code in Hadoop in Action book 
 * 
 */
public class PutMerge {

    public static void main(String[] args) throws IOException {
        Configuration conf = new Configuration();
        FileSystem hdfs = FileSystem.get(conf);
        FileSystem local = FileSystem.getLocal(conf);
        Path inputDir = new Path(args[0]);
        Path hdfsFile = new Path(args[1]);

        FileStatus[] inputFiles = local.listStatus(inputDir);
        FSDataOutputStream out = hdfs.create(hdfsFile);

        for (int i = 0; i < inputFiles.length; i++) {
            System.out.println(inputFiles[i].getPath().getName());

            FSDataInputStream in = local.open(inputFiles[i].getPath());
            byte[] buffer = new byte[256];
            int byteread = 0;

            while ((byteread = in.read(buffer)) > 0) {
                out.write(buffer, 0, byteread);
            }
            in.close();
        }
        out.close();
    }
}
