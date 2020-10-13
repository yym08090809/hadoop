package hadoopdfs;

/**
 * @author 李松柏
 * @createTime 2020/10/13 8:44
 * @description hdfs文件合并
 */

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.IOException;
import java.io.PrintStream;
import java.net.URI;

/**
 * 过滤掉文件名满足特定条件的文件
 */
class MyPathFilter implements PathFilter {
    String reg = null;

    MyPathFilter(String reg) {
        this.reg = reg;
    }

    @Override
    public boolean accept(Path path) {
        if (!(path.toString().matches(reg))) return true;
        return false;
    }
}

public class MergeFile {
    Path inputPath = null;
    Path outputPath = null;

    public MergeFile(String input, String output) {
        this.inputPath = new Path(input);
        this.outputPath = new Path(output);
    }

    public void doMerge() throws IOException, InterruptedException {
        Configuration conf = new Configuration();
        conf.set("fs.defaultFS", "hdfs://hadoop01:9000");
        conf.set("fs.hdfs.impl", "org.apache.hadoop.hdfs.DistributedFileSystem");
        FileSystem fsSource = FileSystem.get(URI.create(inputPath.toString()), conf,"root");
        FileSystem fsDst = FileSystem.get(URI.create(outputPath.toString()), conf,"root");
        //下面过滤掉输入目录中后缀为.abc的文件
        FileStatus[] sourceStatus = fsSource.listStatus(inputPath, new MyPathFilter(".*\\.abc"));
        FSDataOutputStream fsdos = fsDst.create(outputPath);
        PrintStream ps = new PrintStream(System.out);
        //下面分别读取过滤之后的每个文件的内容，并输出到同一个文件中
        for (FileStatus status : sourceStatus) {
            //下面打印后缀不为.abc的文件的路径、文件大小
            System.out.print("路径：" + status.getPath() + "    文件大小：" + status.getLen()
                    + "   权限：" + status.getPermission() + "   内容：");
            FSDataInputStream fsdis = fsSource.open(status.getPath());
            byte[] data = new byte[1024];
            int read = -1;
            while ((read = fsdis.read(data)) > 0) {
                ps.write(data, 0, read);
                fsdos.write(data, 0, read);
            }
            fsdis.close();
        }
        ps.close();
        fsdos.close();
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        MergeFile merge = new MergeFile("hdfs://hadoop01:9000/demo01/input","hdfs://hadoop01:9000/demo01/output/merge.txt");
        merge.doMerge();
    }
}
