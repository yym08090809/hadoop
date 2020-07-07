package day01;



import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;



public class HdfsClients {
  static   FileSystem fileSystem=null;

    public static void main(String[] args) {
      //mkdir();
       see();
    }

    static {
        //获取hadoop的连接驱动
        /*final URI uri, final Configuration conf,
        final String user*/
        try {
            fileSystem = FileSystem.get(new URI("hdfs://192.168.88.110:9000"), new Configuration(), "root");
        } catch (IOException e) {
            e.printStackTrace();
        } catch (URISyntaxException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

    //创建文件夹
    public static void mkdir(){
        try {
            fileSystem.mkdirs(new Path("/hdfs"));
            System.out.println("创建成功！！！");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    //删除文件

    public static void delete(){
        try {
            boolean flag = fileSystem.delete(new Path(""), true);
            System.out.println("删除成功！！！");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    //修改或者移动
    public static void mv(){
        try {
            fileSystem.rename(new Path(""),new Path(""));
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    //查看
    public static void see(){
        try {
            RemoteIterator<LocatedFileStatus> iterator = fileSystem.listFiles(new Path("/"), true);

            while(iterator.hasNext()){
                LocatedFileStatus next = iterator.next();
                System.out.println("文件的路径是："+next.getPath());
                System.out.println("文件的大小是："+next.getLen());
                System.out.println("文件的block块的大小："+next.getBlockSize());
                System.out.println("文件的副本数是："+next.getReplication());
                System.out.println("---------------------------------------------------");
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
