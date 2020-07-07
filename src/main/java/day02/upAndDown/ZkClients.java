package day02.upAndDown;

import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ZkClients {
    static ZooKeeper zkClient = null;
    private static String connectString = "192.168.88.110:2181,192.168.88.111:2181,192.168.88.112:2181";
    private static int sessionTimeout = 2000;
    private static String parentPath = "/servers";

    //1.获取链接
    public static void getConnect() throws Exception {
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            public void process(WatchedEvent watchedEvent) {
                System.out.println(watchedEvent.getType() + "----" + watchedEvent.getPath());

                //每一次 改变  重新刷新列表
                try {
                    getList();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });
    }

    //2.获取 所有节点的列表
    public static void getList() throws Exception {
        //监听
        List<String> list = zkClient.getChildren(parentPath, true);
        //创建一个list
        ArrayList<String> list1 = new ArrayList<>();
        for (String name : list) {
            list1.add(name);
        }

        System.out.println(list1);
    }

    //3.业务方法
    public static void business() throws InterruptedException {
        Thread.sleep(Long.MAX_VALUE);
    }

    //4.main
    public static void main(String[] args) throws Exception {
        getConnect();
        business();
    }
}
