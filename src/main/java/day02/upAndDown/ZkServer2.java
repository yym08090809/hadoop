package day02.upAndDown;

import org.apache.zookeeper.*;

public class ZkServer2 {
    public static ZooKeeper zkClient = null;
    private static String connectString = "192.168.88.110:2181,192.168.88.111:2181,192.168.88.112:2181";
    private static int sessionTimeout = 2000;
    private static String parentPath = "/servers";
    //1.获取链接对象
    public static void getConnect() throws Exception {
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                System.out.println(watchedEvent.getType() + "---" + watchedEvent.getPath());
            }
        });
    }
    //2.注册  创建
    public static void register() throws KeeperException, InterruptedException {
        String name = zkClient.create(parentPath+"/server2","server2".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        System.out.println("server2 服务上线了！！！");
    }

    //3.业务功能  Thread.sleep();
    public static void business() throws InterruptedException {
        System.out.println("server2 开始执行任务惹！！！");
        Thread.sleep(Long.MAX_VALUE);
    }
    //4. main
    public static void main(String[] args) throws Exception {
        getConnect();

        register();

        business();
    }
}
