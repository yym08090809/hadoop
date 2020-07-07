package day02.zkClient;

import org.apache.zookeeper.*;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

public class ZkClient {
    ZooKeeper zkClient = null;
    private static String connectString="192.168.88.110:2181,192.168.88.111:2181,192.168.88.112:2181";
    private static int sessionTimeout = 2000;
    //获取链接对象
    @Before
    public void getConnect() throws Exception {
        zkClient = new ZooKeeper(connectString, sessionTimeout, new Watcher() {
            @Override
            public void process(WatchedEvent watchedEvent) {
                //监听的方法
                System.out.println("发生了修改,监控方法被调用！！！！");
                System.out.println(watchedEvent.getType()+"-----"+watchedEvent.getPath());
            }
        });
    }
    //创建节点
    @Test
    public void create() throws KeeperException, InterruptedException {
        String name = zkClient.create("/idea", "hello word!!!".getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT_SEQUENTIAL);
        System.out.println("创建成功,路径的名字是:"+name);
    }
    @Test
    public void showInfo(){
        try {
            List<String> children = zkClient.getChildren("/nodeTest", true);
            for (String child : children) {
                System.out.println(child);
            }

            //休眠
            Thread.sleep(Long.MAX_VALUE);

        } catch (KeeperException e) {
            e.printStackTrace();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
