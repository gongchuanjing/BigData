package tk.chuanjing.zk;

import org.apache.curator.RetryPolicy;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.TreeCache;
import org.apache.curator.framework.recipes.cache.TreeCacheEvent;
import org.apache.curator.framework.recipes.cache.TreeCacheListener;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;
import org.testng.annotations.Test;

/**
 * @author ChuanJing
 * @date 2019/7/22 17:54
 */
public class ZkStudy {

    @Test
    public void createNode() throws Exception {
        // 定义连接字符串
        //String connectString = "192.168.183.100:2181,192.168.183.110:2181,192.168.183.120:2181";
        //已经在hosts文件中配置了映射地址，所以可以不写ip地址，直接写node01、node02、node03
        String connectString = "node01:2181,node02:2181,node03:2181";

        //设置重试机制
        RetryPolicy retryPolicy = new ExponentialBackoffRetry(3000, 3);

        //CuratorFramework client = CuratorFrameworkFactory.newClient(connectString, 1000, 1000, retryPolicy);
        CuratorFramework client = CuratorFrameworkFactory.newClient(connectString, retryPolicy);

        //调用start开启客户端操作
        client.start();

        //通过create来进行创建节点，并且需要指定节点类型
        //client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath("/createNode01");
        client.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).forPath("/createNode02", "helloworld".getBytes());

        client.close();
    }

    /**
     * 创建临时节点:CreateMode.EPHEMERAL
     *
     * @throws Exception
     */
    @Test
    public void createTempNode() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient("node01:2181,node02:2181,node03:2181", new ExponentialBackoffRetry(3000, 3));
        client.start();

        client.create().creatingParentsIfNeeded()
                .withMode(CreateMode.EPHEMERAL)
                //.forPath("/myTempNode", "tempNode".getBytes());
                .forPath("/hello5/myTempNode", "tempNode".getBytes());

        // 休眠5秒，不然客户端关闭，就看不到临时节点
        Thread.sleep(5000);

        client.close();
    }

    /**
     * 修改节点数据
     *
     * @throws Exception
     */
    @Test
    public void updateNodeData() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient("node01:2181", new ExponentialBackoffRetry(3000, 3));
        client.start();

        client.setData().forPath("/node01", "word5".getBytes());

        client.close();
    }

    /**
     * 几点数据查询
     *
     * @throws Exception
     */
    @Test
    public void getDatas() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient("node03:2181", new ExponentialBackoffRetry(3000, 3));
        client.start();

        byte[] bytes = client.getData().forPath("/node01");
        System.out.println(new String(bytes));

        client.close();
    }

    /**
     * zookeeper的watch机制
     *
     * @throws Exception
     */
    @Test
    public void watchNode() throws Exception {
        CuratorFramework client = CuratorFrameworkFactory.newClient("node01:2181", new ExponentialBackoffRetry(3000, 3));
        client.start();

        TreeCache cache = new TreeCache(client, "/node01");
        cache.getListenable().addListener(new TreeCacheListener() {

            /**
             * 通过 new TreeCacheListener() 创建一个内部类，覆写childEvent方法
             * @param curatorFramework  操作zk的客户端
             * @param treeCacheEvent    事件通知类型，这个类型里面封装了节点的修改、删除、新增等等一些列的操作
             * @throws Exception
             */
            @Override
            public void childEvent(CuratorFramework curatorFramework, TreeCacheEvent treeCacheEvent) throws Exception {
                ChildData data = treeCacheEvent.getData();
                if(null != data) {
                    //表明这个节点的数据有变化了，获取节点变化的类型，究竟是还节点的新增，还是修改，还是删除，还是添加子节点等等
                    TreeCacheEvent.Type type = treeCacheEvent.getType();

                    switch (type) {
                        case NODE_ADDED:
                            //节点新增
                            System.out.println("监听到了节点的新增事件");
                            break;

                        case NODE_UPDATED:
                            //节点数据修改
                            System.out.println("监听到了节点的修改事件");
                            break;

                        case INITIALIZED:
                            //节点的初始化
                            System.out.println("监听到了节点的初始化事件");
                            break;

                        case NODE_REMOVED:
                            //节点的移除
                            System.out.println("监听到了节点的移除事件");
                            break;

                        default:
                            System.out.println("其他事件就不一一列举了");
                            break;
                    }
                }
            }
        });

        //开始监听
        cache.start();
        Thread.sleep(600000000);

        //client.close();
    }
}