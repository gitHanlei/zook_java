import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.FileOutputStream;
import java.io.IOException;

/**
 * @description: zookeeper测试
 * @Author hanlei
 * @create 2019-03-25 10:13
 **/
public class zk implements Watcher, Runnable, DataMonitor.DataMonitorListener {

    String zonde;

    DataMonitor dataMonitor;

    ZooKeeper zooKeeper;

    String filename;

    String exec[];

    public static void main(String args[]) {
        final String hostPost = "172.16.6.15:2181";
        final String znode = "/z_test";
        String filename = "zk_data.txt";

        String exec[] = {hostPost, znode, filename};

        try {
            new zk(hostPost, znode, filename, exec).run();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * @param hostPort 链接地址
     * @param znode    数据节点
     * @param filename 保存的文件路径
     * @param exec
     * @throws KeeperException
     * @throws IOException
     */
    public zk(String hostPort, String znode, String filename,
              String exec[]) throws KeeperException, IOException {
        this.filename = filename;
        this.exec = exec;
        this.zonde = znode;
        zooKeeper = new ZooKeeper(hostPort, 30000, this);
        dataMonitor = new DataMonitor(zooKeeper, znode, null, this);
    }

    public void run() {
        try {
            synchronized (this) {

                while (true) {
                    wait();
                }
            }

        } catch (InterruptedException e) {

        }
    }

    @Override
    public void exists(byte[] data) {
        try {
            FileOutputStream fileOutputStream = new FileOutputStream(filename);
            fileOutputStream.write(data);
            fileOutputStream.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void closing(int rc) {
        synchronized (this) {
            notifyAll();
        }
    }

    /**
     * @param watchedEvent 重写了watcher的方法, 用户数据的监控
     */
    @Override
    public void process(WatchedEvent watchedEvent) {
        dataMonitor.process(watchedEvent);
    }
}
