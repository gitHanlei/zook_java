import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;
import org.apache.zookeeper.KeeperException.CodeDeprecated;

import java.util.Arrays;

/**
 * @description: zk_data
 * @Author hanlei
 * @create 2019-03-27 11:09
 **/
public class DataMonitor implements Watcher, AsyncCallback.StatCallback {
    ZooKeeper zooKeeper;
    String node;
    Watcher watcher;
    boolean dead;

    DataMonitorListener dataMonitorListener;

    byte prevData[];

    public interface DataMonitorListener {
        void exists(byte data[]);

        void closing(int rc);
    }

    public DataMonitor(ZooKeeper zooKeeper, String node, Watcher watcher, DataMonitorListener listener) {
        this.zooKeeper = zooKeeper;
        this.node = node;
        this.watcher = watcher;
        this.dataMonitorListener = listener;
        // 判断节点是否存在，并且监控这个节点的
        this.zooKeeper.exists(this.node, true, this, null);
    }

    public void process(WatchedEvent event) {
        String path = event.getPath();

        // 世界类型很多， 枚举类型
        if (event.getType() == Event.EventType.None) {
            switch (event.getState()) {
                case SyncConnected:
                    break;
                case Expired:
                    dead = true;
                    dataMonitorListener.closing(-112);
                    break;
            }


        } else {
            if (path != null && path.equals(this.node)) {
                zooKeeper.exists(this.node, true, this, null);
            }
        }
        if (watcher != null) {
            watcher.process(event);
        }
    }

    /**
     * @param rc
     * @param ptah
     * @param ctx
     * @param stat
     */
    public void processResult(int rc, String ptah, Object ctx, Stat stat) {
        boolean exists;

        switch (rc) {
            case CodeDeprecated.Ok:
                exists = true;
                break;
            case CodeDeprecated.NoNode:
                exists = false;
            case CodeDeprecated.SessionExpired:
            case CodeDeprecated.AuthFailed:
                dead = true;
                this.dataMonitorListener.closing(rc);
                return;
            default:
                zooKeeper.exists(this.node, true, this, null);
                return;
        }
        byte b[] = null;

        if (exists) {
            try {
                // 获取节点上的值
                b = zooKeeper.getData(this.node, false, null);
            } catch (KeeperException e) {
                e.printStackTrace();
            } catch (InterruptedException e) {
                return;
            }
        }
        // 如果上一的修改数据和本次的修改数据不一致说明说句有变动, 有可能只是设置了相同的值
        if ((b == null && b != prevData) || (b != null && !Arrays.equals(prevData, b))) {
            this.dataMonitorListener.exists(b);
            prevData = b;
        }
    }
}
