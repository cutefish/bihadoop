import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;

import java.net.InetSocketAddress;

public class CacheServer implements CacheProtocol {
  private Server server;
  private static final int port;

  public CacheServer(Configuration conf) {
    this.port = getInt(conf, "cache.service.port", 60000);
    this.server = RPC.getServer(this, "localhost", this.port, conf);
  }

  public String getCachedBlock(String blockId) {
    return "returned from server";
  }
}
