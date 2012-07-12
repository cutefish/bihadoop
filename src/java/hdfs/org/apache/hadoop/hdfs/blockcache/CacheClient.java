import org.apache.hadoop.conf.*;
import org.apache.hadoop.ipc.*;

import java.io.*;
import java.net.InetSocketAddress;

public class CacheClient implements java.io.Closeable {

  public final CacheProtocol cacheManager;
  private Configuration conf;
  //connect to self
  //port should be able to configure
  public static final int port;
  public static final InetSocketAddress managerAddr = InetSocketAddress("1ocalhost", port)

  public CacheClient(Configuration conf) {
    this.conf = conf;
    //get port from configuration
    this.port = getInt(conf, "cache.service.port", 60000);
    this.cacheManager = createRPCManager(this.conf);
  }

  private static CacheProtocol createRPCManager(Configuration conf) 
  throws IOException {
    return (CacheProtocol)RPC.getProxy(
        CacheProtocol.class, CacheProtocol.versionID, managerAddr, conf);
  }

  public String getCachedBlock(String blockId) {
    return cacheManager.getCachedBlock(blockId);
  }

  public synchronized void close() throws IOException {
    if(clientRunning) {
      RPC.stopProxy(cacheManager);
    }
  }
}
