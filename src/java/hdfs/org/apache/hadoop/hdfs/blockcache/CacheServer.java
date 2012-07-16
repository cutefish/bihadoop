package org.apache.hadoop.hdfs.blockcache;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;

import java.io.*;
import java.net.InetSocketAddress;

public class CacheServer implements CacheProtocol {
  private Server server;
  private static int port;

  public CacheServer(Configuration conf) throws IOException {
    this.port = conf.getInt("cache.service.port", 60000);
    this.server = RPC.getServer(this, "localhost", this.port, conf);
    this.server.start();
  }

  public long getProtocolVersion(String protocol,
                                 long clientVersion) {
    return 1;
  }

  public void join() {
    try {
      this.server.join();
    }
    catch (InterruptedException ie) {
    }
  }

  public String getCachedBlock(String blockId) {
    return "returned from server" + blockId;
  }
}
