import java.io.*;
import java.net.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.ipc.RPC;

import org.apache.hadoop.blockcache.BlockCacheProtocol;
import org.apache.hadoop.blockcache.BlockCacheStatus;

public class TestBlockCacheServer {
  public static final Log LOG = LogFactory.getLog(TestBlockCacheServer.class);

  public static void main(String args[]) {
    try {
      Configuration conf = new Configuration();
      int RPC_TIME_OUT = 10000; //10s 
      int port = conf.getInt("block.cache.server.port", 
                             BlockCacheProtocol.DEFAULT_SERVER_PORT);
      InetSocketAddress serverAddr = new InetSocketAddress("127.0.0.1", port);
      BlockCacheProtocol blockCacheServer = (BlockCacheProtocol)RPC.getProxy(
          BlockCacheProtocol.class, BlockCacheProtocol.versionID,
          serverAddr, conf, RPC_TIME_OUT);
      LOG.info("Started blockcache client on" + serverAddr);
      LOG.info("before block cache server get status");
      BlockCacheStatus bcs = blockCacheServer.getStatus("xyu40");
      LOG.info("block cache server returned");
    }
    catch(IOException e) {
      LOG.error("Block Server RPC Error");
    }
  }
}
