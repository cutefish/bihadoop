package org.apache.hadoop.hdfs.blockcache;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient.BlockReader;
import org.apache.hadoop.hdfs.LocatedBlock;
import org.apache.hadoop.ipc.RPC;

import java.io.*;
import java.net.InetSocketAddress;

import org.apache.hadoop.hdfs.blockcache.BlockCacheProtocol.CachedBlock;

public class BlockCacheReader extends BlockReader {
  public static final Log LOG = LogFactory.getLog(BlockCacheReader.class);

  private static final String LOCAL_HOST = "127.0.0.1";

  ////50ms heartbeat to keep file exist
  //private static final long heartbeatInterval = 50; 

  private String src;
  private long posInBlock;
  private BlockCacheProtocol cacheServer;
  private CachedBlock block;
  private FileInputStream dataIn;
  //private Thread heartbeatThread;
  //private long numReads = 0;

  public BlockCacheReader(Configuration conf, 
                          String src, long pos) throws IOException {
    super(new Path("/blk_of_" + src + "_at_" + pos), 1);
    this.src = src;
    int port = conf.getInt("block.cache.server.port", 50200);
    InetSocketAddress serverAddr = new InetSocketAddress(LOCAL_HOST, port);
    cacheServer = (BlockCacheProtocol)RPC.getProxy(
        BlockCacheProtocol.class, BlockCacheProtocol.versionID,
        serverAddr, conf);
    try {
      block = cacheServer.getCachedBlockFile(src, pos);
    }
    catch (IOException e) {
      LOG.warn("BlockCacheServer connect failure at" + 
          " port: " + port + 
          " for file: " + src + 
          " at position: " + pos);
      throw e;
    }
    posInBlock = pos - block.getStartOffset();
    ////send heartbeat if read before.
    //heartbeatThread = new Thread(
    //    new Runnable() {
    //      private long lastNumReads = 0;
    //      public void run() {
    //        Thread.sleep(heartbeatInterval);
    //        if (numReads > lastNumReads) {
    //          try {
    //          cacheServer.heartbeat(block);
    //          lastNumReads = numReads;
    //        }
    //      }
    //    });
    //heartbeatThread.start();

    try {
      File dataFile = new File(block.getLocalPath());
      dataIn = new FileInputStream(dataFile);
      dataIn.skip(posInBlock);
    }
    catch (IOException e) {
      LOG.warn("BlockCacheReader cannot open" + 
          " local file: " + block.getLocalPath() +
          " after connected with server");
      throw e;
    }

    if (LOG.isDebugEnabled()) {
      LOG.debug("New BlockCacheReader for file: " + src + 
          " at position: " + pos + 
          " with startOffset: " + block.getStartOffset() + 
          " length: " + block.getLength() +
          " at local: " + block.getLocalPath());
    }
  }

  @Override
  public synchronized int read(byte[] buf, int off, int len) throws IOException {
    return dataIn.read(buf, off, len);
  }

  @Override
  public synchronized int skip(long n) throws IOException {
    return dataIn.skip(n);
  }

  public synchronized void seek(long n) throws IOException {
    if (LOG.isDebugEnabled()) {
      LOG.debug("seek " + n);
    }
    throw new IOException("seek() is not supported in BlockCacheReader");
  }

  public synchronized void close() throws IOException {
    if (dataIn != null) {
      dataIn.close();
      dataIn = null;
    }
    if (cacheServer != null) {
      RPC.stopProxy(cacheServer);
      cacheServer = null;
    }
  }
}

