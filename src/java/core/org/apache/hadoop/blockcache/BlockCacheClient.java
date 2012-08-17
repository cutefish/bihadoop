package org.apache.hadoop.blockcache;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient.BlockReader;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.URI;

import org.apache.hadoop.blockcache.BlockCacheProtocol.CachedBlock;

public class BlockCacheClient implements java.io.Closable {

  private static final String LOCAL_HOST = "127.0.0.1";
  private static final int RPC_TIME_OUT = 10000; //10 seconds 

  private final UserGroupInformation ugi;
  private BlockCacheProtocol server;
  //A token to identify self with server
  private String token;

  public BlockCacheClient(URI name, Configuraiton conf) {
    ugi = UserGroupInformation.getCurrentUser();
    int port = conf.getInt("block.cache.server.port", 
                           BlockCacheProtocol.DEFAULT_SERVER_PORT);
    InetSocketAddress serverAddr = new InetSocketAddress(LOCAL_HOST, port);
    server = (BlockCacheProtocol)RPC.getProxy(
        BlockCacheProtocol.class, BlockCacheProtocol.versionID,
        serverAddr, conf, RPC_TIME_OUT);
    token = server.registerClient(ugi.getUserName(), name, conf);
  }

  public FSDataInputStream open(Path f, int bufferSize, 
                                FSDataInputStream in) throws IOException {
    return new CachedFSDataInputStream(new CachedFSInputStream(
            f, bufferSize, in));
  }

  public class CachedFSInputStream extends FSInputStream {

    private final FSDataInputStream backupIn;
    private FileInputStream localIn;
    private final Path src;
    private long positionInBlock; // position inside a block
    private long pos;
    private byte[] buf; //cache for local file
    private volatile boolean stillValid;

    public CachedFSInputStream(f, bufferSize, in) throws IOException {
    }

    @Override
    public synchronized void close() throws IOException {

    }

    @Override
    public synchronized int read() throws IOException {
    }

    @Override
    public synchronized int read(
        byte buf[], int off, int len) throws IOException {

    }

    @Override
    public synchronized int read(long position, byte[] buffer,
                                 int offset, int length) throws IOException {
    }

    @Override
    public synchronized void seek(long pos) throws IOException {
    }

    @Override
    public synchronized boolean seekToNewSource(
        long targetPos) throws IOException {
      return false;
    }

    @Override
    public synchronized long getPos() throws IOException {
      return pos;
    }

  }

  public class CachedFSDataInputStream extends FSDataInputStream {

    public CachedFSDataInputStream(CachedFSInputStream in) throws IOException {
      super(in);
    }
  }
}

public class BlockCacheReader extends BlockReader {
  public static final Log LOG = LogFactory.getLog(BlockCacheReader.class);

  private static final String LOCAL_HOST = "127.0.0.1";
  private static final String SERVER_PORT = "block.cache.server.port";

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
    super(new Path("blk_of" + src + "_" + pos), 1);
    this.src = src;
    int port = conf.getInt(SERVER_PORT, 
        BlockCacheProtocol.DEFAULT_SERVER_PORT);
    InetSocketAddress serverAddr = new InetSocketAddress(LOCAL_HOST, port);
    cacheServer = (BlockCacheProtocol)RPC.getProxy(
        BlockCacheProtocol.class, BlockCacheProtocol.versionID,
        serverAddr, conf, 10000);
//    cacheServer = (BlockCacheProtocol)RPC.getProxy(
//        BlockCacheProtocol.class, BlockCacheProtocol.versionID,
//        serverAddr, conf);
    try {
      long start = System.currentTimeMillis();
      block = cacheServer.getCachedBlock(src, pos);
      long end = System.currentTimeMillis();
      LOG.info("RPC getCachedBlock time: " + (end - start) + " ms");
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
          " length: " + block.getBlockLength() +
          " at local: " + block.getLocalPath());
    }
  }

  @Override
  public synchronized int read(byte[] buf, int off, int len) throws IOException {
    int n = dataIn.read(buf, off, len);
    posInBlock += n;
    return n;
  }

  @Override
  public synchronized long skip(long n) throws IOException {
    long actualSkip = dataIn.skip(n);
    posInBlock += n;
    return actualSkip;
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

