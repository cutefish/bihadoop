package org.apache.hadoop.blockcache;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.UserGroupInformation;

import org.apache.hadoop.hdfs.DFSClient.BlockReader;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.URI;

import org.apache.hadoop.blockcache.BlockCacheProtocol.CachedBlock;

public class BlockCacheClient implements java.io.Closeable {

  private static final Log LOG = LogFactory.getLog(BlockCacheClient.class);

  private static final String LOCAL_HOST = "127.0.0.1";
  private static final int RPC_TIME_OUT = 10000; //10 seconds 

  private final UserGroupInformation ugi;
  private BlockCacheProtocol server;
  //A token to identify self with server
  private String token;

  public BlockCacheClient(URI name, Configuration conf) throws IOException {
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
            f, bufferSize), in);
  }

  public void close() {
    server.unregisterClient(token);
    RPC.stopProxy(server);
  }

  public class CachedFSInputStream extends FSInputStream {

    private boolean closed = false;
    private FileInputStream localIn = null;
    private final Path src;
    private String localPath;
    private long blockStartOffset = 0; 
    private long pos = 0;
    private byte[] buf; //cache for local file
    //To Do: maybe have a better protocol to return the file resource?

    public CachedFSInputStream(Path f, int bufferSize) throws IOException {
      src = f;
      buf = new byte[bufferSize];
    }

    @Override
    public synchronized void close() throws IOException {
      if (closed == true) 
        return
      if (localIn != null) 
        localIn.close();
      super.close();
      closed = true;
    }


    @Override
    public synchronized int read() throws IOException {
      byte[] oneByteBuf = new byte[1];
      int ret = read(oneByteBuf, 0, 1);
      return (ret <= 0) ? -1 : (oneByteBuf[0] & 0xff);
    }

    @Override
    public synchronized int read(byte buf[], 
                                 int off, int len) throws IOException {
      int n;
      //try local reads first
      try {
        if (localIn != null) {
          n = localIn.read(buf, off, len);
          if (n > 0) {
            pos += n;
            return n;
          }
        }
      }
      catch(Exception e) {
        LOG.warn("Local file read failure: " + localPath);
      }

      //local input null or reaches EOF or exception
      //try local cache server
      try {
        CachedBlock block = server.cacheBlockAt(Path f, long pos);
        localPath = block.getLocalPath();
        blockStartOffset = block.getStartOffset();
        localIn = new FileInputStream(localPath);
      }
      return 0;
    }

    @Override
    public synchronized int read(long position, byte[] buffer,
                                 int offset, int length) throws IOException {
      return 0;
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

    public CachedFSDataInputStream(CachedFSInputStream in, 
                                   FSDataInputStream backupIn) throws IOException {
      super(in);
    }
  }
}

class BlockCacheReader extends BlockReader {
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

