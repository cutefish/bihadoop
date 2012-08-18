package org.apache.hadoop.blockcache;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.blockcache.BlockCacheProtocol.CachedBlock;

import org.apache.hadoop.hdfs.DFSClient.BlockReader;

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
    return new CachedFSDataInputStream(f, bufferSize, in);
  }

  public void close() {
    server.unregisterClient(token);
    RPC.stopProxy(server);
  }

  /**
   * PosFileInputStream
   *
   * A wrapper class around FileInputStream so that we know exactly where we are
   * when reading from a file.
   */
  public class PosFileInputStream extends FileInputStream {
    private long pos;

    public PosFileInputStream(String name) throws IOException {
      super(name);
    }

    @Override
    public int read() throws IOException {
      int n = super.read();
      if (n < 0) pos = -1;
      else pos ++;
      return n;
    }

    @Override
    public int read(byte[] b) throws IOException {
      int n = super.read(b);
      if (n < 0) pos = -1;
      else pos += n;
      return n;
    }

    @Override
    public int read(byte[] b, int off, int len) throws IOException {
      int n = super.read(b, off, len);
      if (n < 0) pos = -1;
      else pos += n;
      return n;
    }

    @Override
    public long skip(long num) throws IOException {
      int n = super.skip(num);
      pos += n;
      return n;
    }

    /**
     * Return the current position. 
     *
     * It is possible that the returned value is beyond EOF.
     */
    public long getPos() {
      return pos;
    }
  }
  
  public class CachedFSInputStream extends FSInputStream {

    private boolean closed = false;
    private FileInputStream localIn = null;
    private final Path src;
    private String localPath;
    private long blockStartOffset = 0; 
    private long blockLength = 0;
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
          if (n >= 0) {
            pos += n;
            return n;
          }
        }
      }
      catch(Exception e) {
        LOG.warn("Local file [" + localPath + 
                 "] read failure: " + StringUtils.stringyfyException(e));
      }

      localIn.close();

      //local input null or at EOF or exception
      //try local cache server
      CachedBlock block = server.cacheBlockAt(Path f, long pos);
      localPath = block.getLocalPath();
      blockStartOffset = block.getStartOffset();
      blockLength = block.getBlockLength();
      localIn = new FileInputStream(localPath);
      localIn.skip(pos - blockStartOffset);

      n = localIn.read(buf, off, len);
      pos += n;

      return n;
    }

    @Override
    public synchronized int read(long position, byte[] buffer,
                                 int offset, int length) throws IOException {
      int n;
      //try local reads first
      if ((blockStartOffset <= position) &&
          (position <= blockStartOffset + blockLength)) {
      }
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

    private FSDataInputStream backupIn;
    private Path file;
    private int bufferSize;

    public CachedFSDataInputStream(Path f, int bufferSize, 
                                   FSDataInputStream backupIn) throws IOException {
      super(in);
    }
  }
}

