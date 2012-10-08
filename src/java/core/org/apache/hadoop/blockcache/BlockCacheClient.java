package org.apache.hadoop.blockcache;

import java.io.*;
import java.net.InetSocketAddress;
import java.net.URI;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.StringUtils;

public class BlockCacheClient implements java.io.Closeable {

  private static final Log LOG = LogFactory.getLog(BlockCacheClient.class);

  private static final String LOCAL_HOST = "127.0.0.1";
  private static final int RPC_TIME_OUT = 100000; //100 seconds 

  private final UserGroupInformation ugi;
  private BlockCacheProtocol server;

  public BlockCacheClient(Configuration conf) throws IOException {
    ugi = UserGroupInformation.getCurrentUser();
    int port = conf.getInt("block.cache.server.port", 
                           BlockCacheProtocol.DEFAULT_SERVER_PORT);
    InetSocketAddress serverAddr = new InetSocketAddress(LOCAL_HOST, port);
    server = (BlockCacheProtocol)RPC.getProxy(
        BlockCacheProtocol.class, BlockCacheProtocol.versionID,
        serverAddr, conf, RPC_TIME_OUT);
  }

  public FSDataInputStream open(Path f, long versionId, int bufferSize, 
                                FileSystem fs) throws IOException {
    LOG.debug("openning FSDataInputStream for path: " + f);
    return new FSDataInputStream(new CachedFSInputStream(f, versionId,
                                                         bufferSize, fs));
  }

  public void close() {
    RPC.stopProxy(server);
  }

  private static enum ReadState {
    REPLICA,
    CACHE,
    REMOTE,
    EOF
  }

  public class CachedFSInputStream extends FSInputStream {

    //stream status
    private FileSystem fs = null;
    private boolean closed = false;
    private ReadState state = ReadState.REMOTE;
    private FileInputStream localIn = null;
    private FSInputStream backupIn = null;
    private final Path src;
    private long versionId;
    private long pos = 0;

    //block status
    //[blockStart, blockend)
    private long blockStart = 0; 
    private long blockEnd = 0;
    //To Do: maybe have a better protocol to return the file resource?

    public CachedFSInputStream(Path f, long versionId, int bufferSize, 
                               FileSystem fs) throws IOException {
      this.src = f;
      this.versionId = versionId;
      this.fs = fs;
      this.backupIn = fs.getInputStream(f, bufferSize);
      LOG.debug("opened CachedFSInputStream");
    }

    @Override
    public synchronized void close() throws IOException {
      if (closed == true) 
        return;
      if (localIn != null) 
        localIn.close();
      backupIn.close();
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
    public synchronized int read(byte buf[], int off, 
                                 int len) throws IOException {
      if (closed) {
        throw new IOException("Stream closed");
      }

      int n;

      //only contact server and update state once
      boolean shouldUpdateState = true;

      while (true) {
        if (blockStart <= pos && pos < blockEnd) {
          //use the current state to read
          int maxLength = (pos + len >= blockEnd) ? 
              (int)(blockEnd - pos) : len;
          switch (state) {
            case REPLICA:
              n = backupIn.read(buf, off, maxLength);
              pos += n;
              return n;
            case CACHE:
              //use try catch so that we can try again
              try {
                n = localIn.read(buf, off, maxLength);
                if (n < 0) 
                  throw new IOException("Unexpected eof:" + 
                                        " pos: " + pos + 
                                        " blockEnd: " + blockEnd);
                pos += n;
                return n;
              }
              catch (Exception e) {
                LOG.warn("Read cache local failed: " + 
                         StringUtils.stringifyException(e));
                if (localIn != null) {
                  localIn.close();
                  localIn = null;
                }
                state = ReadState.REMOTE;
              }
            case EOF:
              return -1;
            case REMOTE:
              break;
          }
        }

        //we are here because the current state stales(likely out of block)
        if (shouldUpdateState) {
          try {
            updateState();
          }
          catch (Exception e) {
            LOG.warn("Error update state from server: " + 
                     StringUtils.stringifyException(e));
            state = ReadState.REMOTE;
            break;
          }
          shouldUpdateState = false;
          continue;
        }
        break;
      }

      //we are here because we failed again during or after state update
      //just use back up stream for one block
      //this is very inefficient(high latency), but we expect it is rare.
      FileStatus file = fs.getFileStatus(src);
      if (pos >= file.getLen()) return -1;
      BlockLocation[] blocks = fs.getFileBlockLocations(file, pos, 1);
      if ((blocks == null) || (blocks.length == 0)) {
        throw new IOException("No block information for path: " + src);
      }
      if (blocks.length > 1) {
        throw new IOException("Too many block information at" + 
                              " src: " + src.getName() + 
                              " pos: " + pos);
      }
      blockStart = blocks[0].getOffset();
      blockEnd = blockStart + blocks[0].getLength();
      LOG.info("Reading using remote stream on block: " + 
               blockStart + "-" + blocks[0].getLength() + 
               " pos: " + pos);
      int maxLength = (pos + len > blockEnd) ? 
          (int)(blockEnd - pos) : len;
      backupIn.seek(pos);
      n = backupIn.read(buf, off, maxLength);
      pos += n;
      LOG.debug("Finish reading");
      return n;
    }

    private void updateState() throws IOException {
      LOG.debug("Updating status");
      Block block = server.cacheBlockAt(fs.getUri().toString(),
                                        ugi.getUserName(),
                                        src.toUri().toString(), 
                                        versionId, pos);
      LOG.debug("Cached block returned");
      blockStart = block.getOffset();
      blockEnd = blockStart + block.getLength();

      if (blockStart == -1) {
        //we reach eof
        LOG.debug("Encountered EOF");
        state = ReadState.EOF;
        return; 
      }

      if (block.shouldUseReplica()) {
        //server suggests using replica, so we fall back to original method
        //it is not necessary that the backupIn will read from the local
        //node, however, it is highly likely that it does so.
        LOG.debug("Server suggests local read using replica");
        backupIn.seek(pos);
        state = ReadState.REPLICA;
        return;
      }

      //here, server has already cached the block for us
      String localPath = block.getLocalPath();
      LOG.debug("Server cached block at: " + localPath);
      localIn = new FileInputStream(localPath);
      localIn.skip(pos - blockStart);
      state = ReadState.CACHE;
      return;
    }

    @Override
    public synchronized void seek(long pos) throws IOException {
      LOG.debug("seeking position: " + pos);
      if (blockStart <= pos && pos < blockEnd) {
        try {
          if (state == ReadState.REPLICA) {
            backupIn.seek(pos);
            this.pos = pos;
            return;
          }

          if (state == ReadState.CACHE) {
            long shouldSkip = pos - blockStart;
            while(shouldSkip > 0) {
              long n = localIn.skip(shouldSkip);
              shouldSkip -= n;
            }
            this.pos = pos;
            return;
          }
        }
        catch (IOException e) {
          LOG.warn("Seek problem: " + StringUtils.stringifyException(e));
        }
      }
      //set state to remote and let the next read deal with the real reads.
      state = ReadState.REMOTE;
      this.pos = pos;
      if (localIn != null) localIn.close();
      localIn = null;
    }

    public synchronized boolean seekToNewSource(
        long targetPos) throws IOException {
      seek(targetPos);
      return false;
    }

    public synchronized long getPos() throws IOException {
      return pos;
    }
  }
}

