package org.apache.hadoop.hdfs.blockcache;


import java.io.*;
import java.net.InetSocketAddress;
import java.util.Map;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.hdfs.DFSInputStream;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;

import org.apache.hadoop.hdfs.blockcache.BlockCacheProtocol.CachedBlock;

public class BlockCacheServer extends DFSClient implements BlockCacheProtocol {

  private static final Log LOG = LogFactory.getLog(BlockCacheServer.class);

  private static final String SERVER_PORT = "block.cache.server.port";
  private static final String CACHE_CAPACITY = "block.cache.capacity";
  private static final String MMAP_CACHED_FILE = "mmap.cached.file";
  private static final String LOCAL_DIR = "block.cache.local.dir";
  private static final String GC_CAPACITY = "block.cache.gc.capacity.threshold";
  private static final String GC_TIME = "block.cache.gc.time.threshold";

  private static final String LOCAL_HOST = "127.0.0.1";
  private static final int DEFAULT_SERVER_PORT = 50200;
  private static final int DEFAULT_BUFFER_SIZE = 4096;
  private static final int DEFAULT_CACHE_CAPACITY = 
    1024 * 1024 * 1024 / DEFAULT_BLOCK_SIZE; // 1G / blockSize
  private static final long UNDER_CONSTRUCTION = -1;
  private static final long FAILED_CONSTRUCTION = 0;

  private final Map<String, DFSInputStream> streamCache; //file -> DFSInputStream
  private final Map<CachedBlock, TimedCachedBlock> cachedBlocks; 
  private Server rpcListener;
  private final boolean mmapCachedFile = false;
  private String localCacheDir;
  private int gcCapacityThreadshold;
  private long gcTimeThreshold;
  private Thread gcRunner;

  static class TimedCachedBlock {
    CachedBlock block;
    long timestamp;

    TimedCachedBlock(CachedBlock block, long timestamp) {
      this.block = block;
      this.timestamp = timestamp;
    }

    CachedBlock getBlock() {
      return block;
    }

    long getTimestamp() {
      return timestamp;
    }

    void setTimestamp(long t) {
      timestamp = t;
    }
  }

  public BlockCacheServer(Configuration conf) throws IOException {
    super(conf);
    try {
      createServer(conf);
    }
    catch (IOException e) {
      shutdown();
      throw e;
    }
  }

  public void createServer(conf) throws IOException {
    //cache opened DFSInputStream to reduce access to namenode.
    final int cacheSize = 100;
    final float hashTableLoadFactor = 0.75f;
    int hashTableCapacity = (int) Math.ceil(cacheSize / hashTableCapacity) + 1;
    this.streamCache = 
      Collections.synchronizedMap(new LinkedHashMap<String, DFSInputStream>(
          hashTableCapacity, hashTableLoadFactor, true) {
        private static final long serialVersionUID = 1;

        @Override
        protected boolean removeEldestEntry(
          Map.Entry<String, DFSInputStream> eldest) 
        {
          return size() > cacheSize;
        }
      })

    //cachedBlocks
    int numBlocks = conf.getInt(CACHE_CAPACITY, DEFAULT_CACHE_CAPACITY);
    this.cachedBlocks = new HashMap<CachedBlock, long>(numBlocks);

    //rpc server
    int port = conf.getInt(SERVER_PORT, DEFAULT_SERVER_PORT);
    this.rpcListner = RPC.getServer(this, LOCAL_HOST, this.port, conf);

    //local dir
    this.localCacheDir = conf.get(LOCAL_DIR, "/tmp/dfs/block_cache_dir");
    new File(localCacheDir).mkdirs();

    //gc
    this.gcCapacityThreadshold = conf.getInt(GC_CAPACITY, 0.75 * blockCacheSize);
    this.gcTimeThreshold = conf.getLong(GC_TIME, 500);
    this.gcRunner = new Thread(new GCRunner());
    this.gcRunner.setDaemon(true);

    LOG.debug("Block Cache Server instance" +
        ", port: " + port +
        ", cache block capacity: " + numBlocks +
        ", local cache dir: " + localCacheDir + 
        ", gc capacity threshold: " + gcCapacityThreadshold + 
        ", gc time threshold: " + gcTimeThreshold);
  }

  //Garbage collection on the cachedBlocks.
  //If the timestamp of created block exceeds a threshold, the related local
  //file will be deleted. 
  //
  //N.B.
  //This design has two assumptions:
  //1. The underlying OS is linux. The block reader that is reading on the local
  //file can still read the file because the linux OS will not delete the file
  //until all the handler to the file are closed.  
  //2. The block reader would not hold the stream handle very long, and thus
  //the system will not be flooded with to-be-deleted files.
  //
  //This is not good design, need to be fixed.
  public class GCRunner implements Runnable {
    public void run() {
      List<String> toDelete = new ArrayList<String>();
      while(true) {
        Thread.sleep(gcTimeThreshold / 2);
        long curr = System.currentTimeMillis();
        synchronized(cachedBlocks) {
          if (cachedBlocks.size() < gcCapacityThreadshold) {
            continue;
          }
          for (Iterator<CachedBlock, TimedCachedBlock> 
              it = cachedBlocks.iterator(); it.hasNext();) {
            Map.Entry<CachedBlock, TimedCachedBlock> entry = it.next();
            long lastUse = entry.getValue().getTimestamp();
            if (lastUse == UNDER_CONSTRUCTION) {
              continue;
            }
            if (curr - lastUse > gcTimeThreshold) {
              toDelete.add(entry.getKey().getLocalPath());
              it.remove();
            }
          }
        }
        for (String file : toDelete) {
          boolean success;
          try {
            success = (new File(file)).delete();
          }
          catch (IOException e) {
            LOG.warn("GCRunner delete file raised IOException," + 
               " file: " +  file
               " exception: " + StringUtils.stringifyException(e));
          }
          if (!success) {
            LOG.warn("GCRunner delete file failed," + " file: " +  file);
          }
        }
      }
    }
  }

  public long getProtocolVersion(String protocol, long clientVersion) {
    return BlockCacheProtocol.versionID;
  }

  public void start() {
    this.rpcListner.start();
    this.gcRunner.start();
  }

  public void shutdown() {
    if (rpcListner != null) {
      rpcListner.stop();
    }

    for (Map.Entry<String, DFSInputStream> in : streamCache.entrySet()) {
      in.close();
    }
  }

  public void join() {
    try {
      this.server.join();
    }
    catch (InterruptedException ie) {
    }
  }

  /****************************************************************************
   * BlockCacheProtocol interface
   * *************************************************************************/

  /**
   * Returns the cached block instance.
   *
   * Repeat the same thing as an DFSClient object would do
   * otherthan write a block into a local file.
   */
  CachedBlock getCachedBlock(String src, long pos) throws IOException {
    //see if the DFSInputStream already cached
    DFSinputStream remoteIn = streamCache.get(src);
    if (remoteIn == null) {
      remoteIn = new DFSInputStream(src, 
          conf.getInt("io.file.buffer.size", 4096), true);
      streamCache.put(src, remoteIn);
    }
    //get the block information
    LocatedBlock blk = remoteIn.getBlockAt(pos, false);
    CachedBlock block = new CachedBlock(src, blk.getStartOffset(),
        blk.getBlockSize(), "NOT_ASSIGNED_YET");
    //try to get block from cached
    TimedCachedBlock cachedValue;
    boolean shouldConstuct = false;
    synchronized(cachedBlocks) {
      cachedValue = cachedBlocks.get(block);
      //if does not exist
      if (cachedValue == null) {
        cachedValue = new TimedCachedBlock(block, UNDER_CONSTRUCTION);
        cachedBlocks.put(block, cachedValue);
        shouldConstuct = true;
      }
      //exist 
      else {
        long lastCacheTime = cachedValue.getTimestamp();
        //not underconstruction
        if (lastCacheTime != UNDER_CONSTRUCTION) {
          return cachedValue.getBlock();
        }
      }
    }

    //here we have not got the cached block yet
    //either wait or construct and notify
    if (!shouldConstuct) {
      synchronized(cachedValue) {
        cachedValue.wait();
      }
      //check if construction success
      if (cachedValue.getTimestamp == FAILED_CONSTRUCTION) {
        //previous attempt to cache failed
        //do not try again, just raise exception
        LOG.warn("Failed Cache Attempt for" + 
            " src: " + src + 
            " pos: " + pos +
            " block start: " + blk.getStartOffset() + 
            " block length: " + blk.getBlockSize());
        throw IOException("Cache attempt failed");
      }
      assert (cachedValue.getBlock().getLocalPath() 
          != "NOT_ASSIGNED_YET") : "Cache Local Path unassigned";
    }
    else {
      //cache block to file
      block = cachedValue.getBlock();
      FileOutputStream out = new FileOutputStream(createBlockPath(block));
      byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
      long bytesRead = 0;
      int n = 0;
      remoteIn.seek(block.getStartOffset());
      while (bytesRead < block.getBlockLength()) {
        n = remoteIn.read(buffer, 0, buffer.length);
        out.write(buffer, 0, n);
        bytesRead += n;
      }
      out.close();
      long timestamp = System.currentTimeMillis();
      String path = updateBlockPath(block, timestamp);
      //update the value and notify
      synchronized(cachedValue) {
        cachedValue.getBlock().setLocalPath(path);
        cachedValue.setTimestamp(timestamp);
        cachedValue.notifyAll();
      }
      LOG.info("Cache block for" + 
          "src: " + src +
          "start: " + blk.getStartOffset() +
          "pos: " + pos + 
          "length: " + blk.getBlockSize() + 
          "at: " + path);
      return cachedValue.getBlock();
    }
    //should not be here
    assert 0 : "Unexpected control flow point";
  }

}
