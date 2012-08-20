package org.apache.hadoop.blockcache;


import java.io.*;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.util.StringUtils;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;

import org.apache.hadoop.blockcache.BlockCacheProtocol.Block;

public class BlockCacheServer implements BlockCacheProtocol, Runnable {

  private static final Log LOG = LogFactory.getLog(BlockCacheServer.class);

  //configuration strings
  private static final String SERVER_PORT = "block.cache.server.port";
  private static final String SERVER_NHANDLEER = "block.cache.server.num.handler";
  private static final String CACHE_CAPACITY = "block.cache.capacity";
  private static final String LOCAL_DIR = "block.cache.local.dir";

  //default values
  private static final String LOCAL_HOST = "127.0.0.1";
  private static final int DEFAULT_BUFFER_SIZE = 4096;
  private static final long DEFAULT_CACHE_CAPACITY = 1024 * 1024 * 1024; //1G
  enum BlockCachingState {
    DONE,
    UNDER_CONSTRUCTION,
    FAILED
  }

  //states
  private boolean shouldRun = true;
  private long cacheSizePerUserFS;
  private String localCacheDir;
  private Cache cache;
  private CacheFileFreeStore freeStore;
  private Server rpcListener;

  //server
  
  /**
   * A hierarchical cache of data
   * Levels:
   * Token    : caches FileSystem
   * +Path    : caches FSInputStream
   * ++Block  : caches Blocks
   */
  class Cache {

    private Map<String, FileSystem> fsCache; // token -> FS
    private Map<String, FSInputStream> streamCache; // (token, path) -> FSInputStream
    private Map<String, CachedBlocks> blockCache; //token -> CachedBlocks

    Cache() {
      fsCache = new synchronizedMap(
          new HashMap<String, FileSystem>());
      streamCache = new synchronizedMap(
          new HashMap<String, FSInputStream>());
      blockCache = new synchronizedMap(
          new HashMap<String, CachedBlocks>());
    }

    private static String combineKey(Token token, String path) {
      return token.toString() + "/" + path;
    }
    
    void put(Token token, FileSystem fs) {
      FSCache.put(token, fs);
    }

    FileSystem get(Token token) {
      return FSCache.get(token);
    }

    void put(Token token, String path, FSInputStream in) {
      StreamCache.put(combineKey(token, path), in);
    }

    void get(Token token, String path) {
      StreamCache.get(combineKey(token, path));
    }

    /**
     * Put a block into the blockCache
     *
     * @param token userName and FS of the block
     * @param path path of the block
     * @param startOffset startOffset of the block
     * @param length length of the block
     * @return true if successful, false if block already there.
     */
    boolean put(Token token, String path, 
                long startOffset, long length,
                boolean useReplica) {
      CachedBlocks blocks;
      synchronized(blockCache) {
        blocks = blockCache.get(token);
        if (blocks == null) {
          blocks = new CachedBlocks(cacheSizePerUserFS);
          blockCache.put(token, blocks);
        }
      }
      return blocks.put(path, startOffset, length, useReplica);
    }

    Block get(Token token, String path, long pos) {
      synchronized(blockCache) {
        CachedBlocks blocks = blockCache.get(token);
        if (blocks == null) {
          blocks = new CachedBlocks(cacheSizePerUserFS);
          blockCache.put(token, blocks);
          return null;
        }
      }
      return blocks.get(path, pos);
    }

    /**
     * Cached blocks with a limited size for a user, filesystem combination.
     */
    static class CachedBlocks {

      private Map<String, List<Block>> fileBlockLists; //file -> blocks
      private LinkedList<Block> blockQueue; //fifo queue for remove
      private long capacity = 0;
      private long currSize = 0;

      CachedBlocks(long capacity) {
        fileBlockLists = new HashMap<String, List<Block>>();
        blockQueue = new LinkedList<Block>();
        this.capacity = capacity;
      }

      private static int findBlock(List<Block> list, Block key) {
        Comparator<Block> comp = 
            new Comparator<Block>() {
              //Returns 0 iff a is inside b or b is inside a
              public int compare(Block a, Block b) {
                long aBeg = a.getStartOffset();
                long bBeg = b.getStartOffset();
                long aEnd = aBeg + a.getLength();
                long bEnd = bBeg + b.getLength();
                if ((aBeg <= bBeg && bEnd <= aEnd) ||
                    (bBeg <= aBeg && aEnd <= bEnd))
                  return 0;
                if (aBeg < bBeg)
                  return -1;
                return 1;
              }
            }
        return Colloections.binarySearch(list, key, comp);
      }

      private static int getInsertIndex(int binSearchResult) {
        return binSearchResult >= 0 
            ?  binSearchResult : -(binSearchResult + 1);
      }

      synchronized boolean put(String path, long startOffset, 
                               long length, boolean useReplica) {
        if (currSize + length > capacity)
          removeEldestBlocks(length);
        Block key = new Block(path, startOffset, length, "", useReplica);
        List<Block> blocks = fileBlockLists.get(path);
        if (blocks == null) {
          blocks = new LinkedList<Block>();
          fileBlockLists.put(path, blocks);
        }
        int targetIndex = findBlock(blocks, key);
        if (targetIndex >= 0) {
          //already there no need to cache
          return false;
        }
        targetIndex = getInsertIndex(targetIndex);
        blocks.add(targetIndex, key);
        blockQueue.addLast(key);
        currSize += length;
      }

      //assuming we have the object lock
      private void removeEldestBlocks(long length) {
        long sizeToRemove = currSize + length - capacity;
        if (length > capacity)
          LOG.error("try to cache a block larger than capacity");
        while ((sizeToRemove > 0) && (!blockQueue.isEmpty())) {
          Block toRemove = blockQueue.pollFirst();
          String path = toRemove.getFileName();
          if (!removeFromBlockLists(path, toRemove)) {
            LOG.error("inconsistency in CachedBlocks");
          }
          freeStore.add(toRemove);
          sizeToRemove -= toRemove.getLength();
        }
      }

      /**
       * Return false if block not in fileBlockLists
       */
      boolean removeFromBlockLists(String path, long pos) {
        Block key = new Block(path, pos, 1, "", false);
        return removeFromBlockLists(path, key);
      }

      boolean removeFromBlockLists(String path, Block key) {
        List<Block> blocks = fileBlockLists.get(path);
        if (blocks == null) 
          return false;
        int targetIndex = findBlock(blocks, toRemove);
        if (targetIndex < 0) 
          return false;
        blocks.remove(targetIndex);
        return true;
      }

      synchronized Block get(Token token, String path, long pos) {
        Block key = new Block(path, pos, 1, "", false);
        int targetIndex = findBlock(blocks, toRemove);
        if (targetIndex < 0) return null;
        return fileBlockLists.get(path).get(targetIndex);
      }
    }
  }

  /**
   * Garbage collector for local path cache files.
   */
  class CacheFileFreeStore implements Runnable {

    private int NUM_BLOCK_THRESHOLD = 2;
    private List<Block> deleteList = new LinkedLIst<Block>();

    void add(Block b) {
      synchronized(deleteList) {
        deleteList.add(b);
        if (deleteList.size() > NUM_BLOCK_THRESHOLD) {
          deleteList.notify();
        }
      }
    }
    
    void run(Block b) {
      while(true) {
        synchronized(deleteList) {
          if (!deleteList.empty()) {
            for (Block b : deleteList) {
              String file = b.getLocalPath();
              boolean success = (new File(file)).delete();
              if (!success) {
                LOG.warn("CacheFileFreeStore delete file failed," + 
                         " file: " +  file);
              }
            }
          }
          else {
            try {
              deleteList.wait();
            }
            catch(InterruptedException e) {
            }
          }
        }
      }
    }
  }

}

public class BlockCacheServer extends DFSClient 
  implements BlockCacheProtocol, Runnable {

  private static final String MMAP_CACHED_FILE = "mmap.cached.file";
  private static final String GC_CAP_THR = "block.cache.gc.capacity.threshold";
  private static final String GC_TIME_THR = "block.cache.gc.time.threshold";

  private static final int DEFAULT_CACHE_CAPACITY = 
    (int)(1024 * 1024 * 1024 / DEFAULT_BLOCK_SIZE); // 1G / blockSize
  private static final long UNDER_CONSTRUCTION = -1;
  private static final long FAILED_CONSTRUCTION = 0;

  private boolean shouldRun = true;
  private Map<String, DFSInputStream> streamCache; //file -> DFSInputStream
  private Map<CachedBlock, TimedCachedBlock> cachedBlocks; 
  private Server rpcListener;
  private int cacheCapacity;
  private boolean mmapCachedFile = false;
  private String localCacheDir;
  private int gcCapacityThreadshold;
  private long gcTimeThreshold;
  private Thread gcRunner;
  private Thread serverThread;

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

  public void createServer(Configuration conf) throws IOException {
    //cache opened DFSInputStream to reduce access to namenode.
    final int cacheSize = 100;
    final float hashTableLoadFactor = 0.75f;
    int hashTableCapacity = (int) Math.ceil(cacheSize / hashTableLoadFactor) + 1;
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
      });

    //cachedBlocks
    this.cacheCapacity = conf.getInt(CACHE_CAPACITY, DEFAULT_CACHE_CAPACITY);
    this.cachedBlocks = new HashMap<CachedBlock, TimedCachedBlock>(cacheCapacity);

    //rpc server
    int port = conf.getInt(SERVER_PORT, 
        BlockCacheProtocol.DEFAULT_SERVER_PORT);
    int numHandler = conf.getInt(SERVER_NHANDLEER, 3);
    this.rpcListener = RPC.getServer(this, LOCAL_HOST, port, 
        numHandler, false, conf);

    //local dir
    this.localCacheDir = conf.get(LOCAL_DIR, "/tmp/hadoop-xyu40/dfs/blockcache");
    File path = new File(localCacheDir);
    boolean success = true;
    if (path.exists()) {
      deleteDirFiles(path);
    }
    else {
      LOG.info("Making block cacahe dir at " + this.localCacheDir);
      success = path.mkdirs();
    }
    if (!success) {
      throw new IOException("Cannot set local cache dir at " + 
          this.localCacheDir);
    }

    //gc
    this.gcCapacityThreadshold = (int) Math.ceil(conf.getFloat(GC_CAP_THR, 0.75f) * 
      cacheCapacity) + 1;
    this.gcTimeThreshold = conf.getLong(GC_TIME_THR, 5000);
    this.gcRunner = new Thread(new GCRunner());
    this.gcRunner.setDaemon(true);

    LOG.info("Block Cache Server instance" +
        ", port: " + port +
        ", cache block capacity: " + cacheCapacity +
        ", local cache dir: " + localCacheDir + 
        ", gc capacity threshold: " + gcCapacityThreadshold + 
        ", gc time threshold: " + gcTimeThreshold);
  }

  private void deleteDirFiles(File dir) {
    String[] files = dir.list();
    for (String file : files) {
      LOG.info("delete file: " + file);
      (new File(localCacheDir + "/" + file)).delete();
    }
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
        try {
          Thread.sleep(gcTimeThreshold / 2);
        }
        catch (InterruptedException ie) {
        }
        long curr = System.currentTimeMillis();
        synchronized(cachedBlocks) {
          if (cachedBlocks.size() < gcCapacityThreadshold) {
            continue;
          }
          for (Iterator< Map.Entry<CachedBlock, TimedCachedBlock> >
              it = cachedBlocks.entrySet().iterator(); it.hasNext();) {
            Map.Entry<CachedBlock, TimedCachedBlock> entry = it.next();
            long lastUse = entry.getValue().getTimestamp();
            if (lastUse == UNDER_CONSTRUCTION) {
              continue;
            }
            if (curr - lastUse > gcTimeThreshold) {
              toDelete.add(entry.getKey().getLocalPath());
              it.remove();

              LOG.debug("Add to delete list:" + 
                  "file: " + entry.getValue().getBlock().getLocalPath() + 
                  "time: " + lastUse);
            }
          }
        }
        LOG.debug("Deleting files");
        for (String file : toDelete) {
          LOG.info("Deleting file " + file);
          boolean success = (new File(file)).delete();
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

  //No matter what exception we get, keep running
  public void run() {
    while(shouldRun) {
      try {
        this.rpcListener.start();
        this.gcRunner.start();
        join();
      }
      catch (Exception e) {
        LOG.error("Exception: " + StringUtils.stringifyException(e));
        try {
          Thread.sleep(5000);
        }
        catch (InterruptedException ie) {
        }
      }
    }
  }

  public static void startServer(BlockCacheServer s) {
    s.serverThread = new Thread(s);
    s.serverThread.setDaemon(true);
    s.serverThread.start();
  }

  public void shutdown() {
    shouldRun = false;
    if (rpcListener != null) {
      rpcListener.stop();
    }

    try {
      for (Map.Entry<String, DFSInputStream> entry : streamCache.entrySet()) {
        entry.getValue().close();
      }
    }
    catch (IOException e){
    }
  }

  //wait for the server to finish
  //normally it runs forever
  public void join() {
    try {
      this.rpcListener.join();
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
  public CachedBlock getCachedBlock(String src, long pos) throws IOException {
    LOG.debug("Requiring Cache on" + " src: " + src + " pos: " + pos);
    //see if cache is full
    synchronized(cachedBlocks) {
      if (cachedBlocks.size() > cacheCapacity) {
        throw new IOException("Block Cache Full");
      }
    }

    //see if the DFSInputStream already cached
    DFSInputStream remoteIn = streamCache.get(src);
    if (remoteIn == null) {
      remoteIn = open(src);
      streamCache.put(src, remoteIn);
    }
    //get the block information
    LocatedBlock blk = remoteIn.getBlockAtPublic(pos);
    CachedBlock block = new CachedBlock(src, blk.getStartOffset(),
        blk.getBlockSize(), "NOT_ASSIGNED_YET", 0);
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
          LOG.debug("Block" + " src: " + src + " pos: " + pos +
              " exists");
          return cachedValue.getBlock();
        }
      }
    }

    //here we have not got the cached block yet
    //either wait or construct and notify
    if (!shouldConstuct) {
      synchronized(cachedValue) {
        try {
          while (cachedValue.getTimestamp() == UNDER_CONSTRUCTION) {
            cachedValue.wait();
          }
        }
        catch (InterruptedException e) {
        }
      }
      //check if construction success
      if (cachedValue.getTimestamp() == FAILED_CONSTRUCTION) {
        //previous attempt to cache failed
        //do not try again, just raise exception
        LOG.warn("Failed Waked up from waiting on" + 
            " src: " + src + 
            " pos: " + pos +
            " block start: " + blk.getStartOffset() + 
            " block length: " + blk.getBlockSize());
        throw new IOException("Cache remote read failed");
      }
      assert (cachedValue.getBlock().getLocalPath() 
          != "NOT_ASSIGNED_YET") : "Cache Local Path unassigned";
    }
    else {
      try {
        //cache block to file
        block = cachedValue.getBlock();
        FileOutputStream out;
        try {
          out = new FileOutputStream(createBlockPath(block));
        }
        catch (IOException e) {
          LOG.warn("Cannot create file: " + createBlockPath(block));
          throw new IOException("Cache local file operation fail");
        }
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
        //update the value
        cachedValue.getBlock().setLocalPath(path);
        cachedValue.setTimestamp(timestamp);
        LOG.info("Cache block for" + 
            " src: " + src +
            " start: " + blk.getStartOffset() +
            " pos: " + pos + 
            " length: " + blk.getBlockSize() + 
            " at: " + path);
      }
      finally {
        //always wake up waiting threads.
        synchronized(cachedValue) {
          cachedValue.notifyAll();
        }
      }
      return cachedValue.getBlock();
    }
    //should not be here
    assert false : "Unexpected control flow point";
    return cachedValue.getBlock();
  }

  private String createBlockPath(CachedBlock block) {
    String fileName = block.getFileName().replace('/', '@');
    return localCacheDir + "/" + fileName + "@" +
      block.getStartOffset();
  }

  private String updateBlockPath(CachedBlock block, long timestamp) 
    throws IOException {
    //file should exist
    String oldName = createBlockPath(block);
    File oldFile = new File(oldName);
    String newName = oldName + "@" + timestamp;
    File newFile = new File(newName);
    boolean success = oldFile.renameTo(newFile);
    if (!success) {
      LOG.warn("Rename fail" + " from: " + oldName + " to: " + newName);
      throw new IOException("Cache local file operation fail");
    }
    return newName;
  }
}
