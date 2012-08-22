package org.apache.hadoop.blockcache;


import java.io.*;
import java.net.InetSocketAddress;
import java.net.URI;
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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.Segment;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.util.StringUtils;

public class BlockCacheServer implements BlockCacheProtocol, Runnable {

  private static final Log LOG = LogFactory.getLog(BlockCacheServer.class);

  //configuration strings
  private static final String SERVER_PORT = "block.cache.server.port";
  private static final String SERVER_NHANDLEER = "block.cache.server.num.handler";
  private static final String CACHE_CAPACITY = "block.cache.capacity.per.user.fs";
  private static final String LOCAL_DIR = "block.cache.local.dir";

  //default values
  private static final String LOCAL_HOST = "127.0.0.1";
  private static final int DEFAULT_BUFFER_SIZE = 4096;
  private static final long DEFAULT_CACHE_CAPACITY = 1024 * 1024 * 1024; //1G
  private static final String DEFAULT_CACHE_DIR = "/tmp/hadoop/blockcache";

  //default configuration
  private static Configuration conf = new Configuration();

  //states
  private boolean shouldRun = true;
  private long cacheSizePerUser;
  private String localCacheDir;
  private Cache cache;
  private CacheFileFreeStore freeStore = new CacheFileFreeStore();

  //service
  private Server rpcListener;
  private Thread freeStoreThread;

  public BlockCacheServer() throws IOException {
    //states
    cacheSizePerUser = conf.getLong(CACHE_CAPACITY, DEFAULT_CACHE_CAPACITY);
    localCacheDir = conf.getString(LOCAL_DIR, DEFAULT_CACHE_DIR);
    File path = new File(localCacheDir);
    if (path.exists()) {
      deleteDirFiles(File dir);
    }
    else {
      LOG.info("Making block cacahe dir at " + localCacheDir);
      if (!path.mkdir()) {
        throw new IOException(
            "Cannot set local cache dir at " + localCacheDir);
      }
    }
    cache = new Cache();
    freeStoreThread = new Thread(freeStore);
    freeStoreThread.setDaemon(true);

    //server
    int port = conf.getInt(
        SERVER_PORT, BlockCacheProtocol.DEFAULT_SERVER_PORT);
    int numHandler = conf.getInt(SERVER_NHANDLEER, 3);
    rpcListener = RPC.getServer(this, LOCAL_HOST, port, numHandler, 
                                false, conf);
  }
  
  private void deleteDirFiles(File dir) {
    String[] files = dir.list();
    for (String file : files) {
      LOG.info("delete file: " + file);
      (new File(localCacheDir + "/" + file)).delete();
    }
  }

  //No matter what exception we get, keep running
  public void run() {
    while(shouldRun) {
      try {
        rpcListener.start();
        join();
      }
      catch(Exception e) {
        LOG.error("Exception: " + StringUtils.stringifyException(e));
        try {
          Thread.sleep(5000);
        }
        catch (InterruptedException ie) {
        }
      }
    }
  }

  //wait for the server to finish
  //normally it runs forever
  public void join() {
    try {
      rpcListener.join();
    }
    catch (InterruptedException ie) {
    }
  }

  public void shutdown() {
    shouldRun = false;
    if (rpcListener != null) {
      rpcListener.stop();
    }
    freeStoreThread.join();
    deleteDirFiles(new Path(localCacheDir));
  }

  /**
   * A multiple key cache
   * URI: FileSystem
   * Path: caches FSInputStream
   * User: caches Blocks
   */
  class Cache {

    private Map<URI, FileSystem> fsCache; 
    private Map<Path, FSDataInputStream> streamCache; 
    private Map<String, CachedBlocks> blockCache; 

    Cache() {
      fsCache = new synchronizedMap(
          new HashMap<URI, FileSystem>());
      int size = 100;
      streamCache = new synchronizedMap(
          new LinkedHashMap<Path, FSDataInputStream>() {
            @Override
            protected boolean removeEldestEntry(
                Map.Entry<Path, FSDataInputStream> eldest) {
              return size() > size;
            }

          });
      blockCache = new synchronizedMap(
          new HashMap<String, CachedBlocks>());
    }

    //fs key
    void put(URI uri, FileSystem fs) {
      FSCache.put(uri, fs);
    }

    //fs key
    FileSystem get(URI uri) {
      return FSCache.get(uri);
    }

    //stream key
    void put(Path path, FSInputStream in) {
      StreamCache.put(path, in);
    }

    //stream key
    void get(Path path) {
      StreamCache.get(path);
    }

    /**
     * Put a block key value pair into the blockCache
     *
     * @param token userName and FS of the block
     * @param path path of the block
     * @param startOffset startOffset of the block
     * @param length length of the block
     * @return true if successful, false if block already there.
     */
    boolean put(String user, Path path, long startOffset, long length,
                boolean useReplica) {
      CachedBlocks blocks;
      synchronized(blockCache) {
        blocks = blockCache.get(user);
        if (blocks == null) {
          blocks = new CachedBlocks(cacheSizePerUser);
          blockCache.put(user, blocks);
        }
      }
      return blocks.put(path, startOffset, length, useReplica);
    }

    //block key
    Block get(String user, Path path, long pos) {
      synchronized(blockCache) {
        CachedBlocks blocks = blockCache.get(user);
        if (blocks == null) {
          blocks = new CachedBlocks(cacheSizePerUser);
          blockCache.put(user, blocks);
          return null;
        }
      }
      return blocks.get(path, pos);
    }

    /**
     * Cached blocks with a limited size for a user, filesystem combination.
     */
    static class CachedBlocks {

      private Map<Path, List<Block>> fileBlockLists; //file -> blocks
      private LinkedList<Block> blockQueue; //fifo queue for remove
      private long capacity = 0;
      private long currSize = 0;

      CachedBlocks(long capacity) {
        fileBlockLists = new HashMap<Path, List<Block>>();
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

      //We can assume that the segments are non-overlaping becuase
      //this method is being called after a fs block is cached. The fs should
      //have that guarantee.
      synchronized boolean put(Path path, long startOffset, 
                               long length, boolean useReplica) {
        if (currSize + length > capacity)
          removeEldestBlocks(length);
        Block key = new Block(new Segment(path, startOffset, length), 
                              "", useReplica);
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
        return true;
      }

      //assuming we have the object lock
      private void removeEldestBlocks(long length) {
        long sizeToRemove = currSize + length - capacity;
        if (length > capacity)
          LOG.error("try to cache a block larger than capacity");
        while ((sizeToRemove > 0) && (!blockQueue.isEmpty())) {
          Block toRemove = blockQueue.pollFirst();
          Path path = toRemove.getPath();
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
      boolean removeFromBlockLists(Path path, long pos) {
        Block key = new Block(new Segment(path, pos, 1), "", false);
        return removeFromBlockLists(path, key);
      }

      boolean removeFromBlockLists(Path path, Block key) {
        List<Block> blocks = fileBlockLists.get(path);
        if (blocks == null) 
          return false;
        int targetIndex = findBlock(blocks, key);
        if (targetIndex < 0) 
          return false;
        blocks.remove(targetIndex);
        return true;
      }

      synchronized Block get(Path path, long pos) {
        Block key = new Block(new Segment(path, pos, 1), "", false);
        int targetIndex = findBlock(blocks, key);
        if (targetIndex < 0) return null;
        return fileBlockLists.get(path).get(targetIndex);
      }
    }

    //clean up cached resources
    public void cleanup() {
      for (FSDataInputStream in : streamCache.valueSet()) {
        in.close();
      }
    }
  }

  /**
   * Garbage collector for local path cache files.
   */
  class CacheFileFreeStore implements Runnable {

    private int NUM_BLOCK_THRESHOLD = 10;
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
      while(shouldRun) {
        synchronized(deleteList) {
          if (!deleteList.empty()) {
            for (Block b : deleteList) {
              String file = b.getLocalPath();
              boolean success = (new File(file)).delete();
              if (!success) {
                LOG.warn("CacheFileFreeStore delete file failed," + 
                         " file: " +  file);
              }
              else {
                LOG.debug("CacheFileFreeStore delete file succeeded," + 
                         " file: " +  file);
              }
            }
          }
          else {
            try {
              deleteList.wait(1000);
            }
            catch(InterruptedException e) {
            }
          }
        }
      }
    }
  }


  ////////////////////////////////////////////////////
  //The BlockCacheProtocol
  ///////////////////////////////////////////////////
  public long getProtocolVersion(String protocol, long clientVersion) {
    return BlockCacheProtocol.versionID;
  }

  public Block cacheBlockAt(String fsUriStr, String user, 
                            String pathStr, long pos) throws IOException {
      URI fsUri = new URI(fsUri);
      URI pathUri = new URI(path);
      Path path = new Path(fsUri.getScheme(), fsUri.getAuthority(),
                           pathUri.getPath());
      Block block = cache.get(user, path, pos);
      //already cached
      if (block != null) return;
      //not cached, try to cache it
      FSDataInputStream in = cache.get(path);
      FileSystem fs = cache.get(fsUri);
      if (in == null) {
        if (fs == null) {
          fs = FileSystem.get(fsUri, conf);
          cache.put(fsUri, fs);
        }
        in = fs.open(path);
        cache.put(path, in);
      }
      FileStatus file = fs.getFileStatus(src);
      //deal with eof
      if (pos >= file.getLen()) 
        return new Block(new Segment(path, -1, -1), "", false);
      BlockLocation[] blocks = fs.getFileBlockLocations(file, pos, 1);
      //try to see if the block can be local

      block = new Block(new Segment(path, 
                                    blocks[0].getOffset(),
                                    blocks[1].getLength()),
                        "", false);

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

  private volatile boolean shouldRun = true;
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
      freeStoreThread.join();
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
      rpcListener.join();
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
