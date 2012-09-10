package org.apache.hadoop.blockcache;


import java.io.*;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.BlockLocation;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.ipc.RPC;
import org.apache.hadoop.ipc.RPC.Server;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.fs.Segment;
import org.apache.hadoop.fs.Segments;

public class BlockCacheServer implements BlockCacheProtocol, Runnable {

  private static final Log LOG = LogFactory.getLog(BlockCacheServer.class);

  //configuration strings
  private static final String SERVER_PORT = "block.cache.server.port";
  private static final String SERVER_NHANDLEER = "block.cache.server.num.handler";
  private static final String DISK_CACHE_CAPACITY = "block.cache.disk.capacity.per.user";
  private static final String MEM_CACHE_CAPACITY = "block.cache.memory.capacity.per.user";
  private static final String LOCAL_DIR = "block.cache.local.dir";

  //default values
  private static final String LOCAL_HOST = "127.0.0.1";
  private static final int DEFAULT_BUFFER_SIZE = 4096;
  private static final long DEFAULT_CACHE_CAPACITY = 1024 * 1024 * 1024; //1G
  private static final String DEFAULT_CACHE_DIR = "/tmp/hadoop/blockcache";
  private static final long PREFETCH_SIZE = 10 * 64 * 1024 * 1024; //10 * 64M

  //default configuration
  private Configuration conf;

  //states
  private boolean shouldRun = true;
  private long diskCacheSizePerUser;
  private long memCacheSizePerUser;
  private String localCacheDir;
  private Cache cache;
  private CacheFileFreeStore freeStore = new CacheFileFreeStore();
  private BlockCacheStatus prevStatus = new BlockCacheStatus();

  //service
  private Server rpcListener;
  private Thread freeStoreThread;

  public BlockCacheServer(Configuration conf) throws IOException {
    this.conf = conf;
    //states
    diskCacheSizePerUser = conf.getLong(DISK_CACHE_CAPACITY, DEFAULT_CACHE_CAPACITY);
    memCacheSizePerUser = conf.getLong(MEM_CACHE_CAPACITY, DEFAULT_CACHE_CAPACITY);
    localCacheDir = conf.get(LOCAL_DIR, DEFAULT_CACHE_DIR);
    File path = new File(localCacheDir);
    if (path.exists()) {
      deleteDirFiles(path);
    }
    LOG.info("Making block cacahe dir at " + localCacheDir);
    if (!path.mkdirs()) {
      throw new IOException(
          "Cannot set local cache dir at " + localCacheDir);
    }

    cache = new Cache();
    freeStoreThread = new Thread(freeStore);
    freeStoreThread.setDaemon(true);
    freeStoreThread.start();

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
      File f = new File(dir, file);
      if (f.isFile()) {
        LOG.info("delete file: " + file);
        f.delete();
      }
      else {
        deleteDirFiles(f);
      }
    }
    dir.delete();
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
    try {
      shouldRun = false;
      if (rpcListener != null) {
        rpcListener.stop();
      }
      freeStoreThread.join();
      deleteDirFiles(new File(localCacheDir));
      cache.cleanup();
    }
    catch (Throwable e) {
      LOG.error("Shutting down error: " + 
                StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }

  /**
   * Path information for cache
   */
  class PathInfo {
    FileSystem fs;
    FileStatus status;
    FSDataInputStream in;
    List<BlockLocation> blocks;

    PathInfo(FileSystem fs, Path path) throws IOException {
      this.fs = fs;
      status = fs.getFileStatus(path);
      in = fs.open(path);
      blocks = new ArrayList<BlockLocation>(
          Arrays.asList(
              fs.getFileBlockLocations(status, 0, PREFETCH_SIZE)));
    }

    BlockLocation getLocation(long off) throws IOException {
      int index = findBlock(blocks, off);
      //already cached
      if (index >= 0) return blocks.get(index);
      //cache with prefetch
      index = -(index + 1);
      List<BlockLocation> newBlocks = new ArrayList<BlockLocation>(
          Arrays.asList(
              fs.getFileBlockLocations(status, off, PREFETCH_SIZE)));
      int oldIdx = index;
      int insStart = 0, insEnd = 0;
      for (int newIdx = 0; newIdx < newBlocks.size() && oldIdx < blocks.size();
           newIdx ++) {
        long newOff = newBlocks.get(newIdx).getOffset();
        long oldOff = blocks.get(oldIdx).getOffset();
        if (newOff < oldOff) {
          insEnd ++;
        }
        else if (newOff == oldOff) {
          blocks.set(oldIdx, newBlocks.get(newIdx));
          if (insStart < insEnd) {
            blocks.addAll(oldIdx, newBlocks.subList(insStart, insEnd));
            oldIdx += insEnd - insStart;
          }
          insStart = insEnd = newIdx + 1;
          oldIdx ++;
        }
        else {
          assert false : " List of block must be sorted by offset";
        }
      }
      insEnd = newBlocks.size();
      if (insStart < insEnd) {
        blocks.addAll(oldIdx, newBlocks.subList(insStart, insEnd));
      }
      return newBlocks.get(0);
    }

    private int findBlock(List<BlockLocation> list, long off) {
      BlockLocation key = new BlockLocation(null, null, off, 1);
      Comparator<BlockLocation> comp = 
          new Comparator<BlockLocation>() {
            //Returns 0 iff a is inside b or b is inside a
            public int compare(BlockLocation a, BlockLocation b) {
              long aBeg = a.getOffset();
              long bBeg = b.getOffset();
              long aEnd = aBeg + a.getLength();
              long bEnd = bBeg + b.getLength();
              if ((aBeg <= bBeg && bEnd <= aEnd) ||
                  (bBeg <= aBeg && aEnd <= bEnd))
                return 0;
              if (aBeg < bBeg)
                return -1;
              return 1;
            }
          };
      return Collections.binarySearch(list, key, comp);
    }

  }

  /**
   * A multiple key cache
   * URI: FileSystem
   * Path: caches FSInputStream
   * User: caches Blocks
   */
  class Cache {

    private Map<URI, FileSystem> fsCache;
    private Map<Path, PathInfo> pathCache; 
    private Map<String, CachedBlocks> blockCache; 

    Cache() {
      fsCache = new HashMap<URI, FileSystem>();
      pathCache = Collections.synchronizedMap(
          new HashMap<Path, PathInfo>());
      blockCache = Collections.synchronizedMap(
          new HashMap<String, CachedBlocks>());
    }

    //pathinfo key
    void put(URI fsUri, Path path) throws IOException {
      FileSystem fs;
      synchronized(fsCache) {
        fs = fsCache.get(fsUri);
        if (fs == null) {
          fs = FileSystem.get(fsUri, conf);
          fsCache.put(fsUri, fs);
        }
      }
      synchronized(pathCache) {
        if (pathCache.get(path) == null) {
          pathCache.put(path, new PathInfo(fs, path));
        }
      }
    }

    //pathinfo key
    PathInfo get(Path path) {
      return pathCache.get(path);
    }

    /**
     * Put a block key value pair into the blockCache
     *
     * @param user userName
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
          blocks = new CachedBlocks(diskCacheSizePerUser);
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
          return null;
        }
        return blocks.get(path, pos);
      }
    }

    void remove(String user, Block b) {
      pathCache.remove(b.getPath());
      synchronized(blockCache) {
        CachedBlocks blocks = blockCache.get(user);
        if (blocks == null) return;
        blocks.remove(b);
      }
    }

    Segments getSegments(String user) {
      synchronized(blockCache) {
        CachedBlocks blocks = blockCache.get(user);
        if (blocks == null) return new Segments(
            new ArrayList<Segment>());
        return blocks.getCachedSegments();
      }
    }

    /**
     * Cached blocks with a limited size for a user, filesystem combination.
     */
    class CachedBlocks {

      private Map<Path, List<Block>> fileBlockLists; //file -> blocks
      private LinkedList<Block> blockQueue; //fifo queue for remove
      private long capacity = 0;
      private long currSize = 0;

      CachedBlocks(long capacity) {
        fileBlockLists = new HashMap<Path, List<Block>>();
        blockQueue = new LinkedList<Block>();
        this.capacity = capacity;
      }

      //We can assume that the segments are non-overlaping becuase
      //this method is being called after a fs block is cached. The fs should
      //have that guarantee.
      synchronized boolean put(Path path, long startOffset, 
                               long length, boolean useReplica) {
        if (currSize + length > capacity)
          removeEldestBlocks(length);
        Block key = new Block(path, startOffset, length, useReplica, "");
        List<Block> blocks = fileBlockLists.get(path);
        if (blocks == null) {
          blocks = new LinkedList<Block>();
          fileBlockLists.put(path, blocks);
        }
        //use the natural comparetor
        int targetIndex = Collections.binarySearch(blocks, key);
        if (targetIndex >= 0) {
          //already there no need to cache
          return false;
        }
        targetIndex = -(targetIndex + 1);
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
          currSize -= toRemove.getLength();
        }
      }

      public void remove(Block b) {
        if (b == null) return;
        blockQueue.remove(b);
        Path path = b.getPath();
        if (!removeFromBlockLists(path, b)) {
          LOG.error("inconsistency in CachedBlocks");
        }
        freeStore.add(b);
        currSize -= b.getLength();
      }

      /**
       * Return false if block not in fileBlockLists
       */
      boolean removeFromBlockLists(Path path, Block key) {
        List<Block> blocks = fileBlockLists.get(path);
        if (blocks == null) 
          return false;
        int targetIndex = Collections.binarySearch(blocks, key);
        if (targetIndex < 0) 
          return false;
        blocks.remove(targetIndex);
        return true;
      }

      synchronized Block get(Path path, long pos) {
        Block key = new Block(path, pos, 1, false, "");
        List<Block> blocks = fileBlockLists.get(path);
        if (blocks == null) {
          blocks = new LinkedList<Block>();
          fileBlockLists.put(path, blocks);
          return null;
        }
        int targetIndex = Collections.binarySearch(blocks, key);
        //targetIndex is less than zero for sure.
        // empty list return null
        if (targetIndex == -1) 
          if (blocks.size() == 0)
            return null;
        // search for the block
        targetIndex = -(targetIndex + 1);
        if (targetIndex == blocks.size()) targetIndex = blocks.size() - 1;
        Block cached = blocks.get(targetIndex);
        long start = cached.getOffset();
        long end = start + cached.getLength();
        if (start <= pos && pos < end) return cached;
        else return null;
      }

      synchronized Segments getCachedSegments() {
        Segment[] segs = new Segment[blockQueue.size()];
        for (int i = 0; i < segs.length; ++i) {
          segs[i] = blockQueue.get(i).getSegment();
        }
        return new Segments(segs);
      }
    }

    //clean up cached resources
    public void cleanup() throws IOException {
      for (PathInfo info : pathCache.values()) {
        info.in.close();
      }
    }
  }

  /**
   * Garbage collector for local path cache files.
   */
  class CacheFileFreeStore implements Runnable {

    private int NUM_BLOCK_THRESHOLD = 10;
    private List<Block> deleteList = new LinkedList<Block>();

    void add(Block b) {
      synchronized(deleteList) {
        LOG.info("Deleting block: " + b.getSegment().toString());
        deleteList.add(b);
        if (deleteList.size() > NUM_BLOCK_THRESHOLD) {
          LOG.info("Delete process wake up.");
          deleteList.notify();
        }
      }
    }
    
    public void run() {
      while(shouldRun) {
        synchronized(deleteList) {
          if (!deleteList.isEmpty()) {
            LOG.info("Free Store Cleaning up");
            for (Block b : deleteList) {
              String file = b.getLocalPath();
              if (file != "") {
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
            deleteList.clear();
          }
          else {
            try {
              deleteList.wait(10000);
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

  public Block cacheBlockAt(String fsUriStr, String user, String pathStr, 
                            long versionId, long pos) throws IOException {
    LOG.debug("Caching block at" + 
              " user: " + user + 
              " fs: " + fsUriStr + 
              " path: " + pathStr + 
              " pos: " + pos);
    //normalize a path
    URI fsUri, pathUri;
    try {
      fsUri = new URI(fsUriStr);
      pathUri = new URI(pathStr);
    }
    catch (URISyntaxException e) {
      LOG.error("URISyntaxException: " + 
                StringUtils.stringifyException(e));
      throw new IOException("URISyntaxException");
    }
    Path path = new Path(fsUri.getScheme(), fsUri.getAuthority(),
                         pathUri.getPath());
    //search cache
    Block block = cache.get(user, path, pos);
    //already cached
    if (block != null) {
      if (block.getVersion() == versionId) {
        LOG.debug("Block already exists with version: " + versionId);
        return waitOrConstructBlock(false, block, null, user, versionId);
      }
      else {
        LOG.info("Block with staled version: " + block.getVersion() + 
                  " required version: " + versionId + 
                  ". Removing block at: " + 
                  " user: " + user + 
                  " path: " + block.getPath() + 
                  " off: " + block.getOffset() + 
                  " len: " + block.getLength());
        //remove that block
        cache.remove(user, block);
      }
    }
    //not cached, try to cache it
    PathInfo info = cache.get(path);
    if (info == null) {
      cache.put(fsUri, path);
      info = cache.get(path);
    }
    FileStatus file = info.status;
    //deal with eof
    if (pos >= file.getLen()) 
      return new Block(path, -1, -1, false, "");
    LOG.debug("Caching file size: " + file.getLen());
    //try to see if the block can be local
    //if local just return the block
    BlockLocation loc = info.getLocation(pos);
    String[] names = loc.getNames();
    String host = "";
    if (names.length > 0) host = names[0];
    LOG.debug("File location: " + host);
    InetSocketAddress bestAddr = NetUtils.createSocketAddr(host);
    boolean isLocal = isLocalAddress(bestAddr);
    //put into cache
    long off = loc.getOffset();
    long len = loc.getLength();
    boolean first = cache.put(user, path, off, len, isLocal);
    LOG.info("Cached block at: " + 
              " user: " + user + 
              " path: " + path + 
              " off: " + off + 
              " len: " + len + 
              " isLocal: " + isLocal);
    block = cache.get(user, path, off);
    block.setVersion(versionId);
    if (isLocal) return block;
    //caching remote block
    block = waitOrConstructBlock(first, block, info, user, versionId);
    block.setVersion(versionId);
    return block;
  }

  private Block waitOrConstructBlock(
      boolean first, Block block, PathInfo info, 
      String user, long versionId) throws IOException{
    if (block == null) {
      LOG.error(cache.getSegments(user));
      throw new IOException("block null");
    }
    //if not local and we are the first to put it there we are responsible to
    //cache it.
    long off = block.getOffset();
    long len = block.getLength();
    if (first) {
      synchronized(block) {
        String localPath = createLocalPath(user, block, versionId);
        FileOutputStream out;
        try {
          out = new FileOutputStream(localPath);
        }
        catch (IOException e) {
          LOG.warn("Cannot create file: " + localPath);
          block.setLocalPath("/CONSTRUCTION_FAILED");
          throw new IOException("Cache local file operation fail");
        }
        byte[] buffer = new byte[DEFAULT_BUFFER_SIZE];
        long bytesRead = 0;
        int n = 0;
        info.in.seek(off);
        while(bytesRead < len) {
          n = info.in.read(buffer);
          out.write(buffer, 0, n);
          bytesRead += n;
        }
        out.close();

        block.setLocalPath(localPath);
        block.notifyAll();
      }
      LOG.info("Block constructed at" + 
                " local path: " + block.getLocalPath());
      return block;
    }
    else {
      if (block.shouldUseReplica()) return block;
      synchronized(block) {
        try {
          while(block.getLocalPath().equals("")) {
            block.wait();
            if (block.getLocalPath().equals(
                    "/CONSTRUCTION_FAILED")) break;
          }
        }
        catch (InterruptedException e) {
        }
      }
      //check if construction suceeded.
      if (block.getLocalPath().equals(
              "/CONSTRUCTION_FAILED")) {
        LOG.warn("Failed Waked up from waiting on" + 
                 " block: " + block.toString());
        throw new IOException("Cache remote read failed");
      }
      return block;
    }
  }

  private static Set<String> localIpAddresses = 
      Collections.synchronizedSet(new HashSet<String>());

  private static boolean isLocalAddress(InetSocketAddress targetAddr) {
    InetAddress addr = targetAddr.getAddress();
    if (localIpAddresses.contains(addr.getHostAddress())) {
      if (LOG.isTraceEnabled()) {
        LOG.trace("Address " + targetAddr + " is local");
      }
      return true;
    }

    // Check if the address is any local or loop back
    boolean local = addr.isAnyLocalAddress() || addr.isLoopbackAddress();

    // Check if the address is defined on any interface
    if (!local) {
      try {
        local = NetworkInterface.getByInetAddress(addr) != null;
      } catch (SocketException e) {
        local = false;
      }
    }
    if (LOG.isTraceEnabled()) {
      LOG.trace("Address " + targetAddr + " is local");
    }
    if (local == true) {
      localIpAddresses.add(addr.getHostAddress());
    }
    return local;
  }

  private String createLocalPath(String user, Block block, long versionId) 
      throws IOException {
    File dir = new File(localCacheDir, user);
    if (!dir.exists()) {
      if (!dir.mkdir()) {
        throw new IOException("cannot create directory: " + 
                              dir.toString());
      }
    }
    String name = block.toString().replace('/', '#');
    name = name + "#" + versionId;
    return localCacheDir + "/" + user + "/" + name;
  }

  public BlockCacheStatus getStatus(String user) throws IOException {
    BlockCacheStatus status = new BlockCacheStatus(cache.getSegments(user),
                                                   diskCacheSizePerUser,
                                                   memCacheSizePerUser);
    if (!status.equals(prevStatus)) {
      prevStatus = status;
      LOG.info("Status changed: " + prevStatus);
    }
    return status;
  }

  public static void main(String argv[]) throws Exception {
    StringUtils.startupShutdownMessage(BlockCacheServer.class, argv, LOG);
    if (argv.length != 0) {
      System.out.println("usage: BlockCacheServer");
      System.exit(-1);
    }
    try {
      Configuration conf = new Configuration();
      BlockCacheServer bcs = new BlockCacheServer(conf);
      bcs.run();
    }
    catch (Throwable e) {
      LOG.error("Cannot start block cache server because " + 
                StringUtils.stringifyException(e));
      System.exit(-1);
    }
  }

}

