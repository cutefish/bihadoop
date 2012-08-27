package org.apache.hadoop.blockcache;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.ipc.VersionedProtocol;

/******************************************************************************
 * Protocol that a BlockCacheClient used to contact BlockCacheServer.
 * The BlockCacheServer is the server, that implement this protocol.
 ******************************************************************************/
public interface BlockCacheProtocol extends VersionedProtocol {

  public static final long versionID = 01L;
  public static final int DEFAULT_SERVER_PORT = 60200;

  ////////////////////////////////////////////////////
  //The Protocol
  ///////////////////////////////////////////////////
  /**
   * Returns the Block object containing cache information.
   * 
   * The block contains the position required.
   * Throw IOException if cannot cache.
   */
  public Block cacheBlockAt(String fsUri, String user, String path, long pos) 
      throws IOException;

  /**
   * Returns a list of blocks cached on the server for a user
   */
  public BlockCacheStatus getStatus(String user) throws IOException;

}
