import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.Socket;
import java.net.ServerSocket;
import java.net.SocketTimeoutException;
import java.nio.channels.AsynchronousCloseException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import java.util.logging.Level;
import java.util.logging.Logger;

/* FileCache.java
 * Service used to cache a remote file in the local disk and keep it in the file
 * cache of the underlying OS.
 */

public class FileCache {
  //should use hadoop log
  private final static Logger LOG = Logger.getLogger(FileCache.class.getName());

  /* To Do?  A seperate protocol used to query the status of a file */
  //public final byte PROTOCOL_START_BYTE = (byte)80;
  public final int PROTOCOL_PATH_BYTE_SIZE = 128;
  private volatile boolean shouldRun = true;


  /* To Do?  A seperate server class?  */
  /* QueryReceiver
   * Receive query from a socket and check if the path is cached.
   */
  private class QueryReceiver implements Runnable {
    private Socket s;

    public QueryReceiver(Socket s) {
      this.s = s;
    }

    public void run() {
      DataInputStream in = null;
      DataOutputStream out = null;
      try {
        //use NetUtils.get..Stream()
        in = new DataInputStream(new BufferedInputStream(this.s.getInputStream()));
        out = new DataOutputStream(new BufferedOutputStream(this.s.getOutputStream()));
        //use Path
        byte[] pathBuffer = new byte[PROTOCOL_PATH_BYTE_SIZE];
        in.read(pathBuffer);
        String fsPath = new String(pathBuffer);
        //deal with path
        byte[] outBuffer = null;
        String outString = "cache_" + fsPath;
        System.out.println(outString);
        outBuffer = outString.getBytes();
        out.write(outBuffer);
        out.flush();
      }
      catch(Throwable t) {
        LOG.log(Level.SEVERE, "QueryReceiver: ", t);
      }
      finally {
        //use IOUtils.closeStream(), IOUtils.closeSocket()
        try {
          in.close();
          this.s.close();
        }
        catch(IOException e) {
          LOG.log(Level.WARNING, "QueryReceiver close up error: ", e);
        }
      }
    }// end run
  }//end QueryReceiver

  /* To Do? A seperate server class? */
  /* QueryServer
   * Server used for receiving query/sending respsonse.
   */
  private class QueryServer implements Runnable {
    ServerSocket ss;
    Map<Socket, Socket> childSockets = Collections.synchronizedMap(
        new HashMap<Socket, Socket>());

    static final int MAX_RECEIVER_COUNT = 64;

    public QueryServer(ServerSocket ss) {
      this.ss = ss;
    }

    public void run() {
      while(shouldRun) {
        try {
          Socket s = this.ss.accept();
          s.setTcpNoDelay(true);
          Thread receiver = new Thread(new QueryReceiver(s));
          receiver.setDaemon(true);
          receiver.start();
        }
        catch(SocketTimeoutException ignored) {
          // wake up to see if should continue to run
        }
        catch(AsynchronousCloseException ace) {
          LOG.log(Level.WARNING, "QueryServer: ", ace);
          shouldRun = false;
        }
        catch(IOException ie) {
          LOG.log(Level.WARNING, "QueryServer: ", ie);
          shouldRun = false;
        }
        catch(Throwable te) {
          LOG.log(Level.WARNING, "QueryServer: ", te);
          shouldRun = false;
        }// end try
      }// end while

      try {
        this.ss.close();
      }
      catch (IOException ie) {
        LOG.log(Level.WARNING, "QueryServer closing error: ", ie);
      }
    }

    public void kill() {
      try {
        this.ss.close();
      }
      catch(IOException ie) {
        LOG.log(Level.WARNING, "QueryServer closing error: ", ie);
      }

      synchronized (childSockets) {
        for (Iterator<Socket> it = childSockets.values().iterator();
             it.hasNext();) {
          Socket thissocket = it.next();
          try {
            thissocket.close();
          }
          catch (IOException e) {
            LOG.log(Level.WARNING, "QueryReceiver closing error: ", e);
          }
        }// end for
      } //end synchronized
    }// end kill
  } // end QueryServer

  //use configuration
  public void offerService(int port) {
    ServerSocket serverSocket = null;
    try {
      serverSocket = new ServerSocket(port);
    }
    catch(IOException e) {
      System.err.println(e);
    }
    Thread server = new Thread(new QueryServer(serverSocket));
    server.start();

    while(true) {
      try {
        Thread.sleep(10000);
      }
      catch(InterruptedException e) {
      }
      System.out.println("file cache caching");
    }
  }

  //main
  public static void main(String[] arg) {
    FileCache fc = new FileCache();
    fc.offerService(4444);
  }
}
