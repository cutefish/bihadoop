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

/* FileCache.java
 * Service used to cache a remote file in the local disk and keep it in the file
 * cache of the underlying OS.
 */

public class FileCache {
  /* To Do?  A seperate protocol used to query the status of a file */
  //public final byte PROTOCOL_START_BYTE = (byte)80;
  public final int PROTOCOL_PATH_BYTE_SIZE = 128;
  private volatile boolean shouldRun = true;

  /* To Do?  A seperate server class?  */
  /* QueryReceiver
   * Receive query from a socket and check if the path is cached.
   */
  private class QueryReceiver implements Runnable {
    private Socket m_s;

    public QueryReceiver(Socket s) {
      m_s = s;
    }

    public void run() {
      DataInputStream in = null;
      DataOutputStream out = null;
      try {
        //use NetUtils.get..Stream()
        in = new DataInputStream(new BufferedInputStream(m_s.getInputStream()));
        out = new DataOutputStream(new BufferedOutputStream(m_s.getOutputStream()));
        //use Path
        byte[] pathBuffer = new byte[PROTOCOL_PATH_BYTE_SIZE];
        in.read(pathBuffer);
        String fsPath = new String(pathBuffer);
        System.out.println(fsPath);
        out.write(pathBuffer);
      }
      catch(Exception e) {
        //should use log
        System.err.println(e);
      }
      finally {
        //use IOUtils.closeStream(), IOUtils.closeSocket()
        try {
          in.close();
          m_s.close();
        }
        catch(IOException e) {
        }
      }
    }// end run
  }//end QueryReceiver

  /* To Do? A seperate server class? */
  /* QueryServer
   * Server used for receiving query/sending respsonse.
   */
  private class QueryServer implements Runnable {
    ServerSocket m_ss;
    Map<Socket, Socket> m_childSockets = Collections.synchronizedMap(
        new HashMap<Socket, Socket>());

    static final int MAX_RECEIVER_COUNT = 64;

    public QueryServer(ServerSocket ss) {
      m_ss = ss;
    }

    public void run() {
      while(shouldRun) {
        try {
          Socket s = m_ss.accept();
          s.setTcpNoDelay(true);
          Thread receiver = new Thread(new QueryReceiver(s));
          receiver.setDaemon(true);
          receiver.start();
        }
        catch(SocketTimeoutException ignored) {
          // wake up to see if should continue to run
        }
        catch(AsynchronousCloseException ace) {
          //should use log
          System.err.println(ace);
          shouldRun = false;
        }
        catch(IOException ie) {
          System.err.println(ie);
          shouldRun = false;
        }
        catch(Throwable te) {
          System.err.println(te);
          shouldRun = false;
        }// end try
      }// end while

      try {
        m_ss.close();
      }
      catch (IOException ie) {
        System.err.println(ie);
      }
    }

    public void kill() {
      try {
        m_ss.close();
      }
      catch(IOException ie) {
        System.err.println(ie);
      }

      synchronized (m_childSockets) {
        for (Iterator<Socket> it = m_childSockets.values().iterator();
             it.hasNext();) {
          Socket thissocket = it.next();
          try {
            thissocket.close();
          }
          catch (IOException e) {
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
