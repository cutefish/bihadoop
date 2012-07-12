import org.apache.commons.logging.*;

public class TestLog4j {
  public static final Log LOG = LogFactory.getLog(TestLog4j.class);
  public static void main(String[] args) {
    LOG.debug("hello world");
    LOG.info("hello world");
  }
}
