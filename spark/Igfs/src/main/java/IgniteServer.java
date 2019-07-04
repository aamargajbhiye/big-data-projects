import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;

public class IgniteServer {
    public static void main(String[] args) {
        Ignite ignite = Ignition.start(IgniteServer.class.getResource("ignite-config.xml"));
    }
}
