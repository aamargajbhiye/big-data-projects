import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;

public class IgniteServer1 {
    public static void main(String[] args) {
        Ignite ignite = Ignition.start(IgniteServer1.class.getResource("ignite-config.xml"));
    }
}
