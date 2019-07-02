import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.FileSystemConfiguration;
import org.apache.ignite.configuration.IgniteConfiguration;

public class IgniteServer {
    public static void main(String[] args) {
        IgniteConfiguration igniteConfiguration = new IgniteConfiguration();
        FileSystemConfiguration fileSystemConfiguration = new FileSystemConfiguration();
        fileSystemConfiguration.setName("myfilesystem");
        fileSystemConfiguration.set
        Ignite ignite = Ignition.start(IgniteServer.class.getResource("ignite-config.xml"));
    }
}
