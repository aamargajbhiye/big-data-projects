package com.bigdataprojects.ignite.plugin.segmentation;

import com.bigdataprojects.ignite.plugin.segmentation.resolvers.ReachabilitySegmentationResolver;
import org.apache.ignite.Ignition;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.plugin.segmentation.SegmentationResolver;
import org.junit.jupiter.api.Test;

import java.net.InetAddress;
import java.net.UnknownHostException;

public class SegmentationResolverTest {

    @Test
    public void testSegmentationPlugin() throws UnknownHostException {

        IgniteConfiguration igniteConfiguration = new IgniteConfiguration();

        InetAddress inetAddress = InetAddress.getByName("www.google.com");
        ReachabilitySegmentationResolver reachabilitySegmentationResolver =
                new ReachabilitySegmentationResolver(inetAddress);
        SegmentationResolver[] segmentationResolvers = new SegmentationResolver[]
                {reachabilitySegmentationResolver};

        igniteConfiguration.setAllSegmentationResolversPassRequired(true);
        igniteConfiguration.setSegmentationResolveAttempts(3);
        igniteConfiguration.setSegmentationResolvers(segmentationResolvers);
        igniteConfiguration.setPluginConfigurations(new SegmentationPluginConfiguration());

        Ignition.start(igniteConfiguration);

    }
}
