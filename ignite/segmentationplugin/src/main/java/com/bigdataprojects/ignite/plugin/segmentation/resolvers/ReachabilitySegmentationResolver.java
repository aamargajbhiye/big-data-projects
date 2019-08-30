package com.bigdataprojects.ignite.plugin.segmentation.resolvers;

import org.apache.ignite.plugin.segmentation.SegmentationResolver;

import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.TimeUnit;

/**
 * This is implementation of {@link SegmentationResolver} which checks if current segment could reach to a
 * destination node to decide if its in valid segment or not.
 */
public class ReachabilitySegmentationResolver implements SegmentationResolver {

    private InetAddress destinationAddress;

    public ReachabilitySegmentationResolver(InetAddress destinationAddress) {
        this.destinationAddress = destinationAddress;
    }

    public boolean isValidSegment() {
        boolean reachable;
        try {
            Process exec = Runtime.getRuntime().exec("ping " + destinationAddress);
            //0 - normal termination
            reachable = exec.waitFor(1, TimeUnit.SECONDS);
        } catch (IOException e) {
            e.printStackTrace();
            reachable = false;
        } catch (InterruptedException e) {
            e.printStackTrace();
            reachable = false;
        }
        return reachable;
    }
}
