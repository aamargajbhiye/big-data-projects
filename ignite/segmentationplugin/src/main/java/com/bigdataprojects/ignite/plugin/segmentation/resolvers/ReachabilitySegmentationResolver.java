package com.bigdataprojects.ignite.plugin.segmentation.resolvers;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.plugin.segmentation.SegmentationResolver;

import java.io.IOException;
import java.net.*;
import java.util.Enumeration;

/**
 * This is implementation of {@link SegmentationResolver} which checks if current segment could reach to a
 * destination node to decide if its in valid segment or not.
 */
public class ReachabilitySegmentationResolver implements SegmentationResolver {

    private InetAddress destinationAddress;

    public ReachabilitySegmentationResolver(InetAddress destinationAddress) {
        this.destinationAddress = destinationAddress;
    }

    public boolean isValidSegment() throws IgniteCheckedException {
        boolean reachable = true;
        try {
            Socket socket = new Socket();
            socket.bind(new InetSocketAddress(getLocalHostLANAddress(), 60000));
            socket.connect(new InetSocketAddress(destinationAddress, 1000));
        } catch (IOException e) {
            e.printStackTrace();
            reachable = false;
        }
        return reachable;
    }

    /**
     * @return {@link InetAddress}
     * @throws SocketException
     */
    private static InetAddress getLocalHostLANAddress() throws UnknownHostException {
        try {
            InetAddress candidateAddress = null;
            Enumeration<NetworkInterface> networkInterfaces = NetworkInterface.getNetworkInterfaces();
            while (networkInterfaces.hasMoreElements()) {
                NetworkInterface networkInterface = networkInterfaces.nextElement();
                // Iterate all IP addresses assigned to each card...
                Enumeration<InetAddress> inetAddresses = networkInterface.getInetAddresses();
                while (inetAddresses.hasMoreElements()) {
                    InetAddress inetAddr = inetAddresses.nextElement();
                    if (!inetAddr.isLoopbackAddress()) {

                        if (inetAddr.isSiteLocalAddress()) {
                            // Found non-loopback site-local address. Return it immediately...
                            return inetAddr;
                        } else if (candidateAddress == null) {
                            // Found non-loopback address, but not necessarily site-local.
                            // Store it as a candidate to be returned if site-local address is not subsequently found...
                            candidateAddress = inetAddr;
                            // Note that we don't repeatedly assign non-loopback non-site-local addresses as candidates,
                            // only the first. For subsequent iterations, candidate will be non-null.
                        }
                    }
                }
            }
            if (candidateAddress != null) {
                // We did not find a site-local address, but we found some other non-loopback address.
                // Server might have a non-site-local address assigned to its NIC (or it might be running
                // IPv6 which deprecates the "site-local" concept).
                // Return this non-loopback candidate address...
                return candidateAddress;
            }
            // At this point, we did not find a non-loopback address.
            // Fall back to returning whatever InetAddress.getLocalHost() returns...
            InetAddress jdkSuppliedAddress = InetAddress.getLocalHost();
            if (jdkSuppliedAddress == null) {
                throw new UnknownHostException("The JDK InetAddress.getLocalHost() method unexpectedly returned null.");
            }
            return jdkSuppliedAddress;
        } catch (Exception e) {
            UnknownHostException unknownHostException = new UnknownHostException("Failed to determine LAN address: " + e);
            unknownHostException.initCause(e);
            throw unknownHostException;
        }
    }
}
