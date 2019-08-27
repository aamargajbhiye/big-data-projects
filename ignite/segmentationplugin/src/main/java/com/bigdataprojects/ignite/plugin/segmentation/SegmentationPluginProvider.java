package com.bigdataprojects.ignite.plugin.segmentation;

import org.apache.ignite.IgniteCheckedException;
import org.apache.ignite.cluster.ClusterNode;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.GridPluginContext;
import org.apache.ignite.internal.IgniteKernal;
import org.apache.ignite.internal.processors.segmentation.GridSegmentationProcessor;
import org.apache.ignite.plugin.*;
import org.jetbrains.annotations.Nullable;

import java.io.Serializable;
import java.util.UUID;

/**
 * This is {@link PluginProvider} implementation which is used to resolves networks segmentation.
 * This implementation creates {@link SegmentationPlugin} and {@link SegmentationPluginProcessor}
 */
public class SegmentationPluginProvider implements PluginProvider<SegmentationPluginConfiguration> {

    private IgnitePlugin segmentationPlugin;

    public String name() {
        return "Segmentation Plugin";
    }

    public String version() {
        return "1.0.0";
    }

    public String copyright() {
        return null;
    }

    public void initExtensions(PluginContext pluginContext, ExtensionRegistry extensionRegistry) throws IgniteCheckedException {
        segmentationPlugin = new SegmentationPlugin();
    }

    public CachePluginProvider createCacheProvider(CachePluginContext cachePluginContext) {
        return null;
    }

    public void start(PluginContext pluginContext) throws IgniteCheckedException {

    }

    public void stop(boolean b) throws IgniteCheckedException {

    }

    public void onIgniteStart() throws IgniteCheckedException {

    }

    public void onIgniteStop(boolean b) {

    }

    @Nullable
    public Serializable provideDiscoveryData(UUID uuid) {
        return null;
    }

    public void receiveDiscoveryData(UUID uuid, Serializable serializable) {

    }

    public void validateNewNode(ClusterNode clusterNode) throws PluginValidationException {

    }

    @Nullable
    public SegmentationPluginProcessor createComponent(PluginContext pluginContext, Class aClass) {
        if (aClass.equals(GridSegmentationProcessor.class)) {
            return new SegmentationPluginProcessor(((IgniteKernal) pluginContext.grid()).context());

        }
        return null;
    }

    public IgnitePlugin plugin() {
        return segmentationPlugin;
    }
}
