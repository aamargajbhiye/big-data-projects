package com.bigdataprojects.ignite.plugin.segmentation;

import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.internal.GridKernalContext;
import org.apache.ignite.internal.processors.GridProcessorAdapter;
import org.apache.ignite.internal.processors.segmentation.GridSegmentationProcessor;
import org.apache.ignite.plugin.segmentation.SegmentationResolver;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * This is {@link GridSegmentationProcessor} implementation. This implementation checks if segments are valid
 * or not using {@link SegmentationResolver}
 */
public class SegmentationPluginProcessor extends GridProcessorAdapter implements GridSegmentationProcessor {

    private SegmentationResolver[] segmentationResolvers;
    private int segmentationResolveAttempts;
    private boolean allSegmentationResolversPassRequired;
    private Logger logger = Logger.getLogger("SegmentationPluginProcessor");

    /**
     * @param ctx Kernal context.
     */
    protected SegmentationPluginProcessor(GridKernalContext ctx) {
        super(ctx);
        IgniteConfiguration igniteConfiguration = ctx.config();
        segmentationResolveAttempts = igniteConfiguration.getSegmentationResolveAttempts();
        segmentationResolvers = igniteConfiguration.getSegmentationResolvers();
        allSegmentationResolversPassRequired = igniteConfiguration.isAllSegmentationResolversPassRequired();
    }

    /**
     * @return
     */
    public boolean isValidSegment() {

        int attempts = 1;
        boolean validSegment = false;
        for (SegmentationResolver segmentationResolver : segmentationResolvers) {
            while (attempts <= segmentationResolveAttempts) {
                try {
                    validSegment = segmentationResolver.isValidSegment();
                    Thread.sleep(1000 * attempts);
                } catch (Exception e) {
                    logger.log(Level.SEVERE, "Exception while verifying segmentation resolution using {} "
                            + segmentationResolver, e);
                }
                attempts++;
            }

            if ((!validSegment && allSegmentationResolversPassRequired) ||
                    (!allSegmentationResolversPassRequired && validSegment)) {
                break;
            }
        }
        return validSegment;
    }
}
