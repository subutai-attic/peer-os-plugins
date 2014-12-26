package org.safehaus.subutai.plugin.hadoop.impl.handler;


import java.util.UUID;

import org.safehaus.subutai.common.exception.ClusterConfigurationException;
import org.safehaus.subutai.common.exception.ClusterSetupException;
import org.safehaus.subutai.common.protocol.AbstractOperationHandler;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.hadoop.impl.ClusterConfiguration;
import org.safehaus.subutai.plugin.hadoop.impl.HadoopImpl;


public class ConfigureEnvironmentClusterHandler extends AbstractOperationHandler<HadoopImpl, HadoopClusterConfig>
{

    private HadoopClusterConfig config;
    private TrackerOperation po;


    public ConfigureEnvironmentClusterHandler( final HadoopImpl manager, final HadoopClusterConfig config )
    {
        super( manager, config.getClusterName() );
        this.config = config;
        po = manager.getTracker().createTrackerOperation( HadoopClusterConfig.PRODUCT_KEY,
                String.format( "Setting up %s cluster...", config.getClusterName() ) );
    }


    @Override
    public UUID getTrackerId()
    {
        return po.getId();
    }


    @Override
    public void run()
    {
        trackerOperation = po;
        po.addLog( "Building environment..." );

        try
        {
            Environment env = manager.getEnvironmentManager().getEnvironmentByUUID( config.getEnvironmentId() );
            try
            {
                new ClusterConfiguration( trackerOperation, manager ).configureCluster( config, env );
            }
            catch ( ClusterConfigurationException e )
            {
                throw new ClusterSetupException( e.getMessage() );
            }
        }
        catch ( ClusterSetupException e )
        {
            po.addLogFailed( String.format( "Failed to setup cluster %s : %s", clusterName, e.getMessage() ) );
        }
    }
}
