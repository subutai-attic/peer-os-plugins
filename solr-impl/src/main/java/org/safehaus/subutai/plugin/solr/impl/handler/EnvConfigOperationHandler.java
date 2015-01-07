package org.safehaus.subutai.plugin.solr.impl.handler;


import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.safehaus.subutai.common.exception.ClusterSetupException;
import org.safehaus.subutai.common.protocol.AbstractOperationHandler;
import org.safehaus.subutai.common.protocol.ClusterSetupStrategy;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.plugin.solr.api.SolrClusterConfig;
import org.safehaus.subutai.plugin.solr.impl.SolrImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by talas on 1/5/15.
 */
public class EnvConfigOperationHandler extends AbstractOperationHandler<SolrImpl, SolrClusterConfig>
{
    private static final Logger LOGGER = LoggerFactory.getLogger( EnvConfigOperationHandler.class );
    private SolrClusterConfig config;
    private ExecutorService executor = Executors.newCachedThreadPool();


    public EnvConfigOperationHandler( final SolrImpl manager, final SolrClusterConfig config )
    {
        super( manager, config );
        this.config = config;
        trackerOperation = manager.getTracker().createTrackerOperation( SolrClusterConfig.PRODUCT_KEY,
                String.format( "Setting up %s cluster...", clusterName ) );
    }


    @Override
    public void run()
    {
        try
        {
            Environment environment = manager.getEnvironmentManager().getEnvironmentByUUID( config.getEnvironmentId() );
            ClusterSetupStrategy clusterSetupStrategy =
                    manager.getClusterSetupStrategy( environment, config, trackerOperation );
            clusterSetupStrategy.setup();
        }
        catch ( ClusterSetupException e )
        {
            String msg = String.format( "Failed to setup cluster %s : %s", clusterName, e.getMessage() );
            trackerOperation.addLogFailed( msg );
            throw new RuntimeException( msg );
        }

        trackerOperation.addLogDone( String.format( "Cluster %s set up successfully", clusterName ) );
    }
}
