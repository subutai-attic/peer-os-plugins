package org.safehaus.subutai.plugin.solr.impl.handler;


import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.core.env.api.exception.EnvironmentCreationException;
import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.safehaus.subutai.plugin.common.api.ClusterOperationHandlerInterface;
import org.safehaus.subutai.plugin.common.api.ClusterOperationType;
import org.safehaus.subutai.plugin.common.api.ClusterSetupException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.solr.api.SolrClusterConfig;
import org.safehaus.subutai.plugin.solr.impl.SolrImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;


/**
 * Created by ebru on 06.11.2014.
 */
public class ClusterOperationHandler extends AbstractOperationHandler<SolrImpl, SolrClusterConfig>
        implements ClusterOperationHandlerInterface
{
    private static final Logger LOG = LoggerFactory.getLogger( ClusterOperationHandler.class.getName() );
    private ClusterOperationType operationType;
    private SolrClusterConfig config;
    private ExecutorService executor = Executors.newCachedThreadPool();


    public ClusterOperationHandler( final SolrImpl manager, final SolrClusterConfig config,
                                    final ClusterOperationType operationType )
    {
        super( manager, config );
        this.operationType = operationType;
        this.config = config;
        trackerOperation = manager.getTracker().createTrackerOperation( SolrClusterConfig.PRODUCT_KEY,
                String.format( "Creating %s tracker object...", clusterName ) );
    }


    @Override
    public void runOperationOnContainers( final ClusterOperationType operationType )
    {

    }


    @Override
    public void setupCluster()
    {
        trackerOperation.addLog( "Building environment..." );

        try
        {
            Environment env = manager.getEnvironmentManager()
                                     .createEnvironment( config.getClusterName(), config.getEnvironmentTopology(),
                                             false );
            //                                     .buildEnvironment( manager.getDefaultEnvironmentBlueprint( config
            // ) );

            ClusterSetupStrategy clusterSetupStrategy =
                    manager.getClusterSetupStrategy( env, config, trackerOperation );
            clusterSetupStrategy.setup();

            trackerOperation.addLogDone( String.format( "Cluster %s set up successfully", clusterName ) );
        }
        catch ( ClusterSetupException | EnvironmentCreationException e )
        {
            String msg = String.format( "Failed to setup cluster %s : %s", clusterName, e.getMessage() );
            trackerOperation.addLogFailed( msg );
            throw new RuntimeException( msg );
        }
    }


    @Override
    public void destroyCluster()
    {
        SolrClusterConfig config = manager.getCluster( clusterName );
        if ( config == null )
        {
            trackerOperation.addLogFailed(
                    String.format( "Cluster with name %s does not exist. Operation aborted", clusterName ) );
            return;
        }
        manager.getPluginDAO().deleteInfo( SolrClusterConfig.PRODUCT_KEY, config.getClusterName() );
        trackerOperation.addLogDone( "Cluster removed from database" );
    }


    @Override
    public void run()
    {
        Preconditions.checkNotNull( config, "Configuration is null !!!" );
        switch ( operationType )
        {
            case INSTALL:
                setupCluster();
                break;
            case UNINSTALL:
                destroyCluster();
                break;
        }
    }
}
