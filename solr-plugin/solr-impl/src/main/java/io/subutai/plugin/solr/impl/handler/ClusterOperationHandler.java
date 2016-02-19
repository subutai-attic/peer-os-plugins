package io.subutai.plugin.solr.impl.handler;


import java.util.HashSet;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.ContainerHost;
import io.subutai.core.plugincommon.api.AbstractOperationHandler;
import io.subutai.core.plugincommon.api.ClusterOperationHandlerInterface;
import io.subutai.core.plugincommon.api.ClusterOperationType;
import io.subutai.core.plugincommon.api.ClusterSetupException;
import io.subutai.core.plugincommon.api.ClusterSetupStrategy;
import io.subutai.plugin.solr.api.SolrClusterConfig;
import io.subutai.plugin.solr.impl.Commands;
import io.subutai.plugin.solr.impl.SolrImpl;


public class ClusterOperationHandler extends AbstractOperationHandler<SolrImpl, SolrClusterConfig>
        implements ClusterOperationHandlerInterface
{
    private static final Logger LOG = LoggerFactory.getLogger( ClusterOperationHandler.class.getName() );
    private ClusterOperationType operationType;
    private SolrClusterConfig config;


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
        switch ( operationType )
        {
            case INSTALL_OVER_ENV:
                break;
            case UNINSTALL:
                break;
        }
    }


    /**
     * Configures container hosts for solr cluster
     */
    @Override
    public void setupCluster()
    {
        trackerOperation.addLog( "Building environment..." );

        try
        {
            Environment environment = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );
            ClusterSetupStrategy clusterSetupStrategy =
                    manager.getClusterSetupStrategy( environment, config, trackerOperation );
            clusterSetupStrategy.setup();
        }
        catch ( ClusterSetupException | EnvironmentNotFoundException e )
        {
            String msg = String.format( "Failed to setup cluster %s : %s", clusterName, e.getMessage() );
            trackerOperation.addLogFailed( msg );
            throw new RuntimeException( msg );
        }
        trackerOperation.addLogDone( String.format( "Cluster %s set up successfully", clusterName ) );
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
        Environment environment;
        try
        {
            environment = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            LOG.error( "Error getting environment", e );
            trackerOperation.addLogFailed( "Error getting environment" );
            return;
        }

        Set<ContainerHost> clusterHosts = new HashSet<>();
        Set<String> solrNodes = new HashSet<>( config.getNodes() );
        for ( ContainerHost host : environment.getContainerHosts() )
        {
            if ( host.getTemplateName().equals( SolrClusterConfig.TEMPLATE_NAME ) && solrNodes
                    .contains( host.getId() ) )
            {
                clusterHosts.add( host );
            }
        }


        for ( final ContainerHost containerHost : clusterHosts )
        {
            try
            {
                containerHost.execute( new RequestBuilder( Commands.stopCommand ).withTimeout( 30 ) );
            }
            catch ( CommandException e )
            {
                LOG.error( String.format( "Error executing %s command on host %s", Commands.stopCommand,
                        containerHost.getHostname() ), e );
                trackerOperation.addLogFailed(
                        String.format( "Error executing %s command on host %s", Commands.stopCommand,
                                containerHost.getHostname() ) );
            }
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
