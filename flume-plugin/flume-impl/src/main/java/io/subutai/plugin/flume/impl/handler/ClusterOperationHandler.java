package io.subutai.plugin.flume.impl.handler;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.plugin.common.api.AbstractOperationHandler;
import io.subutai.plugin.common.api.ClusterOperationHandlerInterface;
import io.subutai.plugin.common.api.ClusterOperationType;
import io.subutai.plugin.common.api.ClusterSetupException;
import io.subutai.plugin.common.api.ClusterSetupStrategy;
import io.subutai.plugin.flume.api.FlumeConfig;
import io.subutai.plugin.flume.impl.CommandType;
import io.subutai.plugin.flume.impl.Commands;
import io.subutai.plugin.flume.impl.FlumeImpl;


public class ClusterOperationHandler extends AbstractOperationHandler<FlumeImpl, FlumeConfig>
        implements ClusterOperationHandlerInterface
{
    private static final Logger LOG = LoggerFactory.getLogger( ClusterOperationHandler.class.getName() );
    private ClusterOperationType operationType;
    private FlumeConfig config;


    public ClusterOperationHandler( final FlumeImpl manager, final FlumeConfig config,
                                    final ClusterOperationType operationType )
    {
        super( manager, config );
        this.operationType = operationType;
        this.config = config;
        trackerOperation = manager.getTracker().createTrackerOperation( FlumeConfig.PRODUCT_KEY,
                String.format( "Creating %s tracker object...", clusterName ) );
    }


    @Override
    public void runOperationOnContainers( ClusterOperationType clusterOperationType )
    {

    }


    @Override
    public void setupCluster()
    {
        try
        {
            ClusterSetupStrategy s = manager.getClusterSetupStrategy( config, trackerOperation );

            trackerOperation.addLog( "Installing cluster..." );
            s.setup();
            trackerOperation.addLogDone( "Installing cluster completed" );
        }
        catch ( ClusterSetupException e )
        {
            trackerOperation.addLogFailed( String.format( "Could not start all nodes : %s", e.getMessage() ) );
        }
    }


    @Override
    public void destroyCluster()
    {
        TrackerOperation po = trackerOperation;

        if ( manager.getCluster( config.getClusterName() ) == null )
        {
            po.addLogFailed( String.format( "Cluster %s not found", config.getClusterName() ) );
            return;
        }

        po.addLog( "Uninstalling Flume..." );

        for ( String nodeId : config.getNodes() )
        {
            ContainerHost containerHost ;
            try
            {
                containerHost = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() )
                                       .getContainerHostById( nodeId );
            }
            catch ( ContainerHostNotFoundException e )
            {
                LOG.error( "Container host not found", e );
                trackerOperation.addLogFailed( "Container host not found" );
                return;
            }
            catch ( EnvironmentNotFoundException e )
            {
                LOG.error( "Error getting environment by id: " + config.getEnvironmentId(), e );
                return;
            }
            CommandResult result;
            try
            {
                result = containerHost.execute( new RequestBuilder( Commands.make( CommandType.PURGE ) ) );
                if ( !result.hasSucceeded() )
                {
                    po.addLog( result.getStdErr() );
                    po.addLogFailed( "Uninstallation failed" );
                    return;
                }
            }
            catch ( CommandException e )
            {
                LOG.error( e.getMessage(), e );
            }
        }
        po.addLog( "Updating db..." );
        manager.getPluginDao().deleteInfo( FlumeConfig.PRODUCT_KEY, config.getClusterName() );
        po.addLogDone( "Cluster info deleted from DB\nDone" );
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
