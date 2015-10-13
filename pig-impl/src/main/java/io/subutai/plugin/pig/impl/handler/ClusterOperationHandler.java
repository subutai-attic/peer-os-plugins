package io.subutai.plugin.pig.impl.handler;


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
import io.subutai.plugin.common.api.ClusterException;
import io.subutai.plugin.common.api.ClusterOperationHandlerInterface;
import io.subutai.plugin.common.api.ClusterOperationType;
import io.subutai.plugin.common.api.ClusterSetupException;
import io.subutai.plugin.common.api.ClusterSetupStrategy;
import io.subutai.plugin.pig.api.PigConfig;
import io.subutai.plugin.pig.impl.Commands;
import io.subutai.plugin.pig.impl.PigImpl;


public class ClusterOperationHandler extends AbstractOperationHandler<PigImpl, PigConfig>
        implements ClusterOperationHandlerInterface
{
    private static final Logger LOG = LoggerFactory.getLogger( ClusterOperationHandler.class.getName() );
    private ClusterOperationType operationType;
    private PigConfig config;


    public ClusterOperationHandler( final PigImpl manager, final PigConfig config,
                                    final ClusterOperationType operationType )
    {
        super( manager, config );
        this.operationType = operationType;
        this.config = config;
        trackerOperation = manager.getTracker().createTrackerOperation( PigConfig.PRODUCT_KEY,
                String.format( "Creating %s tracker object...", clusterName ) );
    }


    @Override
    public void run()
    {
        Preconditions.checkNotNull( config, "Configuration is null !!!" );
        try
        {
            switch ( operationType )
            {
                case INSTALL:
                    setupCluster();
                    break;
                case DESTROY:
                    uninstallCluster();
                    break;
            }
        }
        catch ( ClusterException e )
        {
            trackerOperation.addLogFailed( String.format( "Operation failed, %s", e.getMessage() ) );
        }
    }


    @Override
    public void runOperationOnContainers( final ClusterOperationType clusterOperationType )
    {

    }


    @Override
    public void setupCluster()
    {
        try
        {
            ClusterSetupStrategy s = manager.getClusterSetupStrategy( config, trackerOperation );
            try
            {
                trackerOperation.addLog( "Installing cluster..." );
                s.setup();
                trackerOperation.addLogDone( "Installing cluster completed" );
            }
            catch ( ClusterSetupException ex )
            {
                throw new ClusterException( "Failed to setup cluster: " + ex.getMessage() );
            }
        }
        catch ( ClusterException e )
        {
            trackerOperation.addLogFailed( String.format( "Could not start all nodes : %s", e.getMessage() ) );
        }
    }


    @Override
    public void destroyCluster()
    {
    }


    public void uninstallCluster() throws ClusterException
    {
        TrackerOperation po = trackerOperation;
        po.addLog( "Uninstalling Pig..." );

        for ( String nodeId : config.getNodes() )
        {
            ContainerHost containerHost;
            try
            {
                containerHost = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() )
                                       .getContainerHostById( nodeId );
            }
            catch ( ContainerHostNotFoundException | EnvironmentNotFoundException e )
            {
                LOG.error( "Error getting environment by id: " + config.getEnvironmentId(), e );
                return;
            }
            try
            {
                CommandResult result = containerHost.execute( new RequestBuilder( Commands.uninstallCommand ) );
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
        manager.deleteConfig( config );
        po.addLogDone( "Cluster info deleted from DB\nDone" );
    }
}
