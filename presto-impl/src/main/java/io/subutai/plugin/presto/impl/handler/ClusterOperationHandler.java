package io.subutai.plugin.presto.impl.handler;


import java.util.UUID;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.CommandUtil;
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
import io.subutai.plugin.presto.api.PrestoClusterConfig;
import io.subutai.plugin.presto.impl.PrestoImpl;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;


public class ClusterOperationHandler extends AbstractOperationHandler<PrestoImpl, PrestoClusterConfig>
        implements ClusterOperationHandlerInterface
{
    private static final Logger LOG = LoggerFactory.getLogger( ClusterOperationHandler.class );
    private ClusterOperationType operationType;
    private PrestoClusterConfig config;
    CommandUtil commandUtil;


    public ClusterOperationHandler( final PrestoImpl manager, final PrestoClusterConfig config,
                                    final ClusterOperationType operationType )
    {
        super( manager, config );
        this.operationType = operationType;
        this.config = config;
        trackerOperation = manager.getTracker().createTrackerOperation( PrestoClusterConfig.PRODUCT_KEY,
                String.format( "Creating %s tracker object...", clusterName ) );
        commandUtil = new CommandUtil();
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

            ClusterSetupStrategy s = manager.getClusterSetupStrategy( trackerOperation, config );
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
        TrackerOperation po = trackerOperation;
        po.addLog( "Uninstalling Presto..." );

        for ( UUID uuid : config.getAllNodes() )
        {
            ContainerHost containerHost = null;
            try
            {
                containerHost = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() )
                                       .getContainerHostById( uuid );
            }
            catch ( ContainerHostNotFoundException e )
            {
                LOG.error( "Container host not found", e );
                trackerOperation.addLogFailed( "Container host not found" );
            }
            catch ( EnvironmentNotFoundException e )
            {
                LOG.error( "Error getting environment by id: " + config.getEnvironmentId().toString(), e );
                return;
            }
            if ( containerHost.getHostname() == null )
            {
                po.addLogFailed(
                        String.format( "Node %s is not connected\nOperation aborted", containerHost.getHostname() ) );
                return;
            }
            try
            {
                CommandResult result = containerHost.execute( manager.getCommands().getUninstallCommand() );
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
        manager.getPluginDAO().deleteInfo( PrestoClusterConfig.PRODUCT_KEY, config.getClusterName() );
        po.addLogDone( "Cluster info deleted from DB\nDone" );
    }


    public void startNStopNCheckAllNodes( ClusterOperationType type ){
        for ( UUID uuid : config.getAllNodes() ){
            ContainerHost containerHost = null;
            try
            {
                containerHost = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() )
                                       .getContainerHostById( uuid );
                try
                {
                    CommandResult result = null;
                    switch ( type ){
                        case START_ALL:
                            result = commandUtil.execute( manager.getCommands().getStartCommand().daemon(), containerHost );
                            break;
                        case STOP_ALL:
                            result = commandUtil.execute( manager.getCommands().getStopCommand().daemon(), containerHost );
                            break;
                        case STATUS_ALL:
                            result = commandUtil.execute( manager.getCommands().getStatusCommand().daemon(), containerHost );
                            break;
                    }
                    NodeOperationHanler.logStatusResults( trackerOperation, result );
                }
                catch ( CommandException e )
                {
                    e.printStackTrace();
                }
            }
            catch ( ContainerHostNotFoundException e )
            {
                LOG.error( "Container host not found", e );
                trackerOperation.addLogFailed( "Container host not found" );
            }
            catch ( EnvironmentNotFoundException e )
            {
                LOG.error( "Error getting environment by id: " + config.getEnvironmentId().toString(), e );
                return;
            }
        }
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
            case DESTROY:
                destroyCluster();
                break;
            case STOP_ALL:
            case START_ALL:
            case STATUS_ALL:
                startNStopNCheckAllNodes( operationType );
                break;
        }
    }
}
