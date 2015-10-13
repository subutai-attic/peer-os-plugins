package io.subutai.plugin.mahout.impl.handler;


import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.plugin.common.api.AbstractOperationHandler;
import io.subutai.plugin.common.api.ClusterOperationHandlerInterface;
import io.subutai.plugin.common.api.ClusterOperationType;
import io.subutai.plugin.common.api.ClusterSetupException;
import io.subutai.plugin.common.api.ClusterSetupStrategy;
import io.subutai.plugin.mahout.api.MahoutClusterConfig;
import io.subutai.plugin.mahout.impl.MahoutImpl;


public class ClusterOperationHandler extends AbstractOperationHandler<MahoutImpl, MahoutClusterConfig>
        implements ClusterOperationHandlerInterface
{
    private static final Logger LOG = LoggerFactory.getLogger( ClusterOperationHandler.class.getName() );
    private ClusterOperationType operationType;
    private MahoutClusterConfig config;


    public ClusterOperationHandler( final MahoutImpl manager, final MahoutClusterConfig config,
                                    final ClusterOperationType operationType )
    {
        super( manager, config );
        this.operationType = operationType;
        this.config = config;
        trackerOperation = manager.getTracker().createTrackerOperation( MahoutClusterConfig.PRODUCT_KEY,
                String.format( "Creating %s tracker object...", clusterName ) );
    }


    @Override
    public void runOperationOnContainers( final ClusterOperationType clusterOperationType )
    {

    }


    @Override
    public void setupCluster()
    {
        if ( Strings.isNullOrEmpty( config.getClusterName() ) )
        {
            trackerOperation.addLogFailed( "Malformed configuration" );
            return;
        }

        if ( manager.getCluster( clusterName ) != null )
        {
            trackerOperation.addLogFailed( String.format( "Cluster with name '%s' already exists", clusterName ) );
            return;
        }

        try
        {
            ClusterSetupStrategy clusterSetupStrategy = manager.getClusterSetupStrategy( config, trackerOperation );
            clusterSetupStrategy.setup();

            trackerOperation.addLogDone( String.format( "Cluster %s set up successfully", clusterName ) );
        }
        catch ( ClusterSetupException e )
        {
            trackerOperation.addLogFailed(
                    String.format( "Failed to setup %s cluster %s : %s", config.getProductKey(), clusterName,
                            e.getMessage() ) );
        }
    }


    @Override
    public void destroyCluster()
    {
        TrackerOperation po = trackerOperation;
        po.addLog( "Uninstalling Mahout..." );

        MahoutClusterConfig config = manager.getCluster( clusterName );
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
            trackerOperation.addLogFailed( String.format( "Environment not found: %s", e ) );
            return;
        }

        Set<EnvironmentContainerHost> nodes;
        try
        {
            nodes = environment.getContainerHostsByIds( config.getNodes() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            trackerOperation.addLogFailed( String.format( "Failed to obtain environment containers: %s", e ) );
            return;
        }

        for ( EnvironmentContainerHost node : nodes )
        {
            CommandResult result;
            try
            {
                result = node.execute( manager.getCommands().getUninstallCommand() );
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
        manager.getPluginDAO().deleteInfo( MahoutClusterConfig.PRODUCT_KEY, config.getClusterName() );
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
            case DESTROY:
                destroyCluster();
                break;
        }
    }
}
