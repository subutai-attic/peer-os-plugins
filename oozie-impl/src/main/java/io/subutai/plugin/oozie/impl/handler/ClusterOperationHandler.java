package io.subutai.plugin.oozie.impl.handler;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.plugin.common.api.AbstractOperationHandler;
import io.subutai.plugin.common.api.ClusterOperationHandlerInterface;
import io.subutai.plugin.common.api.ClusterOperationType;
import io.subutai.plugin.common.api.ClusterSetupException;
import io.subutai.plugin.common.api.ClusterSetupStrategy;
import io.subutai.plugin.oozie.api.OozieClusterConfig;
import io.subutai.plugin.oozie.impl.Commands;
import io.subutai.plugin.oozie.impl.OozieImpl;


public class ClusterOperationHandler extends AbstractOperationHandler<OozieImpl, OozieClusterConfig>
        implements ClusterOperationHandlerInterface
{
    private static final Logger LOG = LoggerFactory.getLogger( ClusterOperationHandler.class );
    private ClusterOperationType operationType;
    private OozieClusterConfig config;


    public ClusterOperationHandler( final OozieImpl manager, final OozieClusterConfig config,
                                    final ClusterOperationType operationType )
    {
        super( manager, config );
        this.operationType = operationType;
        this.config = config;
        trackerOperation = manager.getTracker().createTrackerOperation( OozieClusterConfig.PRODUCT_KEY,
                String.format( "Creating %s tracker object...", clusterName ) );
    }


    @Override
    public void runOperationOnContainers( ClusterOperationType clusterOperationType )
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
        po.addLog( "Uninstalling Oozie client..." );

        OozieClusterConfig config = manager.getCluster( clusterName );
        if ( config == null )
        {
            trackerOperation.addLogFailed(
                    String.format( "Cluster with name %s does not exist. Operation aborted", clusterName ) );
            return;
        }

        for ( String id : config.getClients() )
        {
            ContainerHost containerHost;
            try
            {
                containerHost = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() )
                                       .getContainerHostById( id );
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
            try
            {
                CommandResult result = containerHost.execute( Commands.getUninstallClientsCommand() );
                if ( !result.hasSucceeded() )
                {
                    po.addLog( result.getStdErr() );
                    po.addLogFailed( "Uninstallation of oozie client failed" );
                    return;
                }
            }
            catch ( CommandException e )
            {
                LOG.error( e.getMessage(), e );
            }
        }

        po.addLog( "Uninstalling Oozie server..." );

        ContainerHost containerHost;
        try
        {
            containerHost = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() )
                                   .getContainerHostById( config.getServer() );
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
        try
        {
            CommandResult result = containerHost.execute( Commands.getUninstallServerCommand() );
            if ( !result.hasSucceeded() )
            {
                po.addLog( result.getStdErr() );
                po.addLogFailed( "Uninstallation of oozie server failed" );
                return;
            }
        }
        catch ( CommandException e )
        {
            LOG.error( e.getMessage(), e );
        }


        po.addLog( "Updating db..." );
        manager.getPluginDao().deleteInfo( OozieClusterConfig.PRODUCT_KEY, config.getClusterName() );
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
