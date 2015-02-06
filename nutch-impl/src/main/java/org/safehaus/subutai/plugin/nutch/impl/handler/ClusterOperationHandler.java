package org.safehaus.subutai.plugin.nutch.impl.handler;


import java.util.Set;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandUtil;
import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.safehaus.subutai.plugin.common.api.ClusterOperationHandlerInterface;
import org.safehaus.subutai.plugin.common.api.ClusterOperationType;
import org.safehaus.subutai.plugin.common.api.ClusterSetupException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.nutch.api.NutchConfig;
import org.safehaus.subutai.plugin.nutch.impl.Commands;
import org.safehaus.subutai.plugin.nutch.impl.NutchImpl;

import com.google.common.base.Preconditions;


public class ClusterOperationHandler extends AbstractOperationHandler<NutchImpl, NutchConfig>
        implements ClusterOperationHandlerInterface
{
    private ClusterOperationType operationType;
    private NutchConfig config;
    Commands commands = new Commands();
    CommandUtil commandUtil = new CommandUtil();


    public ClusterOperationHandler( final NutchImpl manager, final NutchConfig config,
                                    final ClusterOperationType operationType )
    {
        super( manager, config );
        this.operationType = operationType;
        this.config = config;
        trackerOperation = manager.getTracker().createTrackerOperation( NutchConfig.PRODUCT_KEY,
                String.format( "Creating %s tracker object...", clusterName ) );
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

            trackerOperation.addLog( "Setting up cluster..." );
            s.setup();
            trackerOperation.addLogDone( "Cluster setup completed" );
        }
        catch ( ClusterSetupException e )
        {
            trackerOperation.addLogFailed( String.format( "Cluster setup failed : %s", e.getMessage() ) );
        }
    }


    @Override
    public void destroyCluster()
    {
        trackerOperation.addLog( "Uninstalling Nutch..." );

        Environment environment;
        try
        {
            environment = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            trackerOperation.addLogFailed( "Environment not found" );
            return;
        }

        Set<ContainerHost> nodes;
        try
        {
            nodes = environment.getContainerHostsByIds( config.getNodes() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            trackerOperation.addLogFailed( String.format( "Failed obtaining environment containers: %s", e ) );
            return;
        }

        for ( ContainerHost node : nodes )
        {
            try
            {
                commandUtil.execute( commands.getUninstallCommand(), node );
            }
            catch ( CommandException e )
            {
                trackerOperation
                        .addLog( String.format( "Failed to uninstall Nutch from node %s", node.getHostname() ) );
            }
        }

        trackerOperation.addLog( "Updating db..." );
        manager.getPluginDao().deleteInfo( NutchConfig.PRODUCT_KEY, config.getClusterName() );
        trackerOperation.addLogDone( "Cluster info deleted from DB" );
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
