package io.subutai.plugin.hive.impl.handler;


import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.settings.Common;
import io.subutai.core.plugincommon.api.AbstractOperationHandler;
import io.subutai.core.plugincommon.api.ClusterOperationHandlerInterface;
import io.subutai.core.plugincommon.api.ClusterOperationType;
import io.subutai.core.plugincommon.api.ClusterSetupException;
import io.subutai.core.plugincommon.api.ClusterSetupStrategy;
import io.subutai.plugin.hive.api.HiveConfig;
import io.subutai.plugin.hive.impl.Commands;
import io.subutai.plugin.hive.impl.HiveImpl;


/**
 * This class handles operations that are related to whole cluster.
 */
public class ClusterOperationHandler extends AbstractOperationHandler<HiveImpl, HiveConfig>
        implements ClusterOperationHandlerInterface
{
    private static final Logger LOG = LoggerFactory.getLogger( ClusterOperationHandler.class.getName() );
    private ClusterOperationType operationType;
    private HiveConfig config;


    public ClusterOperationHandler( final HiveImpl manager, final HiveConfig config,
                                    final ClusterOperationType operationType )
    {
        super( manager, config );
        this.operationType = operationType;
        this.config = config;
        trackerOperation = manager.getTracker().createTrackerOperation( HiveConfig.PRODUCT_KEY,
                String.format( "Creating %s tracker object...", clusterName ) );
    }


    public void run()
    {
        switch ( operationType )
        {
            case INSTALL:
                setupCluster();
                break;
            case UNINSTALL:
                destroyCluster();
                break;
            case START_ALL:
            case STOP_ALL:
            case STATUS_ALL:
                runOperationOnContainers( operationType );
                break;
        }
    }


    @Override
    public void runOperationOnContainers( ClusterOperationType clusterOperationType )
    {
        try
        {
            Environment environment = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );

            CommandResult result = null;
            switch ( clusterOperationType )
            {
                case START_ALL:
                    for ( EnvironmentContainerHost containerHost : environment.getContainerHosts() )
                    {
                        result = executeCommand( containerHost, Commands.START_COMMAND );
                    }
                    break;
                case STOP_ALL:
                    for ( EnvironmentContainerHost containerHost : environment.getContainerHosts() )
                    {
                        result = executeCommand( containerHost, Commands.STOP_COMMAND );
                    }
                    break;
                case STATUS_ALL:
                    for ( EnvironmentContainerHost containerHost : environment.getContainerHosts() )
                    {
                        result = executeCommand( containerHost, Commands.STATUS_COMMAND );
                    }
                    break;
            }
            NodeOperationHandler.logResults( trackerOperation, result );
        }

        catch ( EnvironmentNotFoundException e )
        {
            LOG.error( "Error getting environment by id: " + config.getEnvironmentId(), e );
        }
    }


    private CommandResult executeCommand( EnvironmentContainerHost containerHost, String command )
    {
        CommandResult result = null;
        try
        {
            result = containerHost.execute( new RequestBuilder( command ) );
        }
        catch ( CommandException e )
        {
            LOG.error( "Could not execute command correctly. ", command );
            e.printStackTrace();
        }
        return result;
    }


    @Override
    public void setupCluster()
    {

        try
        {
            ClusterSetupStrategy setupStrategy = manager.getClusterSetupStrategy( config, trackerOperation );
            setupStrategy.setup();

            trackerOperation.addLogDone( "Cluster setup complete" );
        }
        catch ( ClusterSetupException e )
        {
            trackerOperation.addLogFailed( String.format( "Failed to setup cluster: %s", e.getMessage() ) );
        }
    }


    @Override
    public void destroyCluster()
    {

        Environment environment;
        try
        {
            environment = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            LOG.error( "Error getting environment by id: " + config.getEnvironmentId(), e );
            return;
        }

        Set<EnvironmentContainerHost> hiveNodes = null;

        try
        {
            if ( environment != null )
            {
                hiveNodes = environment.getContainerHostsByIds( config.getAllNodes() );
            }
        }
        catch ( ContainerHostNotFoundException e )
        {
            LOG.error( "Container host not found", e );
            trackerOperation.addLogFailed( "Container host not found" );
            e.printStackTrace();
        }
        if ( hiveNodes != null )
        {
            for ( EnvironmentContainerHost host : hiveNodes )
            {
                try
                {
                    CommandResult result;
                    if ( host.getId().equals( config.getServer() ) )
                    {
                        result = host.execute( new RequestBuilder(
                                Commands.UNINSTALL_COMMAND + Common.PACKAGE_PREFIX + Commands.PRODUCT_KEY
                                        .toLowerCase() ) );
                        host.execute(
                                new RequestBuilder( Commands.UNINSTALL_COMMAND + Common.PACKAGE_PREFIX + "derby2" ) );
                    }
                    else
                    {
                        result = host.execute( new RequestBuilder(
                                Commands.UNINSTALL_COMMAND + Common.PACKAGE_PREFIX + Commands.PRODUCT_KEY ) );
                    }
                    if ( result.hasSucceeded() )
                    {
                        config.getClients().remove( host.getId() );
                        trackerOperation.addLog(
                                HiveConfig.PRODUCT_KEY + " is uninstalled from node " + host.getHostname()
                                        + " successfully." );
                    }
                    else
                    {
                        trackerOperation.addLog(
                                "Could not uninstall " + HiveConfig.PRODUCT_KEY + " from node " + host.getHostname() );
                    }
                }
                catch ( CommandException e )
                {
                    trackerOperation
                            .addLog( String.format( "Error uninstalling Hive from node %s", host.getHostname() ) );
                    e.printStackTrace();
                }
            }
        }

        try
        {
            assert environment != null;
            ContainerHost server = environment.getContainerHostById( config.getServer() );
            ContainerHost namenode = environment.getContainerHostById( config.getNamenode() );
            server.execute( new RequestBuilder( "rm -rf /opt/hive" ) );
            server.execute( new RequestBuilder( Commands.STOP_COMMAND ) );
            server.execute( new RequestBuilder( Commands.STOP_DERBY_COMMAND ) );
            namenode.execute( new RequestBuilder( "source /etc/profile ; hdfs dfs -rm -r /tmp" ) );
            namenode.execute( new RequestBuilder( "source /etc/profile ; hdfs dfs -rm -r /user" ) );
        }
        catch ( ContainerHostNotFoundException e )
        {
            LOG.error( "Container host not found", e );
            trackerOperation.addLogFailed( "Container host not found" );
            e.printStackTrace();
        }
        catch ( CommandException e )
        {
            e.printStackTrace();
        }

        manager.getPluginDAO().deleteInfo( HiveConfig.PRODUCT_KEY, config.getClusterName() );
        trackerOperation.addLogDone( "Hive cluster is removed from database" );
    }
}
