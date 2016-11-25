package io.subutai.plugin.galera.impl.handler;


import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.plugincommon.api.AbstractOperationHandler;
import io.subutai.core.plugincommon.api.ClusterConfigurationException;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.core.plugincommon.api.NodeOperationType;
import io.subutai.core.plugincommon.api.NodeType;
import io.subutai.plugin.galera.api.GaleraClusterConfig;
import io.subutai.plugin.galera.impl.ClusterConfiguration;
import io.subutai.plugin.galera.impl.Commands;
import io.subutai.plugin.galera.impl.GaleraImpl;


public class NodeOperationHandler extends AbstractOperationHandler<GaleraImpl, GaleraClusterConfig>
{
    private static final Logger LOGGER = LoggerFactory.getLogger( NodeOperationHandler.class );
    private String clusterName;
    private String hostname;
    private NodeOperationType operationType;
    private NodeType nodeType;


    public NodeOperationHandler( final GaleraImpl manager, final String clusterName, final String hostName,
                                 NodeOperationType operationType )
    {
        super( manager, manager.getCluster( clusterName ) );
        this.hostname = hostName;
        this.clusterName = clusterName;
        this.operationType = operationType;
        this.trackerOperation = manager.getTracker().createTrackerOperation( GaleraClusterConfig.PRODUCT_KEY,
                String.format( "Checking %s cluster...", clusterName ) );
    }


    @Override
    public void run()
    {
        GaleraClusterConfig config = manager.getCluster( clusterName );
        if ( config == null )
        {
            trackerOperation.addLogFailed( String.format( "Cluster with name %s does not exist", clusterName ) );
            return;
        }

        Environment environment;
        try
        {
            environment = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            LOGGER.error( "Error getting environment by id: " + config.getEnvironmentId(), e );
            return;
        }

        if ( environment == null )
        {
            trackerOperation.addLogFailed( "Could not find cluster environment" );
            return;
        }

        Iterator iterator = environment.getContainerHosts().iterator();
        ContainerHost host = null;
        while ( iterator.hasNext() )
        {
            host = ( ContainerHost ) iterator.next();
            if ( host.getHostname().equals( hostname ) )
            {
                break;
            }
        }

        if ( host == null )
        {
            trackerOperation.addLogFailed( String.format( "No Container with ID %s", hostname ) );
            return;
        }

        try
        {
            CommandResult result;
            switch ( operationType )
            {
                case START:
                    result = host.execute( Commands.getStartMySql() );
                    logStatusResults( trackerOperation, result );
                    break;
                case STOP:
                    result = host.execute( Commands.getStopMySql() );
                    logStatusResults( trackerOperation, result );
                    break;
                case STATUS:
                    result = host.execute( Commands.getStatusMySql() );
                    logStatusResults( trackerOperation, result );
                    break;
                case INSTALL:
                    result = installProductOnNode( host );
                    logStatusResults( trackerOperation, result );
                    break;
                case UNINSTALL:
                    uninstallProductOnNode( host );
                    break;
            }
        }
        catch ( CommandException e )
        {
            trackerOperation.addLogFailed( String.format( "Command failed, %s", e.getMessage() ) );
        }
    }


    private void uninstallProductOnNode( final ContainerHost host )
    {
        try
        {
            Environment environment = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );

            ClusterConfiguration clusterConfiguration = new ClusterConfiguration( manager, trackerOperation );
            GaleraClusterConfig updatedConfig = clusterConfiguration.deleteNode( environment, config, host );
            updatedConfig.getNodes().remove( host.getId() );

            manager.saveConfig( updatedConfig );

            trackerOperation.addLogDone(
                    GaleraClusterConfig.PRODUCT_KEY + " is uninstalled from node " + host.getHostname()
                            + " successfully." );
        }
        catch ( EnvironmentNotFoundException e )
        {
            LOGGER.error( "Error getting environment by id: " + config.getEnvironmentId(), e );
        }
        catch ( ContainerHostNotFoundException e )
        {
            e.printStackTrace();
        }
        catch ( ClusterConfigurationException e )
        {
            e.printStackTrace();
        }
        catch ( CommandException e )
        {
            e.printStackTrace();
        }
        catch ( ClusterException e )
        {
            e.printStackTrace();
        }
    }


    private CommandResult installProductOnNode( final ContainerHost host )
    {
        return null;
    }


    public static void logStatusResults( TrackerOperation po, CommandResult result )
    {
        Preconditions.checkNotNull( result );
        StringBuilder log = new StringBuilder();
        String status;
        String cmdResult = result.getStdErr() + result.getStdOut();
        if ( cmdResult.contains( "/usr/bin/mysqladmin" ) )
        {
            status = "Mariadb is running";
        }
        else
        {
            status = "Mariadb is not running";
        }
        log.append( String.format( "%s", status ) );
        po.addLogDone( log.toString() );
    }
}
