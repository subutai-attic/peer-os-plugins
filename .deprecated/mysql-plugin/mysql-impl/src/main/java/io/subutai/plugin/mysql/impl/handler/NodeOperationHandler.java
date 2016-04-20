package io.subutai.plugin.mysql.impl.handler;


import java.util.logging.Logger;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.ContainerHost;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.plugincommon.api.AbstractOperationHandler;
import io.subutai.core.plugincommon.api.ClusterConfigurationException;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.core.plugincommon.api.NodeOperationType;
import io.subutai.core.plugincommon.api.NodeType;
import io.subutai.plugin.mysql.api.MySQLClusterConfig;
import io.subutai.plugin.mysql.impl.ClusterConfig;
import io.subutai.plugin.mysql.impl.MySQLCImpl;
import io.subutai.plugin.mysql.impl.common.Commands;


public class NodeOperationHandler extends AbstractOperationHandler<MySQLCImpl, MySQLClusterConfig>
{
    private String clusterName;
    private String hostName;
    private NodeOperationType operationType;
    private NodeType nodeType;
    private static final Logger LOG = Logger.getLogger( NodeOperationHandler.class.getName() );


    public NodeOperationHandler( final MySQLCImpl manager, final String clusterName, final String hostName,
                                 NodeOperationType operationType, NodeType nodeType )
    {
        super( manager, manager.getCluster( clusterName ) );
        this.hostName = hostName;
        this.clusterName = clusterName;
        this.operationType = operationType;
        this.nodeType = nodeType;
        this.trackerOperation = manager.getTracker().createTrackerOperation( MySQLClusterConfig.PRODUCT_KEY,
                String.format( "Creating %s tracker object...", clusterName ) );
        //LOG.info( String.format( "Created tracker object with: %s", trackerOperation.getId().toString() ) );


    }


    @Override
    public void run()
    {
        MySQLClusterConfig config = manager.getCluster( clusterName );
        if ( config == null )
        {
            LOG.severe( ( String.format( "Cluster with name %s does not exist", clusterName ) ) );
            return;
        }
        Environment environment;
        try
        {
            environment = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            LOG.severe( ( "Cluster environment not found" ) );
            return;
        }

        ContainerHost host = null;
        try
        {
            host = environment.getContainerHostByHostname( hostName );
        }
        catch ( ContainerHostNotFoundException e )
        {
            e.printStackTrace();
        }

        if ( host == null )
        {
            LOG.severe( ( String.format( "No Container with ID %s", hostName ) ) );
            return;
        }

        if ( !config.getAllNodes().contains( host.getId() ) )
        {
            trackerOperation
                    .addLogFailed( String.format( "Node %s does not belong to %s cluster.", hostName, clusterName ) );
            return;
        }
        runCommand( host, operationType, nodeType );
    }


    protected void runCommand( ContainerHost host, NodeOperationType operationType, NodeType nodeType )
    {
        switch ( operationType )
        {
            case START:
                startNode( host, nodeType );
                break;
            case STOP:
                stopNode( host, nodeType );
                break;
            case STATUS:
                checkNode( host, nodeType );
                break;
            case DESTROY:
                destroyNode( host, nodeType );
                break;
            case INSTALL:
                installSqlServer( host, nodeType );
                break;
        }
    }


    private void destroyNode( final ContainerHost host, final NodeType nodeType )
    {
        EnvironmentManager environmentManager = manager.getEnvironmentManager();
        try
        {
            MySQLClusterConfig config = manager.getCluster( clusterName );
            switch ( nodeType )
            {
                case MASTER_NODE:
                    config.getManagerNodes().remove( host.getId() );
                    break;
                case DATANODE:
                    config.getDataNodes().remove( host.getId() );
                    break;
            }
            manager.saveConfig( config );
            ClusterConfig clusterConfig = new ClusterConfig( trackerOperation, manager );
            clusterConfig.configureCluster( config, environmentManager.loadEnvironment( config.getEnvironmentId() ) );
            trackerOperation.addLog( String.format( "Cluster information has been udpated" ) );
            trackerOperation
                    .addLogDone( String.format( "Container %s has been removed from cluster. ", host.getHostname() ) );
        }
        catch ( ClusterException | ClusterConfigurationException | EnvironmentNotFoundException e )
        {
            LOG.severe( "Exception happened during node removal" );
        }
    }


    private void installSqlServer( final ContainerHost host, NodeType nodeType )
    {
        if ( !nodeType.name().equalsIgnoreCase( "server" ) )
        {
            return;
        }
        MySQLClusterConfig config = manager.getCluster( clusterName );
        CommandResult result;

        if ( !config.getIsSqlInstalled().get( host.getHostname() ) )
        {

            try
            {
                result = host.execute( new RequestBuilder(
                        String.format( Commands.initMySQLServer, config.getDataNodeDataDir(),
                                config.getConfNodeDataFile() ) ).withCwd( Commands.sqlDir ) );

                if ( !result.getStdOut().toLowerCase().contains( "fatal" ) )
                {
                    trackerOperation.addLogDone( "Successfully installed MySQL Server API" );
                    config.getIsSqlInstalled().put( host.getHostname(), true );
                }
                trackerOperation.addLogDone( result.getStdOut() );
                manager.saveConfig( config );
            }
            catch ( CommandException | ClusterException e )
            {
                LOG.severe( "Failed to install MySQL Server API" );
            }
        }
    }


    private void startNode( ContainerHost host, NodeType nodeType )
    {
        CommandResult result = null;

        MySQLClusterConfig config = manager.getCluster( clusterName );

        try
        {
            switch ( nodeType )
            {
                case MASTER_NODE:
                    //manager node
                    String commandToExecute =
                            config.getRequiresReloadConf().get( host.getHostname() ) ? Commands.startInitManageNode :
                            Commands.startManagementNode;
                    result = host.execute( new RequestBuilder( commandToExecute ) );
                    break;
                case DATANODE://data node
                    //if first time launch,
                    if ( config.getIsInitialStart().get( host.getHostname() ) )
                    {
                        host.execute( new RequestBuilder(
                                String.format( "cp %s /etc/my.cnf", config.getConfNodeDataFile() ) ) );
                        result = host.execute(
                                new RequestBuilder( Commands.ndbdInit ).withCwd( config.getDataNodeDataDir() ) );
                        config.getIsInitialStart().put( host.getHostname(), false );
                        LOG.info( "Initial start of data node " + host.getHostname() );
                    }
                    else
                    {
                        result = host.execute( new RequestBuilder(
                                String.format( Commands.startCommand, config.getConfNodeDataFile() ) ) );
                    }
                    break;
                case SERVER://mysql server node
                    result = host.execute( new RequestBuilder( Commands.startMySQLServer ) );
                    break;
            }

            try
            {
                manager.saveConfig( config );
            }
            catch ( ClusterException e )
            {
                e.printStackTrace();
            }
            if ( result != null )
            {
                trackerOperation.addLogDone( result.getStdOut() );
            }
        }
        catch ( CommandException e )
        {
            e.printStackTrace();
        }
    }


    private void stopNode( ContainerHost host, NodeType nodeType )
    {
        CommandResult result = null;
        try
        {
            switch ( nodeType )
            {
                case MASTER_NODE://manager node
                    result = host.execute( new RequestBuilder( Commands.stopManagerNode ) );
                    trackerOperation.addLogDone( result.getStdOut() );
                    break;
                case DATANODE://data node
                    result = host.execute( new RequestBuilder( Commands.stopNodeCommand ) );
                    trackerOperation.addLogDone( result.getStdOut() );
                    break;
                case SERVER://mysql server node
                    result = host.execute( new RequestBuilder( Commands.stopMySQLServer ) );
                    break;
            }
            assert result != null;
            trackerOperation.addLog( result.getStdOut().isEmpty() ? result.getStdErr() : result.getStdOut() );
        }
        catch ( CommandException e )
        {
            e.printStackTrace();
        }
    }


    private void checkNode( ContainerHost host, NodeType nodeType )
    {
        CommandResult result = null;
        MySQLClusterConfig config = manager.getCluster( clusterName );

        try
        {
            switch ( nodeType )
            {
                case MASTER_NODE:
                    result = host.execute( new RequestBuilder( Commands.statusManagerNode ) );
                    if ( result.getStdOut().contains( Commands.startInitManageNode ) || result.getStdOut().contains(
                            Commands.startManagementNode ) )
                    {
                        trackerOperation
                                .addLogDone( nodeType.name() + " service is running on node " + host.getHostname() );
                    }
                    else
                    {
                        trackerOperation.addLogDone(
                                nodeType.name() + " service is not running on node " + host.getHostname() );
                    }
                    break;
                case DATANODE:
                    result = host.execute( new RequestBuilder( Commands.statusDataNode ) );
                    if ( result.getStdOut().contains( Commands.ndbdInit ) || result.getStdOut().contains(
                            String.format( Commands.startCommand, config.getConfNodeDataFile() ) ) )
                    {
                        trackerOperation
                                .addLogDone( nodeType.name() + " service is running on node " + host.getHostname() );
                    }
                    else
                    {
                        trackerOperation.addLogDone(
                                nodeType.name() + " service is not running on node " + host.getHostname() );
                    }
                    break;
                case SERVER:
                    result = host.execute( new RequestBuilder( Commands.statusMySQLServer ) );
                    if ( result.getStdOut().contains( "not running" ) )
                    {
                        trackerOperation.addLogDone(
                                nodeType.name() + " service is not running on node " + host.getHostname() );
                    }
                    else
                    {
                        trackerOperation
                                .addLogDone( nodeType.name() + " service is running on node " + host.getHostname() );
                    }
                    break;
            }
            LOG.info( String.format( "Std Out Result:%s", result ) );
        }
        catch ( CommandException e )
        {
            LOG.severe( "Failed to execute command" );
        }
    }
}
