package org.safehaus.subutai.plugin.mysql.impl.handler;


import java.util.Set;
import java.util.UUID;
import java.util.logging.Logger;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.CommandUtil;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentModificationException;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.environment.NodeGroup;
import org.safehaus.subutai.common.environment.Topology;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.protocol.PlacementStrategy;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.core.peer.api.LocalPeer;
import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.safehaus.subutai.plugin.common.api.ClusterConfigurationException;
import org.safehaus.subutai.plugin.common.api.ClusterException;
import org.safehaus.subutai.plugin.common.api.ClusterOperationHandlerInterface;
import org.safehaus.subutai.plugin.common.api.ClusterOperationType;
import org.safehaus.subutai.plugin.common.api.NodeType;
import org.safehaus.subutai.plugin.mysql.impl.ClusterConfig;
import org.safehaus.subutai.plugin.mysql.impl.MySQLCImpl;
import org.safehaus.subutai.plugin.mysql.impl.common.Commands;
import org.safehaus.subutai.plugin.mysqlc.api.MySQLClusterConfig;

import com.google.common.base.Preconditions;


/**
 * Created by tkila on 5/7/15.
 */
public class ClusterOperationHandler extends AbstractOperationHandler<MySQLCImpl, MySQLClusterConfig>
        implements ClusterOperationHandlerInterface
{
    private static final Logger LOG = Logger.getLogger( ClusterOperationHandler.class.getName() );
    private ClusterOperationType operationType;
    private MySQLClusterConfig config;
    private NodeType nodeType;
    private CommandUtil commandUtil;


    public ClusterOperationHandler( final MySQLCImpl manager, final MySQLClusterConfig mySQLClusterConfig,
                                    final ClusterOperationType clusterOperationType, NodeType nodeType )
    {
        super( manager, mySQLClusterConfig );
        this.config = mySQLClusterConfig;
        this.operationType = clusterOperationType;
        this.nodeType = nodeType;
        trackerOperation = manager.getTracker().createTrackerOperation( MySQLClusterConfig.PRODUCT_KEY,
                String.format( "Creating %s tracker object...", clusterName ) );
        commandUtil = new CommandUtil();
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
            case STOP_ALL:
                stopCluster( config );
                break;
            case DESTROY:
                destroyCluster();
                break;
            case START_ALL:
                startCluster( config );
                break;
            case ADD:
                addNode( nodeType );
                break;
        }
    }


    private void addNode( NodeType nodeType )
    {
        LocalPeer localPeer = manager.getPeerManager().getLocalPeer();
        EnvironmentManager environmentManager = manager.getEnvironmentManager();
        NodeGroup nodeGroup = new NodeGroup( MySQLClusterConfig.PRODUCT_NAME, MySQLClusterConfig.TEMPLATE_NAME, 1, 1, 1,
                new PlacementStrategy( "ROUND_ROBIN" ) );
        Topology topology = new Topology();
        topology.addNodeGroupPlacement( localPeer, nodeGroup );

        ContainerHost newNode;


        ContainerHost unusedNodeInEnvironment = findUnUsedContainerInEnvironment( environmentManager );
        if ( unusedNodeInEnvironment != null )
        {
            newNode = unusedNodeInEnvironment;
        }
        else
        {
            Set<ContainerHost> newNodeSet = null;

            try
            {
                newNodeSet = environmentManager.growEnvironment( config.getEnvironmentId(), topology, false );
            }
            catch ( EnvironmentModificationException | EnvironmentNotFoundException e )
            {
                LOG.severe( "Could not add new node to environment" );
                e.printStackTrace();
            }
            newNode = newNodeSet.iterator().next();
        }
        switch ( nodeType )
        {

            case MASTER_NODE:
                config.getManagerNodes().add( newNode.getId() );
                break;
            case DATANODE:
                config.getDataNodes().add( newNode.getId() );
                break;
            case SERVER:
                //                config.get TODO Implement MYSQL API
                break;
        }
        try
        {
            manager.saveConfig( config );
        }
        catch ( ClusterException e )
        {
            LOG.severe( String.format( "Could not save cluster configuration. %s", e.toString() ) );
        }

        try
        {
            //            env = environmentManager.findEnvironment( config.getEnvironmentId() );

            new ClusterConfig( trackerOperation, manager )
                    .configureCluster( config, environmentManager.findEnvironment( config.getEnvironmentId() ) );
            //            configurator.configureCluster( config, environmentManager.findEnvironment( config
            // .getEnvironmentId() ) );
        }
        catch ( EnvironmentNotFoundException | ClusterConfigurationException e )
        {
            e.printStackTrace();
        }
        trackerOperation.addLog( "Node added" );
    }


    private ContainerHost findUnUsedContainerInEnvironment( EnvironmentManager environmentManager )
    {
        ContainerHost unusedNode = null;

        try
        {
            Environment environment = environmentManager.findEnvironment( config.getEnvironmentId() );
            Set<ContainerHost> containerHostSet = environment.getContainerHosts();
            for ( ContainerHost host : containerHostSet )
            {
                if ( ( !config.getAllNodes().contains( host.getId() ) ) && host.getTemplateName().equals(
                        MySQLClusterConfig.TEMPLATE_NAME ) )
                {
                    unusedNode = host;
                    break;
                }
            }
        }
        catch ( EnvironmentNotFoundException e )
        {
            e.printStackTrace();
        }
        return checkUnusedNode( unusedNode );
    }


    private ContainerHost checkUnusedNode( ContainerHost node )
    {
        if ( node != null )
        {
            for ( MySQLClusterConfig config : manager.getClusters() )
            {
                if ( !config.getAllNodes().contains( node.getId() ) )
                {
                    return node;
                }
            }
        }
        return null;
    }


    private void stopCluster( MySQLClusterConfig config )
    {
        Environment env = null;
        CommandResult commandResult = null;

        try
        {
            env = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            e.printStackTrace();
        }

        for ( UUID uuid : config.getManagerNodes() )
        {
            try
            {
                ContainerHost host = env.getContainerHostById( uuid );
                commandResult = host.execute( new RequestBuilder( Commands.stopAllCommand ) );
                trackerOperation.addLog( Commands.stopAllCommand + "\n" + commandResult.getStdOut() );
            }
            catch ( ContainerHostNotFoundException | CommandException e )
            {
                e.printStackTrace();
            }
        }
        //ndb_mgm -e shutdown
        Set<UUID> dataNodes = config.getDataNodes();

        for ( UUID dataNode : dataNodes )
        {
            try
            {
                ContainerHost containerHost = null;
                containerHost = env.getContainerHostById( dataNode );
                commandResult = executeCommand( containerHost, Commands.stopMySQLServer );
                trackerOperation.addLog( commandResult.getStdOut() );
            }
            catch ( ContainerHostNotFoundException e )
            {
                e.printStackTrace();
            }
        }
    }


    public void startCluster( MySQLClusterConfig config )
    {
        Environment env = null;
        try
        {
            env = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            e.printStackTrace();
        }
        CommandResult result = null;
        Set<UUID> dataNodes = config.getDataNodes();
        Set<UUID> managerNodes = config.getManagerNodes();

        ContainerHost containerHost = null;

        for ( UUID managerNode : managerNodes )
        {
            try
            {
                containerHost = env.getContainerHostById( managerNode );
            }
            catch ( ContainerHostNotFoundException e )
            {
                e.printStackTrace();
            }
            String commandToExecute = config.getRequiresReloadConf().get( containerHost.getHostname() ) == true ?
                                      Commands.startInitManageNode : Commands.startManagementNode;

            config.getRequiresReloadConf().put( containerHost.getHostname(),false );
            result = executeCommand( containerHost, commandToExecute );
        }

        trackerOperation.addLog( result.getStdOut() );
        for ( UUID dataNode : dataNodes )
        {
            try
            {
                containerHost = env.getContainerHostById( dataNode );
            }
            catch ( ContainerHostNotFoundException e )
            {
                e.printStackTrace();
            }

            result = executeCommand( containerHost, Commands.startCommand );
        }
        trackerOperation.addLog( result.getStdOut() );

        for ( UUID dataNode : dataNodes )
        {
            try
            {
                containerHost = env.getContainerHostById( dataNode );
            }
            catch ( ContainerHostNotFoundException e )
            {
                e.printStackTrace();
            }

            if ( config.getIsSqlInstalled().get( containerHost.getHostname() ) )
            {
                result = executeCommand( containerHost, Commands.startMySQLServer );
            }
        }
        trackerOperation.addLog( result.getStdOut() );
    }


    @Override
    public void runOperationOnContainers( final ClusterOperationType clusterOperationType )
    {
        //        try
        //        {
        //            Environment env = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );
        //            CommandResult result = null;
        //            Set<UUID> dataNodes = config.getDataNodes();
        //            Set<UUID> managerNodes = config.getManagerNodes();
        //            ContainerHost containerHost = null;
        //            switch ( clusterOperationType )
        //            {
        //                case INSTALL:
        //                    break;
        //                case INSTALL_OVER_ENV:
        //                    break;
        //                case UNINSTALL:
        //                    break;
        //                case DESTROY:
        //                    break;
        //                case START_ALL:
        //                    for ( UUID managerNode : managerNodes )
        //                    {
        //                        try
        //                        {
        //                            containerHost = env.getContainerHostById( managerNode );
        //                        }
        //                        catch ( ContainerHostNotFoundException e )
        //                        {
        //                            e.printStackTrace();
        //                        }
        //
        //                        result = executeCommand( containerHost, Commands.startManagementNode );
        //                    }
        //
        //                    trackerOperation.addLog( result.getStdOut() );
        //                    for ( UUID dataNode : dataNodes )
        //                    {
        //                        try
        //                        {
        //                            containerHost = env.getContainerHostById( dataNode );
        //                        }
        //                        catch ( ContainerHostNotFoundException e )
        //                        {
        //                            e.printStackTrace();
        //                        }
        //
        //                        result = executeCommand( containerHost, Commands.startCommand );
        //                        break;
        //                    }
        //                    trackerOperation.addLog( result.getStdOut() == null ? "Failed" : result.getStdOut() );
        //                    break;
        //
        //                case STOP_ALL:
        //                    for ( UUID managerHost : managerNodes )
        //                    {
        //                        try
        //                        {
        //                            containerHost = env.getContainerHostById( managerHost );
        //                        }
        //                        catch ( ContainerHostNotFoundException e )
        //                        {
        //                            e.printStackTrace();
        //                        }
        //
        //                        result = executeCommand( containerHost, Commands.stopAllCommand );
        //                        break;
        //                    }
        //                    trackerOperation.addLog( result.getStdOut() == null ? "Failed" : result.getStdOut() );
        //                    break;
        //                case STATUS_ALL:
        //                    break;
        //                case DECOMISSION_STATUS:
        //                    break;
        //                case ADD:
        //                    break;
        //                case REMOVE:
        //                    break;
        //                case CUSTOM:
        //                    break;
        //            }
        //        }
        //        catch ( EnvironmentNotFoundException ex )
        //        {
        //
        //            trackerOperation.addLogFailed( "Environment not found" );
        //        }
    }


    private CommandResult executeCommand( ContainerHost host, String command )
    {
        CommandResult result = null;
        try
        {
            result = host.execute( new RequestBuilder( command ) );
        }
        catch ( CommandException ex )
        {
            LOG.severe( String.format( "Could not execute command correctly:%s", command ) );
        }
        return result;
    }


    @Override
    public void setupCluster()
    {
        trackerOperation.addLog( "Setting up cluster ..." );
        try
        {
            Environment env = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );

            new ClusterConfig( trackerOperation, manager ).configureCluster( config, env );
            trackerOperation.addLogDone( String.format( "Cluster %s configured successfully", clusterName ) );
        }
        catch ( EnvironmentNotFoundException | ClusterConfigurationException e )
        {
            trackerOperation
                    .addLogFailed( String.format( "Failed to setup cluster %s : %s", clusterName, e.getMessage() ) );
        }
    }


    @Override
    public void destroyCluster()
    {
        MySQLClusterConfig config = manager.getCluster( clusterName );
        if ( config == null )
        {
            trackerOperation.addLogFailed(
                    String.format( "Cluster with name %s does not exist. Operation aborted", clusterName ) );
            return;
        }
        Environment env = null;
        try
        {
            env = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException ex )
        {
            trackerOperation.addLogFailed( "Environment not found" );
            return;
        }
        try
        {
            manager.deleteConfig( config );
        }
        catch ( ClusterException e )
        {
            trackerOperation.addLogFailed( "Failed to delete cluster information from database." );
            return;
        }
        trackerOperation.addLogDone( "Cluster removed from database" );
    }
}