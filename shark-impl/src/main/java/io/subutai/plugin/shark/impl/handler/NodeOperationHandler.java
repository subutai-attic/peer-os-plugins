package io.subutai.plugin.shark.impl.handler;


import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.core.metric.api.MonitorException;
import io.subutai.plugin.common.api.AbstractOperationHandler;
import io.subutai.plugin.common.api.ClusterException;
import io.subutai.plugin.common.api.OperationType;
import io.subutai.plugin.shark.api.SharkClusterConfig;
import io.subutai.plugin.shark.impl.Commands;
import io.subutai.plugin.shark.impl.SharkImpl;
import io.subutai.plugin.spark.api.SparkClusterConfig;


public class NodeOperationHandler extends AbstractOperationHandler<SharkImpl, SharkClusterConfig>
{
    private static final Logger LOG = LoggerFactory.getLogger( NodeOperationHandler.class.getName() );

    private String hostname;
    private OperationType operationType;
    private Environment environment;
    private EnvironmentContainerHost node;


    public NodeOperationHandler( final SharkImpl manager, final SharkClusterConfig config, final String hostname,
                                 OperationType operationType )
    {
        super( manager, config );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( hostname ), "Invalid hostname" );
        Preconditions.checkNotNull( operationType );
        this.hostname = hostname;
        this.operationType = operationType;
        this.trackerOperation = manager.getTracker().createTrackerOperation( SharkClusterConfig.PRODUCT_KEY,
                String.format( "Executing %s operation on node %s", operationType.name(), hostname ) );
    }


    @Override
    public void run()
    {
        try
        {
            if ( manager.getCluster( clusterName ) == null )
            {
                throw new ClusterException( String.format( "Cluster with name %s does not exist", clusterName ) );
            }

            try
            {
                environment = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );
            }
            catch ( EnvironmentNotFoundException e )
            {
                throw new ClusterException(
                        String.format( "Environment not found by id %s", config.getEnvironmentId() ) );
            }


            try
            {
                node = environment.getContainerHostByHostname( hostname );
            }
            catch ( ContainerHostNotFoundException e )
            {
                throw new ClusterException( String.format( "Node not found in environment by name %s", hostname ) );
            }


            if ( !node.isConnected() )
            {
                throw new ClusterException( String.format( "Node %s is not connected", hostname ) );
            }


            switch ( operationType )
            {

                case INCLUDE:
                    addNode();
                    break;
                case EXCLUDE:
                    removeNode();
                    break;
            }
        }
        catch ( ClusterException e )
        {
            LOG.error( "Error in NodeOperationHandler", e );
            trackerOperation
                    .addLogFailed( String.format( "Operation %s failed: %s", operationType.name(), e.getMessage() ) );
        }
    }


    private void removeNode() throws ClusterException
    {

        //check if node is in the cluster
        if ( !config.getNodeIds().contains( node.getId() ) )
        {
            throw new ClusterException( String.format( "Node %s does not belong to this cluster", hostname ) );
        }

        if ( config.getNodeIds().size() == 1 )
        {
            throw new ClusterException( "This is the last node in the cluster. Please, destroy cluster instead" );
        }

        trackerOperation.addLog( "Uninstalling Shark..." );

        executeCommand( node, manager.getCommands().getUninstallCommand(), true );

        config.getNodeIds().remove( node.getId() );

        trackerOperation.addLog( "Updating db..." );

        manager.saveConfig( config );
        trackerOperation.addLogDone(
                SharkClusterConfig.PRODUCT_KEY + " is uninstalled from node " + node.getHostname() + " successfully." );
    }


    private void addNode() throws ClusterException
    {
        //check if node is in the cluster
        if ( config.getNodeIds().contains( node.getId() ) )
        {
            throw new ClusterException( String.format( "Node %s already belongs to this cluster", hostname ) );
        }

        SparkClusterConfig sparkConfig = manager.getSparkManager().getCluster( config.getSparkClusterName() );
        if ( sparkConfig == null )
        {
            throw new ClusterException(
                    String.format( "Underlying Spark cluster '%s' not found.", config.getSparkClusterName() ) );
        }

        EnvironmentContainerHost sparkMaster;
        try
        {
            sparkMaster = environment.getContainerHostById( sparkConfig.getMasterNodeId() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            throw new ClusterException(
                    String.format( "Master node not found in environment by id %s", sparkConfig.getMasterNodeId() ) );
        }

        if ( !sparkConfig.getAllNodesIds().contains( node.getId() ) )
        {
            throw new ClusterException( "Node does not belong to Spark cluster" );
        }

        //check if node already belongs to some existing Shark cluster
        List<SharkClusterConfig> clusters = manager.getClusters();

        for ( SharkClusterConfig cluster : clusters )
        {
            if ( cluster.getNodeIds().contains( node.getId() ) )
            {
                throw new ClusterException(
                        String.format( "Node %s already belongs to Shark cluster %s", node.getHostname(),
                                cluster.getClusterName() ) );
            }
        }

        trackerOperation.addLog( "Checking prerequisites..." );

        CommandResult result = executeCommand( node, manager.getCommands().getCheckInstalledCommand() );

        boolean install = !result.getStdOut().contains( Commands.PACKAGE_NAME );

        if ( install )
        {
            trackerOperation.addLog( "Installing Shark..." );

            executeCommand( node, manager.getCommands().getInstallCommand() );
        }

        trackerOperation.addLog( "Setting Master IP..." );

        executeCommand( node, manager.getCommands().getSetMasterIPCommand( sparkMaster ) );

        trackerOperation.addLog( "Updating db..." );

        config.getNodeIds().add( node.getId() );

        manager.saveConfig( config );
        trackerOperation.addLogDone(
                SharkClusterConfig.PRODUCT_KEY + " is installed on node " + node.getHostname() + " successfully." );

        //subscribe to alerts
        try
        {
            manager.subscribeToAlerts( node );
        }
        catch ( MonitorException e )
        {
            throw new ClusterException( "Failed to subscribe to alerts: " + e.getMessage() );
        }
    }


    public CommandResult executeCommand( EnvironmentContainerHost host, RequestBuilder command ) throws ClusterException
    {

        return executeCommand( host, command, false );
    }


    public CommandResult executeCommand( EnvironmentContainerHost host, RequestBuilder command, boolean skipError )
            throws ClusterException
    {

        CommandResult result = null;
        try
        {
            result = host.execute( command );
        }
        catch ( CommandException e )
        {
            if ( skipError )
            {
                trackerOperation
                        .addLog( String.format( "Error on container %s: %s", host.getHostname(), e.getMessage() ) );
            }
            else
            {
                throw new ClusterException( e );
            }
        }
        if ( skipError )
        {
            if ( result != null && !result.hasSucceeded() )
            {
                trackerOperation.addLog( String.format( "Error on container %s: %s", host.getHostname(),
                        result.hasCompleted() ? result.getStdErr() : "Command timed out" ) );
            }
        }
        else
        {
            if ( !result.hasSucceeded() )
            {
                throw new ClusterException( String.format( "Error on container %s: %s", host.getHostname(),
                        result.hasCompleted() ? result.getStdErr() : "Command timed out" ) );
            }
        }
        return result;
    }
}
