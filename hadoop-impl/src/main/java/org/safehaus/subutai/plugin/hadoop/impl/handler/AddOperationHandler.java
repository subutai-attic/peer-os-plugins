package org.safehaus.subutai.plugin.hadoop.impl.handler;


import java.util.HashSet;
import java.util.Set;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentModificationException;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.environment.NodeGroup;
import org.safehaus.subutai.common.environment.Topology;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.protocol.PlacementStrategy;
import org.safehaus.subutai.common.settings.Common;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.core.network.api.NetworkManagerException;
import org.safehaus.subutai.core.peer.api.LocalPeer;
import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.hadoop.impl.Commands;
import org.safehaus.subutai.plugin.hadoop.impl.HadoopImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;


public class AddOperationHandler extends AbstractOperationHandler<HadoopImpl, HadoopClusterConfig>
{

    private int nodeCount;
    private static final Logger LOGGER = LoggerFactory.getLogger( AddOperationHandler.class );


    public AddOperationHandler( HadoopImpl manager, String clusterName, int nodeCount )
    {
        super( manager, manager.getCluster( clusterName ) );
        this.nodeCount = nodeCount;
        trackerOperation = manager.getTracker().createTrackerOperation( HadoopClusterConfig.PRODUCT_KEY,
                String.format( "Adding node to cluster %s", clusterName ) );
    }


    @Override
    public void run()
    {
        for ( int i = 0; i < nodeCount; i++ )
        {
            addNode();
        }
    }


    /**
     * Steps: 1) Creates a new container from hadoop template 2) Include node
     */
    public void addNode()
    {

        try
        {
            LocalPeer localPeer = manager.getPeerManager().getLocalPeer();
            EnvironmentManager environmentManager = manager.getEnvironmentManager();

            /**
             * first check if there are containers in environment that is not being used in hadoop cluster,
             * if yes, then do not create new containers.
             */
            Environment environment = environmentManager.findEnvironment( config.getEnvironmentId() );
            int numberOfContainersNotBeingUsed = 0;
            boolean allContainersNotBeingUsed = false;
            for ( ContainerHost containerHost : environment.getContainerHosts() )
            {
                if ( !config.getAllNodes().contains( containerHost.getId() ) )
                {
                    allContainersNotBeingUsed = true;
                    numberOfContainersNotBeingUsed++;
                }
            }

            if ( ( !allContainersNotBeingUsed ) | ( numberOfContainersNotBeingUsed < nodeCount ) )
            {

                String nodeGroupName = HadoopClusterConfig.PRODUCT_NAME + "_" + System.currentTimeMillis();
                NodeGroup nodeGroup =
                        new NodeGroup( nodeGroupName, HadoopClusterConfig.TEMPLATE_NAME,
                                nodeCount - numberOfContainersNotBeingUsed, 1, 2,
                                new PlacementStrategy( "ROUND_ROBIN" ) );
                Topology topology = new Topology();
                topology.addNodeGroupPlacement( manager.getPeerManager().getLocalPeer(), nodeGroup );

                GsonBuilder builder = new GsonBuilder();
                Gson gson = builder.create();
                String ngJSON = gson.toJson( nodeGroup );

                if ( numberOfContainersNotBeingUsed > 0 )
                {
                    trackerOperation.addLog(
                            "Using " + numberOfContainersNotBeingUsed + " existing containers and creating" + (
                                    nodeCount - numberOfContainersNotBeingUsed ) + " containers." );
                }
                else
                {
                    trackerOperation.addLog( "Creating new containers..." );
                }
                environmentManager.growEnvironment( config.getEnvironmentId(), topology, true );
            }
            else
            {
                trackerOperation.addLog( "Using existing containers that are not taking role in cluster" );
            }


            // update cluster configuration on DB
            int count = 0;
            Set<ContainerHost> newlyCreatedContainers = new HashSet<>();
            environment = environmentManager.findEnvironment( config.getEnvironmentId() );
            for ( ContainerHost containerHost : environment.getContainerHosts() )
            {
                if ( !config.getAllNodes().contains( containerHost.getId() ) )
                {
                    if ( count <= nodeCount )
                    {
                        config.getDataNodes().add( containerHost.getId() );
                        config.getTaskTrackers().add( containerHost.getId() );
                        trackerOperation.addLog( containerHost.getHostname() + " is added as slave node." );
                        count++;
                        newlyCreatedContainers.add( containerHost );
                    }
                }
            }
            manager.getPluginDAO().saveInfo( HadoopClusterConfig.PRODUCT_KEY, config.getClusterName(), config );

            // configure ssh keys
            Set<ContainerHost> allNodes = new HashSet<>();
            allNodes.addAll( newlyCreatedContainers );
            allNodes.addAll( environment.getContainerHosts() );
            try
            {
                manager.getNetworkManager().exchangeSshKeys( allNodes );
            }
            catch ( NetworkManagerException e )
            {
                logExceptionWithMessage( "Error exchanging with keys", e );
                return;
            }

            // include newly created containers to existing hadoop cluster
            for ( ContainerHost containerHost : newlyCreatedContainers )
            {
                trackerOperation.addLog( "Configuring " + containerHost.getHostname() );
                configureSlaveNode( config, containerHost, environment );
                manager.includeNode( config, containerHost.getHostname() );
            }
            trackerOperation.addLogDone( "Finished." );
        }
        catch ( EnvironmentNotFoundException | EnvironmentModificationException e )
        {
            logExceptionWithMessage( "Error executing operations with environment", e );
        }
    }


    /**
     * Configures new added slave node.
     *
     * @param config hadoop configuration
     * @param containerHost node to be configured
     * @param environment environment in which given container reside
     */
    private void configureSlaveNode( HadoopClusterConfig config, ContainerHost containerHost, Environment environment )
    {

        try
        {
            // Clear configuration files
            executeCommandOnContainer( containerHost, Commands.getClearMastersCommand() );
            executeCommandOnContainer( containerHost, Commands.getClearSlavesCommand() );
            // Configure NameNode
            ContainerHost namenode = environment.getContainerHostById( config.getNameNode() );
            ContainerHost jobtracker = environment.getContainerHostById( config.getJobTracker() );
            executeCommandOnContainer( containerHost,
                    Commands.getSetMastersCommand( namenode.getHostname(), jobtracker.getHostname(),
                            config.getReplicationFactor() ) );
        }
        catch ( ContainerHostNotFoundException e )
        {
            logExceptionWithMessage( "Error while configuring slave node and getting container host by id", e );
        }
    }


    private void executeCommandOnContainer( ContainerHost containerHost, String command )
    {
        try
        {
            containerHost.execute( new RequestBuilder( command ) );
        }
        catch ( CommandException e )
        {
            logExceptionWithMessage( "Error executing command: " + command, e );
        }
    }


    private void logExceptionWithMessage( String message, Exception e )
    {
        LOGGER.error( message, e );
        trackerOperation.addLogFailed( message );
    }
}
