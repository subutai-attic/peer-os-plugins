package org.safehaus.subutai.plugin.hadoop.impl.handler;


import java.util.HashSet;
import java.util.Set;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.protocol.NodeGroup;
import org.safehaus.subutai.common.protocol.PlacementStrategy;
import org.safehaus.subutai.common.settings.Common;
import org.safehaus.subutai.core.environment.api.EnvironmentManager;
import org.safehaus.subutai.core.environment.api.exception.EnvironmentBuildException;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.core.network.api.NetworkManagerException;
import org.safehaus.subutai.core.peer.api.LocalPeer;
import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.hadoop.impl.Commands;
import org.safehaus.subutai.plugin.hadoop.impl.HadoopImpl;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;


public class AddOperationHandler extends AbstractOperationHandler<HadoopImpl, HadoopClusterConfig>
{

    private int nodeCount;



    public AddOperationHandler( HadoopImpl manager, String clusterName, int nodeCount )
    {
        super( manager, manager.getCluster( clusterName ) );
        this.nodeCount = nodeCount;
        trackerOperation = manager.getTracker().createTrackerOperation( HadoopClusterConfig.PRODUCT_KEY,
                String.format( "Adding node to cluster %s",  clusterName ) );
    }


    @Override
    public void run()
    {
        for ( int i=0; i<nodeCount; i++ ){
            addNode();
        }
    }


    /**
     * Steps:
     *   1) Creates a new container from hadoop template
     *   2) Include node
     */
    public void addNode(){
        LocalPeer localPeer = manager.getPeerManager().getLocalPeer();
        EnvironmentManager environmentManager = manager.getEnvironmentManager();

        /**
         * first check if there are containers in environment that is not being used in hadoop cluster,
         * if yes, then do not create new containers.
         */
        Environment environment = environmentManager.getEnvironmentByUUID( config.getEnvironmentId() );
        int numberOfContainersNotBeingUsed = 0;
        boolean allContainersNotBeingUsed = false;
        for ( ContainerHost containerHost : environment.getContainerHosts() ){
            if ( ! config.getAllNodes().contains( containerHost.getId() ) ){
                allContainersNotBeingUsed = true;
                numberOfContainersNotBeingUsed++;
            }
        }

        try
        {
            if ( ( ! allContainersNotBeingUsed )  | (numberOfContainersNotBeingUsed < nodeCount) ){

                NodeGroup nodeGroup = new NodeGroup();
                String nodeGroupName = HadoopClusterConfig.PRODUCT_NAME + "_" + System.currentTimeMillis();
                nodeGroup.setName( nodeGroupName );
                nodeGroup.setLinkHosts( true );
                nodeGroup.setExchangeSshKeys( true );
                nodeGroup.setDomainName( Common.DEFAULT_DOMAIN_NAME );
                nodeGroup.setTemplateName( HadoopClusterConfig.TEMPLATE_NAME );
                nodeGroup.setPlacementStrategy( new PlacementStrategy( "ROUND_ROBIN" ) );
                nodeGroup.setNumberOfNodes( nodeCount - numberOfContainersNotBeingUsed );

                GsonBuilder builder = new GsonBuilder();
                Gson gson = builder.create();
                String ngJSON = gson.toJson(nodeGroup);

                if ( numberOfContainersNotBeingUsed > 0 ){
                    trackerOperation.addLog( "Using " + numberOfContainersNotBeingUsed + " existing containers and creating"
                            + (nodeCount - numberOfContainersNotBeingUsed ) + " containers." );
                }
                else{
                    trackerOperation.addLog( "Creating new containers..." );
                }
                environmentManager.createAdditionalContainers( config.getEnvironmentId(), ngJSON, localPeer );


            }
            else{
                trackerOperation.addLog( "Using existing containers that are not taking role in cluster" );
            }


            // update cluster configuration on DB
            int count = 0;
            Set<ContainerHost> newlyCreatedContainers = new HashSet<>();
            environment = environmentManager.getEnvironmentByUUID( config.getEnvironmentId() );
            for ( ContainerHost containerHost : environmentManager.getEnvironmentByUUID( config.getEnvironmentId() ).getContainerHosts() )
            {
                if ( ! config.getAllNodes().contains( containerHost.getId() ) ){
                    if ( count <= nodeCount ){
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
                e.printStackTrace();
            }

            // include newly created containers to existing hadoop cluster
            for ( ContainerHost containerHost : newlyCreatedContainers ){
                trackerOperation.addLog( "Configuring " + containerHost.getHostname()   );
                configureSlaveNode( config, containerHost, environment );
                manager.includeNode( config, containerHost.getHostname()  );
            }
            trackerOperation.addLogDone( "Finished." );
        }
        catch ( EnvironmentBuildException e )
        {
            e.printStackTrace();
        }
    }


    /**
     * Configures new added slave node.
     *
     * @param config hadoop configuration
     * @param containerHost node to be configured
     * @param environment environment in which given container reside
     */
    private void configureSlaveNode( HadoopClusterConfig config, ContainerHost containerHost, Environment environment ){

        // Clear configuration files
        executeCommandOnContainer( containerHost, Commands.getClearMastersCommand() );
        executeCommandOnContainer( containerHost, Commands.getClearSlavesCommand() );
        // Configure NameNode
        ContainerHost namenode = environment.getContainerHostById( config.getNameNode() );
        ContainerHost jobtracker = environment.getContainerHostById( config.getJobTracker() );
        executeCommandOnContainer( containerHost, Commands.getSetMastersCommand( namenode.getHostname(), jobtracker.getHostname(), config.getReplicationFactor() ) );
    }


    private void executeCommandOnContainer( ContainerHost containerHost, String command ){
        try
        {
            containerHost.execute( new RequestBuilder( command ) );
        }
        catch ( CommandException e )
        {
            e.printStackTrace();
        }
    }

}
