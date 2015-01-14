package org.safehaus.subutai.plugin.mongodb.impl;


import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.protocol.Criteria;
import org.safehaus.subutai.common.protocol.PlacementStrategy;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.core.peer.api.ContainerHost;
import org.safehaus.subutai.plugin.common.api.ClusterConfigurationException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.mongodb.api.MongoClusterConfig;
import org.safehaus.subutai.plugin.mongodb.api.MongoConfigNode;
import org.safehaus.subutai.plugin.mongodb.api.MongoDataNode;
import org.safehaus.subutai.plugin.mongodb.api.MongoException;
import org.safehaus.subutai.plugin.mongodb.api.MongoRouterNode;
import org.safehaus.subutai.plugin.mongodb.api.NodeType;
import org.safehaus.subutai.plugin.mongodb.impl.common.Commands;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.collect.Range;
import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;


/**
 * This is a mongodb cluster setup strategy.
 */
public class MongoDbSetupStrategy implements ClusterSetupStrategy
{

    private MongoImpl mongoManager;
    private TrackerOperation po;
    private MongoClusterConfigImpl config;
    private Environment environment;


    public MongoDbSetupStrategy( Environment environment, MongoClusterConfig config, TrackerOperation po,
                                 MongoImpl mongoManager )
    {

        Preconditions.checkNotNull( environment, "Environment is null" );
        Preconditions.checkNotNull( config, "Cluster config is null" );
        Preconditions.checkNotNull( po, "Product operation tracker is null" );
        Preconditions.checkNotNull( mongoManager, "Mongo manager is null" );

        this.environment = environment;
        this.mongoManager = mongoManager;
        this.po = po;
        this.config = ( MongoClusterConfigImpl ) config;
    }


    public static PlacementStrategy getNodePlacementStrategyByNodeType( NodeType nodeType )
    {
        switch ( nodeType )
        {
            case CONFIG_NODE:
                return new PlacementStrategy( "BEST_SERVER", Sets.newHashSet( new Criteria( "MORE_RAM", true ) ) );

            case ROUTER_NODE:
                return new PlacementStrategy( "BEST_SERVER", Sets.newHashSet( new Criteria( "MORE_CPU", true ) ) );

            case DATA_NODE:
                return new PlacementStrategy( "BEST_SERVER", Sets.newHashSet( new Criteria( "MORE_HDD", true ) ) );

            default:
                return new PlacementStrategy( "ROUND_ROBIN" );
        }
    }


    @Override
    public MongoClusterConfig setup() throws ClusterSetupException
    {

        if ( Strings.isNullOrEmpty( config.getClusterName() ) ||
                Strings.isNullOrEmpty( config.getDomainName() ) ||
                Strings.isNullOrEmpty( config.getReplicaSetName() ) ||
                Strings.isNullOrEmpty( config.getTemplateName() ) ||
                !Sets.newHashSet( 1, 2, 3 ).contains( config.getNumberOfConfigServers() ) ||
                !Range.closed( 1, 3 ).contains( config.getNumberOfRouters() ) ||
                !Sets.newHashSet( 1, 2, 3, 4, 5, 6, 7 ).contains( config.getNumberOfDataNodes() ) ||
                !Range.closed( 1024, 65535 ).contains( config.getCfgSrvPort() ) ||
                !Range.closed( 1024, 65535 ).contains( config.getRouterPort() ) ||
                !Range.closed( 1024, 65535 ).contains( config.getDataNodePort() ) )
        {
            throw new ClusterSetupException( "Malformed cluster configuration" );
        }

        if ( mongoManager.getCluster( config.getClusterName() ) != null )
        {
            throw new ClusterSetupException(
                    String.format( "Cluster with name '%s' already exists", config.getClusterName() ) );
        }

        if ( environment.getContainerHosts().isEmpty() )
        {
            throw new ClusterSetupException( "Environment has no nodes" );
        }

        int totalNodesRequired =
                config.getNumberOfRouters() + config.getNumberOfConfigServers() + config.getNumberOfDataNodes();
        if ( config.getConfigHostIds().size() + config.getRouterHostIds().size() + config.getDataHostIds().size()
                < totalNodesRequired )
        {
            throw new ClusterSetupException(
                    String.format( "Environment needs to have %d but has %d nodes", totalNodesRequired,
                            environment.getContainerHosts().size() ) );
        }

        Set<ContainerHost> mongoContainers = new HashSet<>();
        Set<ContainerHost> mongoEnvironmentContainers = new HashSet<>();

        //Run through all environment container hosts
        // to check if mongo is installed otherwise install it
        for ( ContainerHost container : environment.getContainerHosts() )
        {
            try
            {
                CommandResult commandResult =
                        container.execute( new RequestBuilder( Commands.checkIfMongoInstalled().getCommand() ) );

                if ( !"install ok installed".equals( commandResult.getStdOut() ) )
                {
                    CommandResult installationResult =
                            container.execute( new RequestBuilder( Commands.installMongoCommand().getCommand() ) );

                    if ( !installationResult.hasSucceeded() )
                    {
                        throw new ClusterSetupException(
                                String.format( "Couldn't install Mongo on container: %s-%s; errorMsg: %s",
                                        container.getHostname(), container.getEnvironmentId(),
                                        installationResult.getStdErr() ) );
                    }
                }
                mongoContainers.add( container );
                mongoEnvironmentContainers.add( container );
            }
            catch ( CommandException e )
            {
                throw new ClusterSetupException( e.toString() );
            }
        }

        Set<MongoConfigNode> configServers = new HashSet<>();
        Set<MongoRouterNode> routers = new HashSet<>();
        Set<MongoDataNode> dataNodes = new HashSet<>();
        for ( ContainerHost environmentContainer : mongoEnvironmentContainers )
        {
            if ( config.getConfigHostIds().contains( environmentContainer.getId() ) )
            {
                MongoConfigNode mongoConfigNode =
                        new MongoConfigNodeImpl( environmentContainer, config.getDomainName(), config.getCfgSrvPort() );
                configServers.add( mongoConfigNode );
            }
            if ( config.getRouterHostIds().contains( environmentContainer.getId() ) )
            {
                MongoRouterNode mongoRouterNode =
                        new MongoRouterNodeImpl( environmentContainer, config.getDomainName(), config.getRouterPort(),
                                config.getCfgSrvPort() );
                routers.add( mongoRouterNode );
            }
            if ( config.getDataHostIds().contains( environmentContainer.getId() ) )
            {
                MongoDataNode mongoDataNode =
                        new MongoDataNodeImpl( environmentContainer, config.getDomainName(), config.getDataNodePort() );
                dataNodes.add( mongoDataNode );
            }
        }

        mongoContainers.removeAll( configServers );
        mongoContainers.removeAll( routers );
        mongoContainers.removeAll( dataNodes );

        if ( configServers.size() < config.getNumberOfConfigServers() )
        {
            //take necessary number of nodes at random
            int numNeededMore = config.getNumberOfConfigServers() - configServers.size();
            Iterator<ContainerHost> it = mongoContainers.iterator();
            for ( int i = 0; i < numNeededMore; i++ )
            {
                ContainerHost environmentContainer = it.next();
                MongoConfigNode mongoConfigNode =
                        new MongoConfigNodeImpl( environmentContainer, config.getDomainName(), config.getCfgSrvPort() );
                configServers.add( mongoConfigNode );
                it.remove();
            }
        }

        if ( routers.size() < config.getNumberOfRouters() )
        {
            //take necessary number of nodes at random
            int numNeededMore = config.getNumberOfRouters() - routers.size();
            Iterator<ContainerHost> it = mongoContainers.iterator();
            for ( int i = 0; i < numNeededMore; i++ )
            {
                ContainerHost environmentContainer = it.next();
                MongoRouterNode mongoRouterNode =
                        new MongoRouterNodeImpl( environmentContainer, config.getDomainName(), config.getRouterPort(),
                                config.getCfgSrvPort() );
                routers.add( mongoRouterNode );
                it.remove();
            }
        }

        if ( dataNodes.size() < config.getNumberOfDataNodes() )
        {
            //take necessary number of nodes at random
            int numNeededMore = config.getNumberOfDataNodes() - dataNodes.size();
            Iterator<ContainerHost> it = mongoContainers.iterator();
            for ( int i = 0; i < numNeededMore; i++ )
            {
                ContainerHost environmentContainer = it.next();
                MongoDataNode mongoDataNode =
                        new MongoDataNodeImpl( environmentContainer, config.getDomainName(), config.getRouterPort() );
                dataNodes.add( mongoDataNode );
                it.remove();
            }
        }

        config.setConfigServers( configServers );
        config.setRouterServers( routers );
        config.setDataNodes( dataNodes );


        try
        {
            configureMongoCluster();
        }
        catch ( ClusterConfigurationException e )
        {
            throw new ClusterSetupException( e.getMessage() );
        }

        po.addLog( "Saving cluster information to database..." );

        Gson gson = new GsonBuilder().serializeNulls().setPrettyPrinting().disableHtmlEscaping()
                                     .excludeFieldsWithoutExposeAnnotation().create();

        String jsonConfig = gson.toJson( config.prepare() );
        mongoManager.getPluginDAO().saveInfo( MongoClusterConfig.PRODUCT_KEY, config.getClusterName(), jsonConfig );
        po.addLog( "Cluster information saved to database" );

        return config;
    }


    private void configureMongoCluster() throws ClusterConfigurationException
    {

        po.addLog( "Configuring cluster..." );
        try
        {
            for ( MongoDataNode dataNode : config.getDataNodes() )
            {
                po.addLog( "Setting replicaSetname: " + dataNode.getHostname() );
                dataNode.setReplicaSetName( config.getReplicaSetName() );
            }

            for ( MongoConfigNode configNode : config.getConfigServers() )
            {
                po.addLog( "Starting config node: " + configNode.getHostname() );
                configNode.start( config );
            }

            for ( MongoRouterNode routerNode : config.getRouterServers() )
            {
                po.addLog( "Starting router node: " + routerNode.getHostname() );
                routerNode.setConfigServers( config.getConfigServers() );
                routerNode.start( config );
            }

            for ( MongoDataNode dataNode : config.getDataNodes() )
            {
                po.addLog( "Stopping data node: " + dataNode.getHostname() );
                dataNode.stop();
            }

            MongoDataNode primaryDataNode = null;
            for ( MongoDataNode dataNode : config.getDataNodes() )
            {
                po.addLog( "Starting data node: " + dataNode.getHostname() );
                dataNode.start( config );
                if ( primaryDataNode == null )
                {
                    primaryDataNode = dataNode;
                    primaryDataNode.initiateReplicaSet();
                    po.addLog( "Primary data node: " + dataNode.getHostname() );
                }
                else
                {
                    po.addLog( "registering secondary data node: " + dataNode.getHostname() );
                    primaryDataNode.registerSecondaryNode( dataNode );
                }
            }
        }
        catch ( MongoException e )
        {
            e.printStackTrace();
            throw new ClusterConfigurationException( e );
        }

        config.setEnvironmentId( environment.getId() );
        po.addLog( String.format( "Cluster %s configured successfully.", config.getClusterName() ) );
    }
}
