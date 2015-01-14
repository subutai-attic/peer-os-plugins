package org.safehaus.subutai.plugin.zookeeper.impl;


import java.util.HashSet;
import java.util.Set;

import org.safehaus.subutai.common.protocol.PlacementStrategy;
import org.safehaus.subutai.common.settings.Common;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.core.peer.api.ContainerHost;
import org.safehaus.subutai.core.peer.api.PeerException;
import org.safehaus.subutai.plugin.common.api.ClusterConfigurationException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;

import com.google.common.base.Strings;


/**
 * Created by talas on 1/14/15.
 */
public class ZookeeperOverEnvironmentSetupStrategy implements ClusterSetupStrategy
{

    private final ZookeeperClusterConfig zookeeperClusterConfig;
    private final ZookeeperImpl zookeeperManager;
    private final TrackerOperation po;
    private final Environment environment;


    public ZookeeperOverEnvironmentSetupStrategy( final Environment environment, final ZookeeperClusterConfig config,
                                                  final TrackerOperation po, final ZookeeperImpl zookeeper )
    {
        this.zookeeperClusterConfig = config;
        this.zookeeperManager = zookeeper;
        this.po = po;
        this.environment = environment;
    }


    public static PlacementStrategy getNodePlacementStrategy()
    {
        return new PlacementStrategy( "ROUND_ROBIN" );
    }


    @Override
    public ZookeeperClusterConfig setup() throws ClusterSetupException
    {
        if ( Strings.isNullOrEmpty( zookeeperClusterConfig.getClusterName() ) ||
                Strings.isNullOrEmpty( zookeeperClusterConfig.getTemplateName() ) ||
                zookeeperClusterConfig.getNumberOfNodes() <= 0 )
        {
            throw new ClusterSetupException( "Malformed configuration" );
        }

        if ( zookeeperManager.getCluster( zookeeperClusterConfig.getClusterName() ) != null )
        {
            throw new ClusterSetupException(
                    String.format( "Cluster with name '%s' already exists", zookeeperClusterConfig.getClusterName() ) );
        }

        if ( environment.getContainerHosts().size() < zookeeperClusterConfig.getNumberOfNodes() )
        {
            throw new ClusterSetupException( String.format( "Environment needs to have %d nodes but has only %d nodes",
                    zookeeperClusterConfig.getNumberOfNodes(), environment.getContainerHosts().size() ) );
        }

        // Install zoo only on selected containers/apparently
        // on those containers where it isn't installed
        Set<ContainerHost> zookeeperNodes = new HashSet<>();
        for ( ContainerHost containerHost : environment.getContainerHosts() )
        {
            try
            {
                if ( containerHost.getTemplate().getProducts()
                                  .contains( Common.PACKAGE_PREFIX + ZookeeperClusterConfig.PRODUCT_NAME )
                        && zookeeperClusterConfig.getNodes().contains( containerHost.getId() ) )
                {
                    zookeeperNodes.add( containerHost );
                }
            }
            catch ( PeerException e )
            {
                e.printStackTrace();
            }
        }

        if ( zookeeperNodes.size() < zookeeperClusterConfig.getNumberOfNodes() )
        {
            throw new ClusterSetupException( String.format(
                    "Environment needs to have %d nodes with ZK installed but has only %d nodes with Zk installed",
                    zookeeperClusterConfig.getNumberOfNodes(), zookeeperNodes.size() ) );
        }


        //check if node agent is connected
        for ( ContainerHost node : zookeeperNodes )
        {
            if ( environment.getContainerHostByHostname( node.getHostname() ) == null )
            {
                throw new ClusterSetupException( String.format( "Node %s is not connected", node.getHostname() ) );
            }
        }

        try
        {

            new ClusterConfiguration( zookeeperManager, po ).configureCluster( zookeeperClusterConfig, environment );
        }
        catch ( ClusterConfigurationException ex )
        {
            throw new ClusterSetupException( ex.getMessage() );
        }

        po.addLog( "Saving cluster information to database..." );

        zookeeperClusterConfig.setEnvironmentId( environment.getId() );

        zookeeperManager.getPluginDAO()
                        .saveInfo( ZookeeperClusterConfig.PRODUCT_KEY, zookeeperClusterConfig.getClusterName(),
                                zookeeperClusterConfig );
        po.addLog( "Cluster information saved to database" );


        return zookeeperClusterConfig;
    }
}
