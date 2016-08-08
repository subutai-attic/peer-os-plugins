package io.subutai.plugin.zookeeper.impl;


import java.util.HashSet;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.settings.Common;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.plugincommon.api.ClusterConfigurationException;
import io.subutai.core.plugincommon.api.ClusterSetupException;
import io.subutai.core.plugincommon.api.ClusterSetupStrategy;
import io.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;


/**
 * This is a standalone zk cluster setup strategy.
 */
public class ZookeeperStandaloneSetupStrategy implements ClusterSetupStrategy
{

    private final ZookeeperClusterConfig zookeeperClusterConfig;
    private final ZookeeperImpl zookeeperManager;
    private final TrackerOperation po;
    private final Environment environment;


    public ZookeeperStandaloneSetupStrategy( final Environment environment,
                                             final ZookeeperClusterConfig zookeeperClusterConfig, TrackerOperation po,
                                             ZookeeperImpl zookeeperManager )
    {
        Preconditions.checkNotNull( environment, "Environment is null" );
        Preconditions.checkNotNull( zookeeperClusterConfig, "Cluster config is null" );
        Preconditions.checkNotNull( po, "Product operation tracker is null" );
        Preconditions.checkNotNull( zookeeperManager, "ZK manager is null" );

        this.zookeeperClusterConfig = zookeeperClusterConfig;
        this.po = po;
        this.zookeeperManager = zookeeperManager;
        this.environment = environment;
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

        Set<EnvironmentContainerHost> zookeeperNodes = new HashSet<>();
        for ( EnvironmentContainerHost containerHost : environment.getContainerHosts() )
        {
            try
            {
                CommandResult commandResult = containerHost.execute( new RequestBuilder(
                        String.format( "dpkg -l | grep %s",
                                Common.PACKAGE_PREFIX + ZookeeperClusterConfig.PRODUCT_NAME ) ) );

                if ( commandResult.getStdOut().contains( Common.PACKAGE_PREFIX + ZookeeperClusterConfig.PRODUCT_NAME ) )
                {
                    zookeeperNodes.add( containerHost );
                }
            }
            catch ( CommandException e )
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

        Set<String> zookeeperIDs = new HashSet<>();
        for ( EnvironmentContainerHost containerHost : zookeeperNodes )
        {
            zookeeperIDs.add( containerHost.getId() );
        }
        zookeeperClusterConfig.setNodes( zookeeperIDs );

        try
        {

            //check if node agent is connected
            for ( ContainerHost node : zookeeperNodes )
            {
                if ( environment.getContainerHostByHostname( node.getHostname() ) == null )
                {
                    throw new ClusterSetupException( String.format( "Node %s is not connected", node.getHostname() ) );
                }
            }

            new ClusterConfiguration( zookeeperManager, po ).configureCluster( zookeeperClusterConfig, environment );

            po.addLog( "Saving cluster information to database..." );

            zookeeperClusterConfig.setEnvironmentId( environment.getId() );

            zookeeperManager.getPluginDAO()
                            .saveInfo( ZookeeperClusterConfig.PRODUCT_KEY, zookeeperClusterConfig.getClusterName(),
                                    zookeeperClusterConfig );
            po.addLog( "Cluster information saved to database" );
        }
        catch ( ClusterConfigurationException | ContainerHostNotFoundException ex )
        {
            throw new ClusterSetupException( ex.getMessage() );
        }

        return zookeeperClusterConfig;
    }
}
