package org.safehaus.subutai.plugin.hipi.impl;


import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.settings.Common;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.common.util.CollectionUtil;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.core.peer.api.CommandUtil;
import org.safehaus.subutai.core.peer.api.ContainerHost;
import org.safehaus.subutai.plugin.common.api.ClusterException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.common.api.ConfigBase;
import org.safehaus.subutai.plugin.common.api.NodeOperationType;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.hipi.api.HipiConfig;

import com.google.common.base.Strings;


public class HipiSetupStrategy implements ClusterSetupStrategy
{
    private Environment environment;
    final HipiImpl manager;
    final HipiConfig config;
    final TrackerOperation trackerOperation;
    private Set<ContainerHost> nodes;
    CommandUtil commandUtil = new CommandUtil();


    public HipiSetupStrategy( HipiImpl manager, HipiConfig config, TrackerOperation trackerOperation )
    {
        this.manager = manager;
        this.config = config;
        this.trackerOperation = trackerOperation;
    }


    @Override
    public ConfigBase setup() throws ClusterSetupException
    {
        check();

        configure();

        return config;
    }


    private void check() throws ClusterSetupException
    {
        if ( Strings.isNullOrEmpty( config.getClusterName() ) )
        {
            throw new ClusterSetupException( "Invalid cluster name" );
        }
        if ( Strings.isNullOrEmpty( config.getHadoopClusterName() ) )
        {
            throw new ClusterSetupException( "Invalid Hadoop cluster name" );
        }

        if ( CollectionUtil.isCollectionEmpty( config.getNodes() ) )
        {
            throw new ClusterSetupException( "Nodes are not specified" );
        }

        if ( manager.getCluster( config.getClusterName() ) != null )
        {
            throw new ClusterSetupException(
                    String.format( "Cluster with name '%s' already exists", config.getClusterName() ) );
        }

        HadoopClusterConfig hadoopClusterConfig =
                manager.getHadoopManager().getCluster( config.getHadoopClusterName() );

        if ( hadoopClusterConfig == null )
        {
            throw new ClusterSetupException(
                    String.format( "Hadoop cluster %s not found", config.getHadoopClusterName() ) );
        }

        environment =manager.getEnvironmentManager().getEnvironmentByUUID( hadoopClusterConfig.getEnvironmentId() );

        if ( environment == null )
        {
            throw new ClusterSetupException( "Environment not found" );
        }

        if ( !hadoopClusterConfig.getAllNodes().containsAll( config.getNodes() ) )
        {
            throw new ClusterSetupException(
                    String.format( "Not all nodes belong to Hadoop cluster %s", config.getHadoopClusterName() ));
        }

        nodes = environment.getContainerHostsByIds( config.getNodes() );

        if ( nodes.size() < config.getNodes().size() )
        {
            throw new ClusterSetupException(
                    String.format( "Found only %d nodes in environment whereas %d expected", nodes.size(),
                            config.getNodes().size() ) );
        }

        List<HipiConfig> hipiClusters = manager.getClusters();

        for ( HipiConfig cluster : hipiClusters )
        {
            for ( ContainerHost node : nodes )
            {
                if ( cluster.getNodes().contains( node.getId() ) )
                {
                    throw new ClusterSetupException(
                            String.format( "Node %s already belongs to cluster %s", node.getHostname(),
                                    cluster.getClusterName() ) );
                }
            }
        }

        for ( ContainerHost node : nodes )
        {
            if ( !node.isConnected() )
            {
                throw new ClusterSetupException( String.format( "Node %s is not connected", node.getHostname() ) );
            }

            try
            {
                RequestBuilder statusCommand = new RequestBuilder( CommandFactory.build( NodeOperationType.CHECK_INSTALLATION ) );
                CommandResult result = commandUtil.execute( statusCommand, node );
                if ( result.getStdOut().contains( HipiConfig.PRODUCT_PACKAGE ) )
                {
                    trackerOperation.addLog(
                            String.format( "Node %s already has Hipi installed. Omitting this node from installation",
                                    node.getHostname() ) );
                    config.getNodes().remove( node.getId() );
                }
                else if ( !result.getStdOut()
                                 .contains( Common.PACKAGE_PREFIX + HadoopClusterConfig.PRODUCT_NAME.toLowerCase() ) )
                {
                    trackerOperation.addLog(
                            String.format( "Node %s has no Hadoop installation. Omitting this node from installation",
                                    node.getHostname() ) );
                    config.getNodes().remove( node.getId() );
                }
            }
            catch ( CommandException e )
            {
                throw new ClusterSetupException( "Failed to check presence of installed subutai packages" );
            }
        }
        if ( config.getNodes().isEmpty() )
        {
            throw new ClusterSetupException( "No nodes eligible for installation" );
        }
    }



    private void configure() throws ClusterSetupException
    {

        trackerOperation.addLog( "Updating Hipi..." );
        config.setEnvironmentId( environment.getId() );

        try
        {
            manager.saveConfig( config );
        }
        catch ( ClusterException e )
        {
            throw new ClusterSetupException( e );
        }

        trackerOperation.addLog( "Cluster info saved to DB\nInstalling Hipi..." );



        for ( ContainerHost node : nodes )
        {
            try
            {
                RequestBuilder installCommand = new RequestBuilder( CommandFactory.build( NodeOperationType.INSTALL ) )
                        .withTimeout( 300 );
                commandUtil.execute( installCommand, node );
            }
            catch ( CommandException e )
            {
                throw new ClusterSetupException(
                        String.format( "Error while installing Hipi on container %s; %s", node.getHostname(),
                                e.getMessage() ) );
            }

        }
        boolean saved = manager.getPluginDao().saveInfo( HipiConfig.PRODUCT_KEY, config.getClusterName(), config );

        if ( saved )
        {
            trackerOperation.addLog( "Installation info successfully saved" );
        }
        else
        {
            throw new ClusterSetupException( "Failed to save installation info" );
        }
    }
}
