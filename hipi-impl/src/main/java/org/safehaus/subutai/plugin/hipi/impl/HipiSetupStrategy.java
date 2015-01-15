package org.safehaus.subutai.plugin.hipi.impl;


import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.common.util.CollectionUtil;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.core.peer.api.ContainerHost;
import org.safehaus.subutai.plugin.common.api.ClusterSetupException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.common.api.ConfigBase;
import org.safehaus.subutai.plugin.common.api.NodeOperationType;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.hipi.api.HipiConfig;

import com.google.common.base.Strings;


public class HipiSetupStrategy implements ClusterSetupStrategy
{
    final HipiImpl manager;
    final HipiConfig config;
    final TrackerOperation trackerOperation;
    private Set<ContainerHost> nodes;


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

        final Environment environment =
                manager.getEnvironmentManager().getEnvironmentByUUID( hadoopClusterConfig.getEnvironmentId() );

        if ( environment == null )
        {
            throw new ClusterSetupException( "Environment not found" );
        }

        if ( !hadoopClusterConfig.getAllNodes().containsAll( config.getNodes() ) )
        {
            throw new ClusterSetupException( "Not all nodes belong to Hadoop cluster" );
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
        }
    }


    private void configure() throws ClusterSetupException
    {
        trackerOperation.addLog( "Checking prerequisites..." );
        String statusCommand = CommandFactory.build( NodeOperationType.CHECK_INSTALLATION );
        for ( Iterator<ContainerHost> it = nodes.iterator(); it.hasNext(); )
        {
            ContainerHost node = it.next();
            try
            {
                CommandResult result = node.execute( new RequestBuilder( statusCommand ) );
                if ( result.hasSucceeded() && result.getStdOut().contains( CommandFactory.PACKAGE_NAME ) )
                {
                    trackerOperation
                            .addLog( String.format( "Node %s has already Hipi installed.", node.getHostname() ) );
                    it.remove();
                }
                else
                {
                    throw new ClusterSetupException( "Failed to check installed packages on " + node.getHostname() );
                }
            }
            catch ( CommandException ex )
            {
                throw new ClusterSetupException( ex );
            }
        }

        trackerOperation.addLog( "Installing Hive..." );
        String installCommand = CommandFactory.build( NodeOperationType.INSTALL );
        for ( ContainerHost node : nodes )
        {
            try
            {
                CommandResult result = node.execute( new RequestBuilder( installCommand ) );

                if ( result.hasSucceeded() )
                {
                    trackerOperation.addLog( "Hipi installed on " + node.getHostname() );
                }
                else
                {
                    throw new ClusterSetupException( "Failed to install Hipi on " + node.getHostname() );
                }
            }
            catch ( CommandException ex )
            {
                throw new ClusterSetupException( ex );
            }
        }

        trackerOperation.addLog( "Saving to db..." );
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
