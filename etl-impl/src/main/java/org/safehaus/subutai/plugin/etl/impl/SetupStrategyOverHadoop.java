package org.safehaus.subutai.plugin.etl.impl;


import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.settings.Common;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.plugin.common.api.ClusterConfigurationException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupException;
import org.safehaus.subutai.plugin.common.api.ConfigBase;
import org.safehaus.subutai.plugin.common.api.NodeOperationType;
import org.safehaus.subutai.plugin.etl.api.SetupType;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.etl.api.SqoopConfig;


public class SetupStrategyOverHadoop
{
    private SqoopImpl manager;
    private SqoopConfig config;
    private Environment environment;
    private TrackerOperation trackerOperation;

    public SetupStrategyOverHadoop( SqoopImpl manager, SqoopConfig config, Environment env, TrackerOperation trackerOperation )
    {
        this.manager = manager;
        this.config = config;
        this.environment = env;
        this.trackerOperation = trackerOperation;
    }


    public ConfigBase setup() throws ClusterSetupException
    {

        checkConfig();

        //check if nodes are connected
        Set<ContainerHost> nodes = environment.getContainerHostsByIds( config.getNodes() );
        if ( nodes.size() < config.getNodes().size() )
        {
            throw new ClusterSetupException( "Fewer nodes found in the environment than expected" );
        }
        for ( ContainerHost node : nodes )
        {
            if ( !node.isConnected() )
            {
                throw new ClusterSetupException( String.format( "Node %s is not connected", node.getHostname() ) );
            }
        }

        HadoopClusterConfig hc = manager.hadoopManager.getCluster( config.getHadoopClusterName() );
        if ( hc == null )
        {
            throw new ClusterSetupException( "Could not find Hadoop cluster " + config.getHadoopClusterName() );
        }

        if ( !hc.getAllNodes().containsAll( config.getNodes() ) )
        {
            throw new ClusterSetupException(
                    "Not all nodes belong to Hadoop cluster " + config.getHadoopClusterName() );
        }
        config.setHadoopNodes( new HashSet<>( hc.getAllNodes() ) );

        // check if already installed
        String s = CommandFactory.build( NodeOperationType.STATUS, null );
        String hadoop_pack = Common.PACKAGE_PREFIX + HadoopClusterConfig.PRODUCT_NAME.toLowerCase();
        Iterator<ContainerHost> it = nodes.iterator();
        while ( it.hasNext() )
        {
            ContainerHost node = it.next();
            try
            {
                CommandResult res = node.execute( new RequestBuilder( s ) );
                if ( res.hasSucceeded() )
                {
                    if ( res.getStdOut().contains( CommandFactory.PACKAGE_NAME ) )
                    {
                        trackerOperation.addLog( String.format( "Node %s has already Sqoop installed.", node.getHostname() ) );
                        it.remove();
                    }
                    else if ( res.getStdOut().contains( hadoop_pack ) )
                    {
                        throw new ClusterSetupException( "Hadoop not installed on node " + node.getHostname() );
                    }
                }
                else
                {
                    throw new ClusterSetupException( "Failed to check installed packges on " + node.getHostname() );
                }
            }
            catch ( CommandException ex )
            {
                throw new ClusterSetupException( ex );
            }
        }
        if ( nodes.isEmpty() )
        {
            throw new ClusterSetupException( "No nodes to install Sqoop" );
        }

        // installation
        s = CommandFactory.build( NodeOperationType.INSTALL, null );
        it = nodes.iterator();
        while ( it.hasNext() )
        {
            ContainerHost node = it.next();
            try
            {
                CommandResult res = node.execute( new RequestBuilder( s ) );
                if ( res.hasSucceeded() )
                {
                    trackerOperation.addLog( "Sqoop installed on " + node.getHostname() );
                }
                else
                {
                    throw new ClusterSetupException( "Failed to install Sqoop on " + node.getHostname() );
                }
            }
            catch ( CommandException ex )
            {
                throw new ClusterSetupException( ex );
            }
        }

        trackerOperation.addLog( "Saving to db..." );
        boolean saved = manager.getPluginDao().saveInfo( SqoopConfig.PRODUCT_KEY, config.getClusterName(), config );
        if ( saved )
        {
            trackerOperation.addLog( "Installation info successfully saved" );
            configure();
        }
        else
        {
            throw new ClusterSetupException( "Failed to save installation info" );
        }

        return config;
    }

    public void checkConfig() throws ClusterSetupException
    {

        String m = "Invalid configuration: ";

        if ( config.getClusterName() == null || config.getClusterName().isEmpty() )
        {
            throw new ClusterSetupException( m + "name is not specified" );
        }

        if ( manager.getCluster( config.getClusterName() ) != null )
        {
            throw new ClusterSetupException(
                    m + String.format( "Sqoop installation already exists: %s", config.getClusterName() ) );
        }

        if ( environment == null )
        {
            throw new ClusterSetupException( "Environment not specified" );
        }

        if ( config.getSetupType() == SetupType.OVER_HADOOP )
        {
            if ( config.getNodes() == null || config.getNodes().isEmpty() )
            {
                throw new ClusterSetupException( m + "Target nodes not specified" );
            }
        }
    }

    void configure() throws ClusterSetupException
    {
        ClusterConfiguration cc = new ClusterConfiguration();
        try
        {
            cc.configureCluster( config, environment );
        }
        catch ( ClusterConfigurationException ex )
        {
            throw new ClusterSetupException( ex );
        }
    }
}

