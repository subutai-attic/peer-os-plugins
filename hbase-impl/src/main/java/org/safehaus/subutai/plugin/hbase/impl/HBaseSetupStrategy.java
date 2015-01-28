package org.safehaus.subutai.plugin.hbase.impl;


import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.common.util.CollectionUtil;
import org.safehaus.subutai.core.env.api.Environment;
import org.safehaus.subutai.core.env.api.exception.ContainerHostNotFoundException;
import org.safehaus.subutai.core.env.api.exception.EnvironmentNotFoundException;
import org.safehaus.subutai.plugin.common.api.ClusterConfigurationException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupException;
import org.safehaus.subutai.plugin.common.api.ConfigBase;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;
import org.safehaus.subutai.plugin.hbase.api.HBaseConfig;
import org.safehaus.subutai.plugin.hbase.api.SetupType;

import com.google.common.base.Strings;


public class HBaseSetupStrategy
{

    private Hadoop hadoop;
    private HBaseImpl manager;
    private HBaseConfig config;
    private Environment environment;
    private TrackerOperation trackerOperation;


    public HBaseSetupStrategy( HBaseImpl manager, Hadoop hadoop, HBaseConfig config, Environment environment,
                               TrackerOperation po )
    {
        this.manager = manager;
        this.config = config;
        this.environment = environment;
        this.trackerOperation = po;
        this.hadoop = hadoop;
    }


    public ConfigBase setup() throws ClusterSetupException
    {
        checkConfig();

        Set<ContainerHost> nodes = new HashSet<>();
        try
        {
            nodes = environment.getContainerHostsByIds( config.getAllNodes() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            e.printStackTrace();
        }

        if ( nodes.size() < config.getAllNodes().size() )
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

        if ( Strings.isNullOrEmpty( config.getClusterName() ) || CollectionUtil
                .isCollectionEmpty( config.getAllNodes() ) )
        {
            throw new ClusterSetupException( "Malformed configuration\nInstallation aborted" );
        }

        if ( manager.getCluster( config.getClusterName() ) != null )
        {
            throw new ClusterSetupException(
                    String.format( "Cluster with name '%s' already exists\nInstallation aborted",
                            config.getClusterName() ) );
        }

        if ( config.getAllNodes().isEmpty() )
        {
            throw new ClusterSetupException( "No nodes eligible for installation. Operation aborted" );
        }

        trackerOperation.addLog( "Checking prerequisites..." );

        // Check installed packages
        trackerOperation.addLog( "Installing HBase..." );

        for ( Iterator<ContainerHost> it = nodes.iterator(); it.hasNext(); )
        {
            ContainerHost node = it.next();
            try
            {
                CommandResult result = node.execute( Commands.getCheckInstalledCommand() );
                if ( result.hasSucceeded() && result.getStdOut().contains( Commands.PACKAGE_NAME ) )
                {
                    trackerOperation
                            .addLog( String.format( "Node %s has already HBase installed.", node.getHostname() ) );
                    it.remove();
                }
            }
            catch ( CommandException ex )
            {
                throw new ClusterSetupException( ex );
            }
        }

        if ( nodes.isEmpty() )
        {
            throw new ClusterSetupException( "No nodes eligible for installation. Operation aborted" );
        }


        for ( ContainerHost node : nodes )
        {
            try
            {
                CommandResult result = node.execute( Commands.getInstallCommand() );

                if ( result.hasSucceeded() )
                {
                    trackerOperation.addLog( "HBase installed on " + node.getHostname() );
                }
                else
                {
                    throw new ClusterSetupException( "Failed to install HBase on " + node.getHostname() );
                }
            }
            catch ( CommandException ex )
            {
                throw new ClusterSetupException( ex );
            }
        }

        try
        {
            Environment env = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );
            try
            {
                new ClusterConfiguration( trackerOperation, manager, hadoop ).configureCluster( config, env );
            }
            catch ( ClusterConfigurationException e )
            {
                throw new ClusterSetupException( e.getMessage() );
            }
        }
        catch ( ClusterSetupException | EnvironmentNotFoundException e )
        {
            trackerOperation.addLogFailed(
                    String.format( "Failed to setup cluster %s : %s", config.getClusterName(), e.getMessage() ) );
        }


        trackerOperation.addLog( "Saving to db..." );
        boolean saved = manager.getPluginDAO().saveInfo( HBaseConfig.PRODUCT_KEY, config.getClusterName(), config );

        if ( saved )
        {
            trackerOperation.addLog( "Installation info successfully saved" );
        }
        else
        {
            throw new ClusterSetupException( "Failed to save installation info" );
        }

        return config;
    }


    void checkConfig() throws ClusterSetupException
    {
        String m = "Invalid configuration: ";

        if ( config.getClusterName() == null || config.getClusterName().isEmpty() )
        {
            throw new ClusterSetupException( m + "Cluster name not specified" );
        }

        if ( manager.getCluster( config.getClusterName() ) != null )
        {
            throw new ClusterSetupException(
                    m + String.format( "Cluster '%s' already exists", config.getClusterName() ) );
        }

        if ( config.getSetupType() == SetupType.OVER_HADOOP )
        {
            if ( config.getAllNodes() == null || config.getAllNodes().isEmpty() )
            {
                throw new ClusterSetupException( m + "Target nodes not specified" );
            }
        }
    }
}
