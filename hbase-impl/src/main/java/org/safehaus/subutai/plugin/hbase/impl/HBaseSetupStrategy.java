package org.safehaus.subutai.plugin.hbase.impl;


import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.CommandUtil;
import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.peer.Host;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.common.util.CollectionUtil;
import org.safehaus.subutai.plugin.common.api.ClusterConfigurationException;
import org.safehaus.subutai.plugin.common.api.ClusterException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupException;
import org.safehaus.subutai.plugin.common.api.ConfigBase;
import org.safehaus.subutai.plugin.hadoop.api.Hadoop;
import org.safehaus.subutai.plugin.hbase.api.HBaseConfig;
import org.safehaus.subutai.plugin.hbase.impl.handler.ClusterOperationHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;


public class HBaseSetupStrategy
{

    private static final Logger LOG = LoggerFactory.getLogger( HBaseSetupStrategy.class.getName() );
    private Hadoop hadoop;
    private HBaseImpl manager;
    private HBaseConfig config;
    private Environment environment;
    private TrackerOperation trackerOperation;
    private CommandUtil commandUtil;


    public HBaseSetupStrategy( HBaseImpl manager, Hadoop hadoop, HBaseConfig config, Environment environment,
                               TrackerOperation po )
    {
        this.manager = manager;
        this.config = config;
        this.environment = environment;
        this.trackerOperation = po;
        this.hadoop = hadoop;
        this.commandUtil = new CommandUtil();
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

        try
        {
            Set<Host> hostSet = getHosts( config, environment );
            Map<Host, CommandResult> resultMap = commandUtil.executeParallel( Commands.getInstallCommand(), hostSet );
            if ( ClusterOperationHandler.isAllSuccessful( resultMap, hostSet ) )
            {
                trackerOperation.addLog( "HBase debian package is installed on all containers successfully" );
            }
        }
        catch ( CommandException e )
        {
            LOG.error( "Error while executing commands in parallel", e );
            e.printStackTrace();
        }

        try
        {
            new ClusterConfiguration( trackerOperation, manager, hadoop ).configureCluster( config, environment );
        }
        catch ( ClusterConfigurationException e )
        {
            throw new ClusterSetupException( e.getMessage() );
        }


        trackerOperation.addLog( "Saving to db..." );
        try
        {
            manager.saveConfig( config );
            trackerOperation.addLog( "Installation info successfully saved" );
        }
        catch ( ClusterException e )
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
    }


    public static Set<Host> getHosts( HBaseConfig config, Environment environment )
    {
        Set<Host> hosts = new HashSet<>();
        for ( UUID uuid : config.getAllNodes() )
        {
            try
            {
                hosts.add( environment.getContainerHostById( uuid ) );
            }
            catch ( ContainerHostNotFoundException e )
            {
                e.printStackTrace();
            }
        }
        return hosts;
    }
}
