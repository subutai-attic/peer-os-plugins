package org.safehaus.subutai.plugin.nutch.impl;


import java.util.Set;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.CommandUtil;
import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.settings.Common;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.common.util.CollectionUtil;
import org.safehaus.subutai.plugin.common.api.ClusterException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.common.api.ConfigBase;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.nutch.api.NutchConfig;

import com.google.common.base.Strings;


class NutchSetupStrategy implements ClusterSetupStrategy
{

    private Environment environment;
    private NutchImpl manager;
    private NutchConfig config;
    private TrackerOperation trackerOperation;
    Commands commands = new Commands();
    CommandUtil commandUtil = new CommandUtil();


    public NutchSetupStrategy( NutchImpl manager, NutchConfig config, TrackerOperation trackerOperation )
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
        if ( Strings.isNullOrEmpty( config.getClusterName() ) || Strings.isNullOrEmpty( config.getHadoopClusterName() )
                || CollectionUtil.isCollectionEmpty( config.getNodes() ) )
        {
            throw new ClusterSetupException( "Malformed configuration" );
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

        try
        {
            environment = manager.getEnvironmentManager().findEnvironment( hadoopClusterConfig.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            throw new ClusterSetupException( e );
        }


        //check nodes are connected
        Set<ContainerHost> nodes;
        try
        {
            nodes = environment.getContainerHostsByIds( config.getNodes() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            throw new ClusterSetupException( String.format( "Failed obtaining environment containers: %s", e ) );
        }

        for ( ContainerHost host : nodes )
        {
            if ( !host.isConnected() )
            {
                throw new ClusterSetupException( String.format( "Container %s is not connected", host.getHostname() ) );
            }
        }

        if ( !hadoopClusterConfig.getAllNodes().containsAll( config.getNodes() ) )
        {
            throw new ClusterSetupException(
                    String.format( "Not all nodes belong to Hadoop cluster %s", config.getHadoopClusterName() ) );
        }

        trackerOperation.addLog( "Checking prerequisites..." );

        for ( ContainerHost node : nodes )
        {
            try
            {
                CommandResult result = commandUtil.execute( commands.getCheckInstallationCommand(), node );
                if ( result.getStdOut().contains( NutchConfig.PRODUCT_PACKAGE ) )
                {
                    trackerOperation.addLog(
                            String.format( "Node %s already has Nutch installed. Omitting this node from installation",
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
        trackerOperation.addLog( "Updating db..." );
        //save to db
        config.setEnvironmentId( environment.getId() );
        try
        {
            manager.saveConfig( config );
        }
        catch ( ClusterException e )
        {
            throw new ClusterSetupException( e );
        }

        trackerOperation.addLog( "Cluster info saved to DB\nInstalling Nutch..." );

        Set<ContainerHost> nodes;
        try
        {
            nodes = environment.getContainerHostsByIds( config.getNodes() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            throw new ClusterSetupException( String.format( "Failed obtaining environment containers: %s", e ) );
        }

        //install nutch,
        for ( ContainerHost node : nodes )
        {
            try
            {
                commandUtil.execute( commands.getInstallCommand(), node );
            }
            catch ( CommandException e )
            {
                throw new ClusterSetupException(
                        String.format( "Error while installing Nutch on container %s; %s", node.getHostname(),
                                e.getMessage() ) );
            }
        }
    }
}
