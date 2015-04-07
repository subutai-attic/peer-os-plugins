package org.safehaus.subutai.plugin.mahout.impl;


import java.util.Set;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.settings.Common;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.common.util.CollectionUtil;
import org.safehaus.subutai.plugin.common.api.ClusterSetupException;
import org.safehaus.subutai.plugin.common.api.ConfigBase;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.mahout.api.MahoutClusterConfig;

import com.google.common.base.Strings;


class OverHadoopSetupStrategy extends MahoutSetupStrategy
{
    private Environment environment;


    public OverHadoopSetupStrategy( MahoutImpl manager, MahoutClusterConfig config, TrackerOperation po )
    {
        super( manager, config, po );
    }


    private void check() throws ClusterSetupException
    {

        if ( Strings.isNullOrEmpty( config.getHadoopClusterName() ) || CollectionUtil
                .isCollectionEmpty( config.getNodes() ) )
        {
            throw new ClusterSetupException( "Malformed configuration\nInstallation aborted" );
        }

        if ( manager.getCluster( config.getClusterName() ) != null )
        {
            throw new ClusterSetupException(
                    String.format( "Cluster with name '%s' already exists\nInstallation aborted",
                            config.getClusterName() ) );
        }

        //check hadoopcluster
        HadoopClusterConfig hc = manager.getHadoopManager().getCluster( config.getHadoopClusterName() );
        if ( hc == null )
        {
            throw new ClusterSetupException( "Could not find Hadoop cluster " + config.getHadoopClusterName() );
        }
        if ( !hc.getAllNodes().containsAll( config.getNodes() ) )
        {
            throw new ClusterSetupException(
                    "Not all nodes belong to Hadoop cluster " + config.getHadoopClusterName() );
        }

        try
        {
            environment = manager.getEnvironmentManager().findEnvironment( hc.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            throw new ClusterSetupException( "Hadoop environment not found" );
        }


        //check nodes are connected
        Set<ContainerHost> nodes;
        try
        {
            nodes = environment.getContainerHostsByIds( config.getNodes() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            throw new ClusterSetupException( String.format( "Failed to obtain environment containers: %s", e ) );
        }
        for ( ContainerHost host : nodes )
        {
            if ( !host.isConnected() )
            {
                throw new ClusterSetupException( String.format( "Container %s is not connected", host.getHostname() ) );
            }
        }


        trackerOperation.addLog( "Checking prerequisites..." );
        RequestBuilder checkInstalledCommand = manager.getCommands().getCheckInstalledCommand();
        for ( ContainerHost node : nodes )
        {
            try
            {
                CommandResult result = node.execute( checkInstalledCommand );
                if ( result.getStdOut().contains( MahoutClusterConfig.PRODUCT_PACKAGE ) )
                {
                    trackerOperation.addLog(
                            String.format( "Node %s already has Mahout installed. Omitting this node from installation",
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
            throw new ClusterSetupException( "No nodes eligible for installation. Operation aborted" );
        }
    }


    private void configure() throws ClusterSetupException
    {
        Set<ContainerHost> nodes;
        try
        {
            nodes = environment.getContainerHostsByIds( config.getNodes() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            throw new ClusterSetupException( String.format( "Failed to obtain environment containers: %s", e ) );
        }
        for ( ContainerHost node : nodes )
        {
            try
            {
                CommandResult result = node.execute( manager.getCommands().getInstallCommand() );
                processResult( node, result );
            }
            catch ( CommandException e )
            {
                throw new ClusterSetupException( String.format( "Failed to install Mahout on server node: %s", e ) );
            }
        }
        trackerOperation.addLog( "Updating db..." );
        config.setEnvironmentId( environment.getId() );
        manager.getPluginDAO().saveInfo( MahoutClusterConfig.PRODUCT_KEY, config.getClusterName(), config );
        trackerOperation.addLog( "Cluster info saved to DB\nInstalling Mahout..." );
    }


    public void processResult( ContainerHost host, CommandResult result ) throws ClusterSetupException
    {
        CommandResult statusResult;
        try
        {
            RequestBuilder checkInstalledCommand = manager.getCommands().getCheckInstalledCommand();
            statusResult = host.execute( checkInstalledCommand );
        }
        catch ( CommandException e )
        {
            throw new ClusterSetupException( String.format( "Error on container %s:", host.getHostname()) );
        }

        if ( !( result.hasSucceeded() && statusResult.getStdOut().contains( MahoutClusterConfig.PRODUCT_PACKAGE ) ) )
        {
            trackerOperation.addLogFailed( String.format( "Error on container %s:", host.getHostname()) );
            throw new ClusterSetupException( String.format( "Error on container %s: %s", host.getHostname(),
                    result.hasCompleted() ? result.getStdErr() : "Command timed out" ) );
        }
    }


    @Override
    public ConfigBase setup() throws ClusterSetupException
    {
        check();
        configure();
        return config;
    }
}