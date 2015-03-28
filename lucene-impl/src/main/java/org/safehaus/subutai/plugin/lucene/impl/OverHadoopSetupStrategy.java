package org.safehaus.subutai.plugin.lucene.impl;


import java.util.Set;
import java.util.UUID;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.settings.Common;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.common.util.CollectionUtil;
import org.safehaus.subutai.plugin.common.api.ClusterException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupException;
import org.safehaus.subutai.plugin.common.api.ConfigBase;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.lucene.api.LuceneConfig;

import com.google.common.base.Strings;
import com.google.common.collect.Sets;


class OverHadoopSetupStrategy extends LuceneSetupStrategy
{
    private Environment environment;


    public OverHadoopSetupStrategy( LuceneImpl manager, LuceneConfig config, TrackerOperation po,
                                    Environment environment )
    {
        super( manager, config, po );
        this.environment = environment;
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
        //check nodes are connected
        Set<ContainerHost> nodes = Sets.newHashSet();
        try
        {
            for( UUID nodeId : config.getNodes() )
            {
                nodes.add( environment.getContainerHostById( nodeId ) );
            }
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

        trackerOperation.addLog( "Checking prerequisites..." );
        RequestBuilder checkInstalledCommand = new RequestBuilder( Commands.checkCommand );


        for ( ContainerHost node : nodes )
        {
            try
            {
                CommandResult result = node.execute( checkInstalledCommand );
                if ( result.getStdOut().contains( Commands.PACKAGE_NAME ) )
                {
                    trackerOperation.addLog(
                            String.format( "Node %s already has Lucene installed. Omitting this node from installation",
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
        Set<ContainerHost> nodes = Sets.newHashSet();
        try
        {
            for( UUID nodeId : config.getNodes() )
            {
                nodes.add( environment.getContainerHostById( nodeId ) );
            }
        }
        catch ( ContainerHostNotFoundException e )
        {
            throw new ClusterSetupException( String.format( "Failed to obtain environment containers: %s", e ) );
        }
        for ( ContainerHost node : nodes )
        {
            try
            {
                CommandResult result = node.execute( new RequestBuilder( Commands.installCommand ).withTimeout( 1000 ) );
                checkInstalled( node, result );
            }
            catch ( CommandException e )
            {
                throw new ClusterSetupException( String.format( "Failed to install Lucene on server node" ) );
            }
        }
        trackerOperation.addLog( "Updating db..." );
        config.setEnvironmentId( environment.getId() );
        try
        {
            manager.saveConfig( config );
        }
        catch ( ClusterException e )
        {
            throw new ClusterSetupException( e );
        }
        trackerOperation.addLog( "Cluster info saved to DB\nInstalling Lucene..." );
    }


    public void processResult( ContainerHost host, CommandResult result ) throws ClusterSetupException
    {

        if ( !result.hasSucceeded() )
        {
            throw new ClusterSetupException( String.format( "Error on container %s: %s", host.getHostname(),
                    result.hasCompleted() ? result.getStdErr() : "Command timed out" ) );
        }
    }

    public void checkInstalled( ContainerHost host, CommandResult result) throws ClusterSetupException
    {
        CommandResult statusResult;
        try
        {
            statusResult = host.execute( new RequestBuilder( Commands.checkCommand) );
        }
        catch ( CommandException e )
        {
            throw new ClusterSetupException( String.format( "Error on container %s:", host.getHostname()) );
        }

        if ( !( result.hasSucceeded() && statusResult.getStdOut().contains( LuceneConfig.PRODUCT_PACKAGE ) ) )
        {
            trackerOperation.addLogFailed( String.format( "Error on container %s:", host.getHostname()) );
            throw new ClusterSetupException( String.format( "Error on container %s: %s", host.getHostname(),
                    result.hasCompleted() ? result.getStdErr() : "Command timed out" ) );
        }
    }
}
