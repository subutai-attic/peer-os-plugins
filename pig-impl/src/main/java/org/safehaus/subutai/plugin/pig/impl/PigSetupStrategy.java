package org.safehaus.subutai.plugin.pig.impl;


import java.util.Set;
import java.util.UUID;

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
import org.safehaus.subutai.plugin.common.api.ClusterException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.common.api.ConfigBase;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.pig.api.PigConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;


class PigSetupStrategy implements ClusterSetupStrategy
{
    private static final Logger LOG = LoggerFactory.getLogger( PigSetupStrategy.class.getName() );
    final PigImpl manager;
    final PigConfig config;
    final TrackerOperation trackerOperation;
    private Environment environment;


    public PigSetupStrategy( PigImpl manager, PigConfig config, TrackerOperation trackerOperation )
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
            LOG.error( "Error getting environment by id: " + hc.getEnvironmentId().toString(), e );
            return;
        }

        if ( environment == null )
        {
            throw new ClusterSetupException( "Could not find Hadoop cluster environment" );
        }

        //      check nodes are connected
        Set<ContainerHost> nodes = null;
        try
        {
            nodes = environment.getContainerHostsByIds( config.getNodes() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            LOG.error( "Container hosts not found", e );
            trackerOperation.addLogFailed( "Container hosts not found" );
        }
        for ( ContainerHost host : nodes )
        {
            if ( !host.isConnected() )
            {
                throw new ClusterSetupException( String.format( "Container %s is not connected", host.getHostname() ) );
            }
        }


        trackerOperation.addLog( "Checking prerequisites..." );

        RequestBuilder checkInstalledCommand = new RequestBuilder( Commands.checkCommand );
        for ( UUID uuid : config.getNodes() )
        {
            ContainerHost node = null;
            try
            {
                node = environment.getContainerHostById( uuid );
            }
            catch ( ContainerHostNotFoundException e )
            {
                LOG.error( "Container host not found", e );
                trackerOperation.addLogFailed( "Container host not found" );
            }
            try
            {
                CommandResult result = node.execute( checkInstalledCommand );
                if ( result.getStdOut().contains( Commands.PACKAGE_NAME ) )
                {
                    trackerOperation.addLog(
                            String.format( "Node %s already has Pig installed. Omitting this node from installation",
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
        trackerOperation.addLog( "Cluster info saved to DB\nInstalling Pig..." );
        //install pig,
        try
        {
            for ( ContainerHost node : environment.getContainerHostsByIds( config.getNodes() ) )
            {
                try
                {
                    CommandResult result = node.execute( new RequestBuilder( Commands.installCommand ).withTimeout( 600 ) );
                    processResult( node,result );
                }
                catch ( CommandException e )
                {
                    throw new ClusterSetupException(
                            String.format( "Error while installing Pig on container %s; %s", node.getHostname(),
                                    e.getMessage() ) );
                }
            }
        }
        catch ( ContainerHostNotFoundException e )
        {
            LOG.error( "Container hosts not found", e );
            trackerOperation.addLogFailed( "Container hosts not found" );
        }

        trackerOperation.addLog( "Configuring cluster..." );
    }

    public void processResult( ContainerHost host, CommandResult result ) throws ClusterSetupException
    {

        if ( !result.hasSucceeded() )
        {
            throw new ClusterSetupException( String.format( "Error on container %s: %s", host.getHostname(),
                    result.hasCompleted() ? result.getStdErr() : "Command timed out" ) );
        }
    }
}
