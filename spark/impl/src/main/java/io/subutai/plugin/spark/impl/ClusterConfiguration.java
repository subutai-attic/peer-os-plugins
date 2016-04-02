package io.subutai.plugin.spark.impl;


import java.util.Set;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.plugincommon.api.ClusterConfigurationException;
import io.subutai.core.plugincommon.api.ClusterConfigurationInterface;
import io.subutai.plugin.spark.api.SparkClusterConfig;

import com.google.common.collect.Sets;


/**
 * Configures Spark cluster
 */
public class ClusterConfiguration implements ClusterConfigurationInterface<SparkClusterConfig>
{
    private final SparkImpl manager;
    private final TrackerOperation po;


    public ClusterConfiguration( SparkImpl manager, TrackerOperation po )
    {
        this.manager = manager;
        this.po = po;
    }


    @Override
    public void configureCluster( final SparkClusterConfig config, final Environment environment )
            throws ClusterConfigurationException
    {
        final EnvironmentContainerHost master;
        try
        {
            master = environment.getContainerHostById( config.getMasterNodeId() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            throw new ClusterConfigurationException( e );
        }
        final Set<EnvironmentContainerHost> slaves;
        try
        {
            slaves = environment.getContainerHostsByIds( config.getSlaveIds() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            throw new ClusterConfigurationException( e );
        }

        //configure master IP
        po.addLog( "Setting master IP..." );

        RequestBuilder setMasterIPCommand = manager.getCommands().getSetMasterIPCommand( master.getHostname() );
        for ( EnvironmentContainerHost host : slaves )
        {
            executeCommand( host, setMasterIPCommand );
        }
        po.addLog( "Setting master IP succeeded" );

        //register slaves
        po.addLog( "Registering slave(s)..." );

        Set<String> slaveHostnames = Sets.newHashSet();

        for ( EnvironmentContainerHost host : slaves )
        {
            slaveHostnames.add( host.getHostname() );
        }

        RequestBuilder addSlavesCommand = manager.getCommands().getAddSlavesCommand( slaveHostnames );

        executeCommand( master, addSlavesCommand );

        po.addLog( "Slave(s) successfully registered" );
    }


    public CommandResult executeCommand( EnvironmentContainerHost host, RequestBuilder command )
            throws ClusterConfigurationException
    {

        CommandResult result;
        try
        {
            result = host.execute( command );
        }
        catch ( CommandException e )
        {
            throw new ClusterConfigurationException( e );
        }
        if ( !result.hasSucceeded() )
        {
            throw new ClusterConfigurationException( String.format( "Error on container %s: %s", host.getHostname(),
                    result.hasCompleted() ? result.getStdErr() : "Command timed out" ) );
        }
        return result;
    }
}
