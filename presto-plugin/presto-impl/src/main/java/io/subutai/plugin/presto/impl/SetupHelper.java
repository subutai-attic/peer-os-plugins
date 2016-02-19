package io.subutai.plugin.presto.impl;


import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.CommandUtil;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.plugincommon.api.ClusterSetupException;
import io.subutai.plugin.presto.api.PrestoClusterConfig;


public class SetupHelper
{

    private static final Logger LOG = LoggerFactory.getLogger( SetupHelper.class );
    final TrackerOperation po;
    final PrestoImpl manager;
    final PrestoClusterConfig config;
    CommandUtil commandUtil;


    public SetupHelper( TrackerOperation po, PrestoImpl manager, PrestoClusterConfig config )
    {

        Preconditions.checkNotNull( config, "Presto cluster config is null" );
        Preconditions.checkNotNull( po, "Product operation tracker is null" );
        Preconditions.checkNotNull( manager, "Presto manager is null" );

        this.po = po;
        this.manager = manager;
        this.config = config;
        commandUtil = new CommandUtil();
    }


    void checkConnected( Environment environment ) throws ClusterSetupException
    {

        if ( !getCoordinatorHost( environment ).isConnected() )
        {
            throw new ClusterSetupException( "Coordinator node is not connected" );
        }

        try
        {
            for ( EnvironmentContainerHost host : environment.getContainerHostsByIds( config.getWorkers() ) )
            {
                if ( !host.isConnected() )
                {
                    throw new ClusterSetupException( "Not all worker nodes are connected" );
                }
            }
        }
        catch ( ContainerHostNotFoundException e )
        {
            LOG.error( "Container host not found", e );
            po.addLogFailed( "Container host not found" );
        }
    }


    public void configureAsCoordinator( EnvironmentContainerHost host, Environment environment )
            throws ClusterSetupException, CommandException
    {
        po.addLog( "Configuring coordinator..." );

        CommandResult result =
                host.execute( manager.getCommands().getSetCoordinatorCommand( getCoordinatorHost( environment ) ) );
        processResult( host, result );
    }


    public void configureAsWorker( Set<EnvironmentContainerHost> workerHosts ) throws ClusterSetupException
    {
        po.addLog( "Configuring worker(s)..." );

        for ( EnvironmentContainerHost host : workerHosts )
        {
            try
            {
                CommandResult result = host.execute( manager.getCommands().getSetWorkerCommand( host ) );
                processResult( host, result );
            }
            catch ( CommandException e )
            {
                throw new ClusterSetupException(
                        String.format( "Failed to configure workers Presto node(s): %s", e.getMessage() ) );
            }
        }
    }


    public void startNodes( final Set<EnvironmentContainerHost> set ) throws ClusterSetupException
    {
        po.addLog( "Starting Presto node(s)..." );
        for ( EnvironmentContainerHost host : set )
        {
            try
            {
                CommandResult result = host.execute( manager.getCommands().getStartCommand().daemon() );
                processResult( host, result );
            }
            catch ( CommandException e )
            {
                throw new ClusterSetupException(
                        String.format( "Failed to start Presto node(s): %s", e.getMessage() ) );
            }
            po.addLogDone( "Presto node(s) started successfully\nDone" );
        }
    }


    public void processResult( EnvironmentContainerHost host, CommandResult result ) throws ClusterSetupException
    {

        if ( !result.hasSucceeded() )
        {
            throw new ClusterSetupException( String.format( "Error on container %s: %s", host.getHostname(),
                    result.hasCompleted() ? result.getStdErr() : "Command timed out" ) );
        }
    }


    public EnvironmentContainerHost getCoordinatorHost( Environment environment )
    {
        try
        {
            return environment.getContainerHostById( config.getCoordinatorNode() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            LOG.error( "Container host not found", e );
            po.addLogFailed( "Container host not found" );
        }
        return null;
    }


    public void checkInstalled( EnvironmentContainerHost host, CommandResult result ) throws ClusterSetupException
    {
        CommandResult statusResult;
        try
        {
            statusResult = commandUtil.execute( manager.getCommands().getCheckInstalledCommand(), host );
        }
        catch ( CommandException e )
        {
            throw new ClusterSetupException( String.format( "Error on container %s:", host.getHostname() ) );
        }

        if ( !( result.hasSucceeded() && statusResult.getStdOut().contains( PrestoClusterConfig.PRODUCT_PACKAGE ) ) )
        {
            po.addLogFailed( String.format( "Error on container %s:", host.getHostname() ) );
            throw new ClusterSetupException( String.format( "Error on container %s: %s", host.getHostname(),
                    result.hasCompleted() ? result.getStdErr() : "Command timed out" ) );
        }
    }
}
