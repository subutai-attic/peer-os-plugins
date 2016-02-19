package io.subutai.plugin.accumulo.impl.handler;


import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.plugin.accumulo.api.AccumuloClusterConfig;
import io.subutai.plugin.accumulo.impl.AccumuloImpl;
import io.subutai.plugin.accumulo.impl.Commands;
import io.subutai.core.plugincommon.api.AbstractOperationHandler;


/**
 * Handles add accumulo config property operation
 */
public class AddPropertyOperationHandler extends AbstractOperationHandler<AccumuloImpl, AccumuloClusterConfig>
{
    private static final Logger LOGGER = LoggerFactory.getLogger( AddPropertyOperationHandler.class );
    private final String propertyName;
    private final String propertyValue;


    public AddPropertyOperationHandler( AccumuloImpl manager, String clusterName, String propertyName,
                                        String propertyValue )
    {
        super( manager, manager.getCluster( clusterName ) );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( propertyName ), "Property name is null or empty" );
        Preconditions.checkArgument( !Strings.isNullOrEmpty( propertyValue ), "Property Value is null or empty" );
        this.propertyName = propertyName;
        this.propertyValue = propertyValue;
        trackerOperation = manager.getTracker().createTrackerOperation( AccumuloClusterConfig.PRODUCT_KEY,
                String.format( "Adding property %s=%s", propertyName, propertyValue ) );
    }


    @Override
    public UUID getTrackerId()
    {
        return trackerOperation.getId();
    }


    @Override
    public void run()
    {
        AccumuloClusterConfig accumuloClusterConfig = manager.getCluster( clusterName );

        if ( accumuloClusterConfig == null )
        {
            trackerOperation.addLogFailed( String.format( "Cluster with name %s does not exist", clusterName ) );
            return;
        }

        Environment environment;
        try
        {
            environment = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            String msg = String.format( "Environment with id: %s doesn't exists.", config.getEnvironmentId() );
            trackerOperation.addLogFailed( msg );
            LOGGER.error( msg, e );
            return;
        }
        CommandResult result;
        boolean allSuccess = true;

        Set<EnvironmentContainerHost> containerHosts;
        try
        {
            containerHosts = new HashSet<>( environment.getContainerHostsByIds( accumuloClusterConfig.getAllNodes() ) );
        }
        catch ( ContainerHostNotFoundException e )
        {
            String msg = String.format( "Some container hosts with ids: %s not found in environment: %s",
                    accumuloClusterConfig.getAllNodes().toString(), environment.getName() );
            trackerOperation.addLogFailed( msg );
            LOGGER.error( msg, e );
            return;
        }

        for ( EnvironmentContainerHost containerHost : containerHosts )
        {
            try
            {
                result = containerHost
                        .execute( new RequestBuilder( Commands.getAddPropertyCommand( propertyName, propertyValue ) ) );
                if ( result.hasSucceeded() )
                {
                    trackerOperation.addLog( "Property added successfully to node " + containerHost.getHostname() );
                }
                else
                {
                    allSuccess = false;
                }
            }
            catch ( CommandException e )
            {
                allSuccess = false;
                String msg = String.format( "Adding property failed, %s:%s, on container host: %s", propertyName,
                        propertyValue, containerHost.getHostname() );
                trackerOperation.addLogFailed( msg );
                LOGGER.error( msg, e );
            }
        }
        if ( allSuccess )
        {
            trackerOperation.addLog( "Restarting cluster... " );
            EnvironmentContainerHost master;
            try
            {
                master = environment.getContainerHostById( accumuloClusterConfig.getMasterNode() );
            }
            catch ( ContainerHostNotFoundException e )
            {
                String msg = String.format( "Container host with id: %s doesn't exists in environment: %s",
                        accumuloClusterConfig.getMasterNode(), environment.getName() );
                trackerOperation.addLogFailed( msg );
                LOGGER.error( msg, e );
                return;
            }
            try
            {
                master.execute( Commands.stopCommand );
                master.execute( Commands.startCommand );
            }
            catch ( CommandException e )
            {
                String msg = String.format( "Accumulo cluster restart operation failed !!!" );
                trackerOperation.addLogFailed( msg );
                LOGGER.error( msg, e );
                return;
            }
        }
        trackerOperation.addLogDone( "Done" );
    }
}
