package org.safehaus.subutai.plugin.accumulo.impl.handler;//package org.safehaus.subutai.plugin.accumulo.impl.handler;


import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.plugin.accumulo.api.AccumuloClusterConfig;
import org.safehaus.subutai.plugin.accumulo.impl.AccumuloImpl;
import org.safehaus.subutai.plugin.accumulo.impl.Commands;
import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;


/**
 * Handles remove accumulo config property operation
 */
public class RemovePropertyOperationHandler extends AbstractOperationHandler<AccumuloImpl, AccumuloClusterConfig>
{
    private final String propertyName;
    private static final Logger LOGGER = LoggerFactory.getLogger( RemovePropertyOperationHandler.class );


    public RemovePropertyOperationHandler( AccumuloImpl manager, String clusterName, String propertyName )
    {
        super( manager, manager.getCluster( clusterName ) );
        this.propertyName = propertyName;
        Preconditions.checkArgument( !Strings.isNullOrEmpty( propertyName ), "Property name is null or empty" );
        trackerOperation = manager.getTracker().createTrackerOperation( AccumuloClusterConfig.PRODUCT_KEY,
                String.format( "Removing property %s", propertyName ) );
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

        Environment environment = null;
        try
        {
            environment = manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            String msg =
                    String.format( "Couldn't find find environment with id: %s", config.getEnvironmentId().toString() );
            trackerOperation.addLogFailed( msg );
            LOGGER.error( msg, e );
            return;
        }
        CommandResult result = null;
        boolean allSuccess = true;
        Set<ContainerHost> containerHosts = new HashSet<>();
        try
        {
            containerHosts.addAll( environment.getContainerHostsByIds( accumuloClusterConfig.getAllNodes() ) );
        }
        catch ( ContainerHostNotFoundException e )
        {
            String msg = String.format( "Some container hosts couldn't be retrieved from %s environment with ids: %s",
                    environment.getName(), accumuloClusterConfig.getAllNodes().toString() );
            LOGGER.error( msg, e );
            trackerOperation.addLogFailed( msg );
        }

        for ( ContainerHost containerHost : containerHosts )
        {
            try
            {
                result = containerHost
                        .execute( new RequestBuilder( Commands.getRemovePropertyCommand( propertyName ) ) );
                if ( result.hasSucceeded() )
                {
                    trackerOperation.addLog( "Property removed successfully to node " + containerHost.getHostname() );
                }
                else
                {
                    allSuccess = false;
                }
            }
            catch ( CommandException e )
            {
                allSuccess = false;
                String msg = String.format( "Error removing property %s", propertyName );
                trackerOperation.addLogFailed( msg );
                LOGGER.error( msg, e );
                throw new NullPointerException( msg );
            }
        }
        if ( allSuccess )
        {
            trackerOperation.addLog( "Restarting cluster... " );
            ContainerHost master = null;
            try
            {
                master = environment.getContainerHostById( accumuloClusterConfig.getMasterNode() );
            }
            catch ( ContainerHostNotFoundException e )
            {
                String msg = String.format( "Container host with id: %s is not available from environment %s",
                        accumuloClusterConfig.getMasterNode().toString(), environment.getName() );
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
            }
        }
        trackerOperation.addLogDone( "Done" );
    }
}
