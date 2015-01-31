package org.safehaus.subutai.plugin.zookeeper.impl.handler;


import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.safehaus.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;
import org.safehaus.subutai.plugin.zookeeper.impl.Commands;
import org.safehaus.subutai.plugin.zookeeper.impl.ZookeeperImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;


/**
 * Handles ZK config property addition
 */
public class AddPropertyOperationHandler extends AbstractOperationHandler<ZookeeperImpl, ZookeeperClusterConfig>
{
    private static final Logger LOGGER = LoggerFactory.getLogger( AddPropertyOperationHandler.class );
    private final String fileName;
    private final String propertyName;
    private final String propertyValue;


    public AddPropertyOperationHandler( ZookeeperImpl manager, String clusterName, String fileName, String propertyName,
                                        String propertyValue )
    {
        super( manager, clusterName );
        this.fileName = fileName;
        this.propertyName = propertyName;
        this.propertyValue = propertyValue;
        trackerOperation = manager.getTracker().createTrackerOperation( ZookeeperClusterConfig.PRODUCT_KEY,
                String.format( "Adding property %s=%s to file %s", propertyName, propertyValue, fileName ) );
    }


    @Override
    public UUID getTrackerId()
    {
        return trackerOperation.getId();
    }


    @Override
    public void run()
    {
        if ( Strings.isNullOrEmpty( clusterName ) || Strings.isNullOrEmpty( fileName ) || Strings
                .isNullOrEmpty( propertyName ) )
        {
            trackerOperation.addLogFailed( "Malformed arguments\nOperation aborted" );
            return;
        }
        final ZookeeperClusterConfig config = manager.getCluster( clusterName );
        if ( config == null )
        {
            trackerOperation.addLogFailed(
                    String.format( "Cluster with name %s does not exist\nOperation aborted", clusterName ) );
            return;
        }

        trackerOperation.addLog( "Adding property..." );


        try
        {

            Environment zookeeperEnvironment =
                    manager.getEnvironmentManager().findEnvironment( config.getEnvironmentId() );
            Set<ContainerHost> zookeeperNodes = zookeeperEnvironment.getContainerHostsByIds( config.getNodes() );


            String addPropertyCommand = Commands.getAddPropertyCommand( fileName, propertyName, propertyValue );


            List<CommandResult> commandResultList = runCommandOnContainers( addPropertyCommand, zookeeperNodes );

            if ( getFailedCommandResults( commandResultList ).size() == 0 )
            {
                trackerOperation.addLog( "Property added successfully\nRestarting cluster..." );

                String restartCommand = Commands.getRestartCommand();

                commandResultList = runCommandOnContainers( restartCommand, zookeeperNodes );
                trackerOperation.addLogDone( "Restarting cluster finished..." );
            }
            else
            {
                StringBuilder stringBuilder = new StringBuilder();
                for ( CommandResult commandResult : getFailedCommandResults( commandResultList ) )
                {
                    stringBuilder.append( commandResult.getStdErr() );
                }
                trackerOperation
                        .addLogFailed( String.format( "Removing property failed: %s", stringBuilder.toString() ) );
            }
        }
        catch ( EnvironmentNotFoundException e )
        {
            LOGGER.error(
                    String.format( "Couldn't retrieve environment with id: %s", config.getEnvironmentId().toString() ),
                    e );
            trackerOperation.addLogFailed( String.format( "Couldn't retrieve environment with id: %s",
                    config.getEnvironmentId().toString() ) );
        }
        catch ( ContainerHostNotFoundException e )
        {
            LOGGER.error(
                    String.format( "Some container hosts weren't fetched for ids: %s", config.getNodes().toString() ),
                    e );
            trackerOperation.addLogFailed(
                    String.format( "Some container hosts weren't fetched for ids: %s", config.getNodes().toString() ) );
        }
    }


    private List<CommandResult> runCommandOnContainers( String command, final Set<ContainerHost> zookeeperNodes )
    {
        List<CommandResult> commandResults = new ArrayList<>();
        for ( ContainerHost containerHost : zookeeperNodes )
        {
            try
            {
                commandResults.add( containerHost.execute( new RequestBuilder( command ) ) );
            }
            catch ( CommandException e )
            {
                e.printStackTrace();
            }
        }
        return commandResults;
    }


    public List<CommandResult> getFailedCommandResults( final List<CommandResult> commandResultList )
    {
        List<CommandResult> failedCommands = new ArrayList<>();
        for ( CommandResult commandResult : commandResultList )
        {
            if ( !commandResult.hasSucceeded() )
            {
                failedCommands.add( commandResult );
            }
        }
        return failedCommands;
    }
}
