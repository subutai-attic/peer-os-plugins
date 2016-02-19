package io.subutai.plugin.zookeeper.impl.handler;


import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.core.plugincommon.api.AbstractOperationHandler;
import io.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;
import io.subutai.plugin.zookeeper.impl.Commands;
import io.subutai.plugin.zookeeper.impl.ZookeeperImpl;


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
                    manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );
            Set<EnvironmentContainerHost> zookeeperNodes =
                    zookeeperEnvironment.getContainerHostsByIds( config.getNodes() );


            String addPropertyCommand = Commands.getAddPropertyCommand( fileName, propertyName, propertyValue );


            List<CommandResult> commandResultList = runCommandOnContainers( addPropertyCommand, zookeeperNodes );

            if ( getFailedCommandResults( commandResultList ).size() == 0 )
            {
                trackerOperation.addLog( "Property added successfully\nRestarting cluster..." );

                String restartCommand = Commands.getRestartCommand();

                runCommandOnContainers( restartCommand, zookeeperNodes );
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
            LOGGER.error( String.format( "Couldn't retrieve environment with id: %s", config.getEnvironmentId() ), e );
            trackerOperation.addLogFailed(
                    String.format( "Couldn't retrieve environment with id: %s", config.getEnvironmentId() ) );
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


    private List<CommandResult> runCommandOnContainers( String command,
                                                        final Set<EnvironmentContainerHost> zookeeperNodes )
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
