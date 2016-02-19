package io.subutai.plugin.zookeeper.impl.handler;


import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

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
 * Handles ZK config property removal
 */
public class RemovePropertyOperationHandler extends AbstractOperationHandler<ZookeeperImpl, ZookeeperClusterConfig>
{
    private final String fileName;
    private final String propertyName;


    public RemovePropertyOperationHandler( ZookeeperImpl manager, String clusterName, String fileName,
                                           String propertyName )
    {
        super( manager, clusterName );
        this.fileName = fileName;
        this.propertyName = propertyName;
        trackerOperation = manager.getTracker().createTrackerOperation( ZookeeperClusterConfig.PRODUCT_KEY,
                String.format( "Removing property %s from file %s", propertyName, fileName ) );
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

        trackerOperation.addLog( "Removing property..." );

        String removePropertyCommand = Commands.getRemovePropertyCommand( fileName, propertyName );

        Environment environment;
        try
        {
            environment = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            trackerOperation.addLogFailed( "Couldn't retrieve cluster with id: " + config.getEnvironmentId() );
            return;
        }
        Set<EnvironmentContainerHost> zookeeperNodes;
        try
        {
            zookeeperNodes = environment.getContainerHostsByIds( config.getNodes() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            trackerOperation.addLogFailed( "ContainerHosts not found for ids: " + config.getNodes().toString() );
            return;
        }
        List<CommandResult> commandResultList = new ArrayList<>();
        for ( ContainerHost zookeeperNode : zookeeperNodes )
        {
            try
            {
                CommandResult commandResult = zookeeperNode.execute( new RequestBuilder( removePropertyCommand ) );
                commandResultList.add( commandResult );
            }
            catch ( CommandException e )
            {
                e.printStackTrace();
            }
        }

        if ( getFailedCommandResults( commandResultList ).size() == 0 )
        {
            trackerOperation.addLog( "Property removed successfully\nRestarting cluster..." );

            commandResultList = new ArrayList<>();
            String restartCommand = manager.getCommands().getRestartCommand();

            for ( ContainerHost zookeeperNode : zookeeperNodes )
            {
                try
                {
                    CommandResult commandResult = zookeeperNode.execute( new RequestBuilder( restartCommand ) );
                    commandResultList.add( commandResult );
                }
                catch ( CommandException e )
                {
                    e.printStackTrace();
                }
            }
            trackerOperation.addLogDone( String.format( "Cluster %s restarted succesfully", clusterName ) );
        }
        else
        {
            StringBuilder stringBuilder = new StringBuilder();
            for ( CommandResult commandResult : getFailedCommandResults( commandResultList ) )
            {
                stringBuilder.append( commandResult.getStdErr() );
            }
            trackerOperation.addLogFailed( String.format( "Removing property failed: %s", stringBuilder.toString() ) );
        }
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
