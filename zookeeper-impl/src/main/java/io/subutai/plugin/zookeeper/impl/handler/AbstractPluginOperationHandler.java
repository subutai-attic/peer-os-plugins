package io.subutai.plugin.zookeeper.impl.handler;


import java.util.ArrayList;
import java.util.List;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.tracker.OperationState;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.plugin.common.api.AbstractOperationHandler;
import io.subutai.plugin.common.api.ApiBase;
import io.subutai.plugin.common.api.ConfigBase;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;


public abstract class AbstractPluginOperationHandler<T extends ApiBase, V extends ConfigBase>
        extends AbstractOperationHandler<T, V>
{

    private static final Logger LOG = LoggerFactory.getLogger( AbstractPluginOperationHandler.class );


    public AbstractPluginOperationHandler( final T manager, final V config )
    {
        super( manager, config );
    }


    public AbstractPluginOperationHandler( final T manager, final String clusterName )
    {
        super( manager, clusterName );
    }


    protected CommandResult executeCommand( ContainerHost containerHost, String command )
    {
        CommandResult result = null;
        try
        {
            result = containerHost.execute( new RequestBuilder( command ).daemon() );
        }
        catch ( CommandException e )
        {
            LOG.error( "Could not execute command successfully! ", command );
            LOG.error( "Error: ", e );
        }
        return result;
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


    public void logResults( TrackerOperation po, List<CommandResult> commandResultList )
    {
        Preconditions.checkNotNull( commandResultList );
        for ( CommandResult commandResult : commandResultList )
        {
            po.addLog( commandResult.getStdOut() );
        }
        if ( po.getState() == OperationState.FAILED )
        {
            po.addLogFailed( "" );
        }
        else
        {
            po.addLogDone( "" );
        }
    }
}
