package io.subutai.plugin.generic.impl;


import java.io.*;
import java.util.UUID;

import io.subutai.common.command.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

import io.subutai.common.peer.ContainerHost;
import io.subutai.plugin.generic.api.model.Operation;


public class ExecutorManager
{
    private static final Logger LOG = LoggerFactory.getLogger( ExecutorManager.class.getName() );
    // private ExecutorService executor = SubutaiExecutors.newCachedThreadPool();
    private String output;
    private ContainerHost containerHost;
    private Operation operation;
    private RequestBuilder requestBuilder;


    public ExecutorManager( final ContainerHost host, final Operation operation )
    {
        this.containerHost = host;
        this.operation = operation;
    }


    private String parseCommand()
    {
        StringBuilder command = new StringBuilder( operation.getCommandName() );
        StringBuilder result = new StringBuilder( operation.getCommandName() );
        int counter = 0;
        for ( int i = 0; i < command.length(); ++i )
        {
            if ( command.charAt( i ) == '\n' )
            {
                LOG.info( "1 " + String.valueOf( counter ) + " " + String.valueOf( i ) );
                result.setCharAt( i + counter, '\\' );
                result.insert( i + counter + 1, 'n' );
                ++counter;
            }
            else if ( command.charAt( i ) == '\"' )
            {
                LOG.info( "2 " + String.valueOf( counter ) + " " + String.valueOf( i ) );
                result.insert( i + counter, '\\' );
                ++counter;
            }
            else if ( command.charAt( i ) == '\\' )
            {
                LOG.info( "3 " + String.valueOf( counter ) + " " + String.valueOf( i ) );
                result.insert( i + counter, '\\' );
                ++counter;
            }
            else if ( command.charAt( i ) == '$' )
            {
                LOG.info( "4 " + String.valueOf( counter ) + " " + String.valueOf( i ) );
                result.insert( i + counter, '\\' );
                ++counter;
            }
        }
        LOG.info( "Parsed:\n" + command.toString() );
        return result.toString();
    }


    private void executeOnContainer()
    {
        try
        {
            containerHost.execute( requestBuilder, new CommandCallback()
            {
                @Override
                public void onResponse( Response response, CommandResult commandResult )
                {
                    StringBuilder out = new StringBuilder();
                    if ( !Strings.isNullOrEmpty( response.getStdOut() ) )
                    {
                        out.append( response.getStdOut() ).append( "\n" );
                    }
                    if ( !Strings.isNullOrEmpty( response.getStdErr() ) )
                    {
                        out.append( response.getStdErr() ).append( "\n" );
                    }
                    if ( commandResult.hasCompleted() || commandResult.hasTimedOut() )
                    {
                        if ( response.getType() == ResponseType.EXECUTE_RESPONSE && response.getExitCode() != 0 )
                        {
                            out.append( "Exit code: " ).append( response.getExitCode() ).append( "\n\n" );
                        }
                        else if ( response.getType() != ResponseType.EXECUTE_RESPONSE )
                        {
                            out.append( response.getType() ).append( "\n\n" );
                        }
                    }

                    if ( out.length() > 0 )
                    {
                        output = String.format( "%s [%d]:%n%s", containerHost.getHostname(), response.getPid(), out );
                    }
                }
            } );
        }
        catch ( CommandException e )
        {
            LOG.error( "Error in ExecuteCommandTask", e );
            output = e.getMessage() + "\n";
        }
    }


    public String execute()
    {
        if ( operation.getScript() )
        {
            String uuid = UUID.randomUUID().toString();
            requestBuilder = new RequestBuilder( "rm -rf " + uuid + ".sh" );
            try
            {

                containerHost.execute( requestBuilder );
                requestBuilder = new RequestBuilder( "echo \"" + parseCommand() + " \" > " + uuid + ".sh" );
                containerHost.execute( requestBuilder );
                requestBuilder = new RequestBuilder( "chmod 777 " + uuid + ".sh" );
                containerHost.execute( requestBuilder );
                requestBuilder = new RequestBuilder( "bash " + uuid + ".sh" );
                this.executeOnContainer();
                requestBuilder = new RequestBuilder( "rm -rf " + uuid + ".sh" );
                containerHost.execute( requestBuilder );
            }
            catch ( CommandException e )
            {
                LOG.error( "script failed to be created" );
            }
        }
        else
        {
            requestBuilder = new RequestBuilder( operation.getCommandName() );
            requestBuilder.withCwd( operation.getCwd() );
            // TODO: timeout cannot be float???
            requestBuilder.withTimeout( Integer.parseInt( operation.getTimeout() ) );
            if ( operation.getDaemon() )
            {
                requestBuilder.daemon();
            }
            /* executor.execute (new Runnable()
			{
				@Override
				public void run()
				{*/
            this.executeOnContainer();
			/*	}
			});*/
        }
        return output;
    }
}
