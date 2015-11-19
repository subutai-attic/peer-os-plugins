package io.subutai.plugin.generic.impl;


import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.codec.binary.Base64;

import com.google.common.base.Strings;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
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
            CommandResult result = containerHost.execute( requestBuilder );
            StringBuilder out = new StringBuilder();

            if ( !Strings.isNullOrEmpty( result.getStdOut() ) )
            {
                out.append( result.getStdOut() );
            }
            if ( !Strings.isNullOrEmpty( result.getStdErr() ) )
            {
                out.append( result.getStdErr() );
            }

            output = String.format( "%s:%n%s", containerHost.getHostname(), out );
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
                requestBuilder = new RequestBuilder( "echo " + operation.getCommandName() + " | base64 --decode >> " + uuid + ".sh" );
                containerHost.execute( requestBuilder );
                requestBuilder = new RequestBuilder( "chmod +x " + uuid + ".sh" );
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
            byte[] decodedBytes = Base64.decodeBase64( operation.getCommandName() );
            requestBuilder = new RequestBuilder( new String( decodedBytes ) );
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
