package io.subutai.plugin.generic.impl;


import java.util.concurrent.ExecutorService;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Strings;

import io.subutai.common.command.CommandCallback;
import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.command.Response;
import io.subutai.common.command.ResponseType;
import io.subutai.common.mdc.SubutaiExecutors;
import io.subutai.plugin.generic.api.GenericPluginConfiguration;

public class ExecutorManager
{
	private static final Logger LOG = LoggerFactory.getLogger (ExecutorManager.class.getName());
	private GenericPluginConfiguration config;
	private ExecutorService executor = SubutaiExecutors.newCachedThreadPool();
	private String output;
	public ExecutorManager (GenericPluginConfiguration config)
	{
		this.config = config;
	}

	public String execute()
	{
		final RequestBuilder requestBuilder = new RequestBuilder (this.config.getCommand());
		requestBuilder.withCwd (this.config.getCwd());
		// TODO: timeout cannot be float???
		requestBuilder.withTimeout (this.config.getTimeOut().intValue());
		if (this.config.getDaemon())
		{
			requestBuilder.daemon();
		}
/*		executor.execute (new Runnable()
		{
			@Override
			public void run()
			{*/
				try
				{
					config.getHost().execute (requestBuilder, new CommandCallback()
					{
						@Override
						public void onResponse (Response response, CommandResult commandResult)
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
								LOG.info ("out is:\n" + out);
								output = String.format ( "%s [%d]:%n%s", config.getHost().getHostname(), response.getPid(), out );
							}
						}
					});


				}
				catch ( CommandException e )
				{
					//LOG.error( "Error in ExecuteCommandTask", e );
					output = e.getMessage() + "\n";
				}
				finally
				{
					//form.taskCount.decrementAndGet();
					//if ( form.taskCount.get() == 0 )
					//{
					//	form.indicator.setVisible( false );
					//}
				}
		/*	}
		});*/

		LOG.info ("Output is:\n" + output);
		return output;
	}
}
