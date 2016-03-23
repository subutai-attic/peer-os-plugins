package io.subutai.plugin.presto.impl;


import io.subutai.common.command.OutputRedirection;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.settings.Common;
import io.subutai.plugin.presto.api.PrestoClusterConfig;


public class Commands
{

    public static final String PACKAGE_NAME = Common.PACKAGE_PREFIX + PrestoClusterConfig.PRODUCT_KEY.toLowerCase();

	public RequestBuilder getAptUpdate()
	{
		return new RequestBuilder( "apt-get --force-yes --assume-yes update").withTimeout( 600 )
				.withStdOutRedirection(
						OutputRedirection.NO );
	}


	public RequestBuilder getInstallPython()
	{
		return new RequestBuilder( "apt-get --force-yes --assume-yes install python").withTimeout( 600 )
				.withStdOutRedirection(
						OutputRedirection.NO );
	}

    public RequestBuilder getInstallCommand()
    {
        return new RequestBuilder( "apt-get --force-yes --assume-yes install " + PACKAGE_NAME ).withTimeout( 600 )
                                                                                               .withStdOutRedirection(
                                                                                                       OutputRedirection.NO );
    }


    public RequestBuilder getUninstallCommand()
    {
        return new RequestBuilder( "apt-get --force-yes --assume-yes purge " + PACKAGE_NAME ).withTimeout( 600 );
    }


    public RequestBuilder getCheckInstalledCommand()
    {
        return new RequestBuilder( "dpkg -l | grep '^ii' | grep " + Common.PACKAGE_PREFIX_WITHOUT_DASH );
    }


    public RequestBuilder getStartCommand()
    {
        return new RequestBuilder( "service presto start" );
    }


    public RequestBuilder getStopCommand()
    {
        return new RequestBuilder( "service presto stop" );
    }


    public RequestBuilder getStatusCommand()
    {
        return new RequestBuilder( "service presto status" );
    }


    public RequestBuilder getSetCoordinatorCommand( ContainerHost coordinatorNode )
    {
        String s = String.format( "presto-config.sh coordinator %s", coordinatorNode.getHostname() );
        return new RequestBuilder( s );
    }


    public RequestBuilder getSetWorkerCommand( ContainerHost node )
    {
        String s = String.format( "presto-config.sh worker %s", node.getHostname() );
        return new RequestBuilder( s );
    }


}
