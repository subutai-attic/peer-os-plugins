package io.subutai.plugin.shark.impl;


import io.subutai.common.command.OutputRedirection;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.settings.Common;
import io.subutai.plugin.shark.api.SharkClusterConfig;


public class Commands
{

    public static final String PACKAGE_NAME = Common.PACKAGE_PREFIX + SharkClusterConfig.PRODUCT_KEY.toLowerCase();


    public RequestBuilder getInstallCommand()
    {
        return new RequestBuilder( "apt-get --force-yes --assume-yes install " + PACKAGE_NAME ).withTimeout( 900 )
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


    public RequestBuilder getSetMasterIPCommand( ContainerHost master )
    {
        return new RequestBuilder(
                String.format( "/opt/shark*/bin/sharkConf.sh clear master ; /opt/shark*/bin/sharkConf.sh master %s",
                        master.getHostname() ) ).withTimeout( 60 );
    }


    public RequestBuilder getServiceStatusCommand()
    {
        return new RequestBuilder( "service shark status" );
    }
}

