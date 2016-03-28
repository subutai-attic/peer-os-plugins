package io.subutai.plugin.nutch.impl;


import io.subutai.common.command.OutputRedirection;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.settings.Common;
import io.subutai.plugin.nutch.api.NutchConfig;


public class Commands
{

    public RequestBuilder getInstallCommand()
    {
        return new RequestBuilder(
                String.format( "apt-get --force-yes --assume-yes install %s", NutchConfig.PRODUCT_PACKAGE ) )
                .withTimeout( 600 );
    }


    public RequestBuilder getUninstallCommand()
    {
        return new RequestBuilder(
                String.format( "apt-get --force-yes --assume-yes purge %s", NutchConfig.PRODUCT_PACKAGE ) )
                .withTimeout( 300 );
    }


    public RequestBuilder getCheckInstallationCommand()
    {
        return new RequestBuilder( String.format( "dpkg -l | grep '^ii' | grep %s", Common.PACKAGE_PREFIX ) );
    }


    public static RequestBuilder getAptUpdate()
    {
        return new RequestBuilder( "apt-get --force-yes --assume-yes update" ).withTimeout( 2000 ).withStdOutRedirection(
                OutputRedirection.NO );
    }
}
