package io.subutai.plugin.nutch.impl;


import io.subutai.common.command.OutputRedirection;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.settings.Common;
import io.subutai.plugin.nutch.api.NutchConfig;


public class Commands
{
    public static final String PACKAGE_NAME = Common.PACKAGE_PREFIX + "nutch2";

    public static RequestBuilder getInstallCommand()
    {
        return new RequestBuilder(
                String.format( "apt-get --force-yes --assume-yes install %s", PACKAGE_NAME ) )
                .withTimeout( 2000 );
    }


    public static RequestBuilder getUninstallCommand()
    {
        return new RequestBuilder(
                String.format( "apt-get --force-yes --assume-yes purge %s", PACKAGE_NAME ) )
                .withTimeout( 300 );
    }


    public static RequestBuilder getCheckInstallationCommand()
    {
        return new RequestBuilder( String.format( "dpkg -l | grep '^ii' | grep %s", Common.PACKAGE_PREFIX ) );
    }


    public static RequestBuilder getAptUpdate()
    {
        return new RequestBuilder( "apt-get --force-yes --assume-yes update" ).withTimeout( 2000 ).withStdOutRedirection(
                OutputRedirection.NO );
    }
}
