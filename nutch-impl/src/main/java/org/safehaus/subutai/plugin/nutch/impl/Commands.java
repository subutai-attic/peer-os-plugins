package org.safehaus.subutai.plugin.nutch.impl;


import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.settings.Common;
import org.safehaus.subutai.plugin.nutch.api.NutchConfig;


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
}
