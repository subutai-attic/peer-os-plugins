/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.safehaus.subutai.plugin.mahout.impl;


import org.safehaus.subutai.common.command.OutputRedirection;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.settings.Common;
import org.safehaus.subutai.plugin.mahout.api.MahoutClusterConfig;


public class Commands
{

    public RequestBuilder getInstallCommand()
    {
        return new RequestBuilder( "apt-get --force-yes --assume-yes install " + MahoutClusterConfig.PRODUCT_PACKAGE )
                .withTimeout( 360 ).withStdOutRedirection( OutputRedirection.NO );
    }


    public RequestBuilder getUninstallCommand()
    {
        return new RequestBuilder( "apt-get --force-yes --assume-yes purge " + MahoutClusterConfig.PRODUCT_PACKAGE )
                .withTimeout( 60 );
    }


    public RequestBuilder getCheckInstalledCommand()
    {
        return new RequestBuilder( "dpkg -l | grep '^ii' | grep " + Common.PACKAGE_PREFIX_WITHOUT_DASH );
    }
}
