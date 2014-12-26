/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.safehaus.subutai.plugin.mahout.impl;


import java.util.Set;

import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.command.OutputRedirection;
import org.safehaus.subutai.common.settings.Common;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.plugin.mahout.api.MahoutClusterConfig;

import com.google.common.base.Preconditions;


public class Commands
{
    public static final String PACKAGE_NAME = Common.PACKAGE_PREFIX + MahoutClusterConfig.PRODUCT_KEY.toLowerCase();


    public RequestBuilder getInstallCommand()
    {
        RequestBuilder rb = new RequestBuilder( "apt-get --force-yes --assume-yes install " + PACKAGE_NAME ).withTimeout( 360 )
                                                                                                            .withStdOutRedirection(
                                                                                                                    OutputRedirection.NO );
        return rb;
    }


    public RequestBuilder getUninstallCommand()
    {
        RequestBuilder rb =
                new RequestBuilder( "apt-get --force-yes --assume-yes purge " + PACKAGE_NAME ).withTimeout( 60 );
        return rb;

    }


    public RequestBuilder getCheckInstalledCommand()
    {
        RequestBuilder rb =
                new RequestBuilder( "dpkg -l | grep '^ii' | grep " + Common.PACKAGE_PREFIX_WITHOUT_DASH );
        return rb;
    }
}
