/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.mahout.impl;


import io.subutai.common.command.OutputRedirection;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.settings.Common;
import io.subutai.plugin.mahout.api.MahoutClusterConfig;


public class Commands
{
    public static final String PACKAGE_NAME = "subutai-mahout2";

    public RequestBuilder getInstallCommand()
    {
        return new RequestBuilder( "apt-get --force-yes --assume-yes install " + PACKAGE_NAME )
                .withTimeout( 1000 ).withStdOutRedirection( OutputRedirection.NO );
    }


    public RequestBuilder getUninstallCommand()
    {
        return new RequestBuilder( "apt-get --force-yes --assume-yes purge " + PACKAGE_NAME )
                .withTimeout( 60 );
    }


    public RequestBuilder getCheckInstalledCommand()
    {
        return new RequestBuilder( "dpkg -l | grep '^ii' | grep " + PACKAGE_NAME );
    }

    public static final String updateCommand = "apt-get --force-yes --assume-yes update";

}
