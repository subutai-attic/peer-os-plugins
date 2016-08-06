/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.zookeeper.impl;


import io.subutai.common.command.OutputRedirection;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.settings.Common;
import io.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;


/**
 * <p/> <p/> * @todo refactor addPropertyCommand & removePropertyCommand to not use custom scripts
 */
public class Commands
{

    public static final String PACKAGE_NAME = Common.PACKAGE_PREFIX + ZookeeperClusterConfig.PRODUCT_KEY.toLowerCase();


    public static String getCheckInstalledCommand()
    {
        return "dpkg -l | grep '^ii' | grep " + Common.PACKAGE_PREFIX_WITHOUT_DASH;
    }


    public static String getInstallCommand()
    {
        return "apt-get --force-yes --assume-yes install " + PACKAGE_NAME;
    }


    public static RequestBuilder getAptUpdate()
    {
        return new RequestBuilder( "apt-get --force-yes --assume-yes update" ).withTimeout( 2000 )
                                                                              .withStdOutRedirection(
                                                                                      OutputRedirection.NO );
    }


    public static String getUninstallCommand()
    {
        return "apt-get --force-yes --assume-yes purge " + PACKAGE_NAME;
    }


    public static String getStartCommand()
    {
        return "service zookeeper start";
    }

    public static String getStartZkServerCommand()
    {
        return "/usr/share/zookeeper/bin/zkServer.sh start";
    }


    public static String getRestartCommand()
    {
        return "service zookeeper restart";
    }


    public static String getStopCommand()
    {
        return "service zookeeper stop";
    }

    public static String getStopZkServerCommand()
    {
        return "/usr/share/zookeeper/bin/zkServer.sh stop";
    }


    public static String getStatusCommand()
    {
        return "service zookeeper status";
    }

    public static String getStatusZkServerCommand()
    {
        return "/usr/share/zookeeper/bin/zkServer.sh status";
    }


    public static String getZooNHadoopStatusCommand()
    {
        return "service zookeeper status & service hadoop-all status";
    }


    public static String getConfigureClusterCommand( String zooCfgFileContents, String zooCfgFilePath, int id )
    {
        return String.format( "bash /etc/zookeeper/scripts/zookeeper-setID.sh %s && echo '%s' > %s", id, zooCfgFileContents,
                zooCfgFilePath );
    }


    public static String getAddPropertyCommand( String fileName, String propertyName, String propertyValue )
    {
        return String.format( "/opt/zookeeper*/bin/zookeeper-property.sh add %s %s %s", fileName, propertyName,
                propertyValue );
    }


    public static String getRemovePropertyCommand( String fileName, String propertyName )
    {
        return String.format( "/opt/zookeeper*/bin/zookeeper-property.sh remove %s %s", fileName, propertyName );
    }
}
