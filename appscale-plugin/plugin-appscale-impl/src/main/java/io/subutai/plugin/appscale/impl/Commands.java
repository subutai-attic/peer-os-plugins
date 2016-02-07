/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.appscale.impl;


import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.LoggerFactory;

import io.subutai.plugin.appscale.api.AppScaleConfig;


/**
 *
 * @author caveman
 * @author Beyazıt Kelçeoğlu
 */
public class Commands
{
    AppScaleConfig config;
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger ( Commands.class.getName () );


    public Commands ( AppScaleConfig config )
    {
        this.config = config;
    }


    /**
     * we have to install zookeeper manually and configure it.
     *
     * @return
     */
    public static String getInstallZookeeper ()
    {
        return ( "apt-get install -y zookeeper zookeeperd zookeeper-bin" );
    }


    public static String getEditZookeeperConf ()
    {
        try
        {
            List<String> lines = Files.readAllLines ( Paths.get ( "/etc/init/zookeeper.conf" ) );
            lines.stream ().filter ( (line) -> ( line.contains ( "limit" ) ) ).forEach ( (line)
                    ->
                    {
                        line = "#" + line;
            } );
            Files.write ( Paths.get ( "/etc/init/zookeeper.conf" ), lines );
            return "Complated";
        }
        catch ( IOException ex )
        {
            LOG.error ( ex.getLocalizedMessage () );
        }

        return null;
    }


    public static List<String> getZookeeperStopAndDisable ()
    {
        List<String> ret = new ArrayList<> ();
        ret.add ( "/etc/init.d/zookeeper stop" );
        ret.add ( "disableservice zookeeper" );
        return ret;
    }


    public static String getAppScaleStartCommand ()
    {
        return ( "/root/appscale-tools/bin/appscale up" );
    }


    public static String getAppScaleStopCommand ()
    {
        return ( "/root/appscale-tools/bin/appscale down" );
    }


    public static String getAppscaleInit ()
    {
        return ( "/root/appscale-tools/bin/appscale init cluster" );
    }


    public static String getAppscaleToolsBuild ()
    {
        return ( "bash /root/appscale-tools/debian/appscale_build.sh" );
    }


    public static String getAppscaleBuild ()
    {
        return ( "bash /root/appscale/debian/appscale_build.sh" );
    }


    public static String getRemoveSubutaiList ()
    {
        return ( "rm -f /etc/apt/sources.list.d/subutai-repo.list" );
    }


    public static String getAutoRemove ()
    {
        return ( "apt-get autoremove" );
    }


    public static String getTermColorInstall ()
    {
        // forget to install term color in template...

        return ( "pip install termcolor" );
    }

}

