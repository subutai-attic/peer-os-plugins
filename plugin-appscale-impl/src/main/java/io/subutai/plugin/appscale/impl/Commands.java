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
    AppScaleConfig appScaleConfig;
    private static final org.slf4j.Logger LOG = LoggerFactory.getLogger ( ClusterConfiguration.class.getName () );


    public Commands ( AppScaleConfig appScaleConfig )
    {
        this.appScaleConfig = appScaleConfig;
    }


    public static String getInstallGit ()
    {
        return ( "apt-get install -y git-core" );
    }


    /**
     * should be run in root folder
     *
     * @return
     */
    public static String getGitAppscale ()
    {
        return ( "git clone git://github.com/AppScale/appscale.git" );
    }


    /**
     * should be run in root folder
     *
     * @return
     */
    public static String getGitAppscaleTools ()
    {
        return ( "git clone git://github.com/AppScale/appscale-tools.git" );
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


    public static String getAddUbuntuUser ()
    {
        return ( "useradd -m -p (openssl passwd -1 a) ubuntu" );
    }


    public static String getChangeRootPasswd ()
    {
        return ( "usermod -p (openssl passwd -1 a) root" );
    }


    public static String getEditSSHD ()
    {
        try
        {
            List<String> lines = Files.readAllLines ( Paths.get ( "/etc/ssh/sshd_config" ) );
            lines.stream ().filter (
                    (line) -> ( line.contains ( "PermitRootLogin" ) || line.contains ( "PermitEmptyPassword" ) ) ).forEach (
                            (line)
                            ->
                            {
                                if ( "PermitRootLogin".equals ( line ) )
                                {
                                    line = "PermitRootLogin = yes";
                                }
                                if ( "PermitEmptyPassword".equals ( line ) )
                                {
                                    line = "PermitEmptyPassword = yes";

                                }
                    } );
            Files.write ( Paths.get ( "/etc/ssh/sshd_config" ), lines );
            return "Completed";
        }
        catch ( IOException ex )
        {
            LOG.error ( ex.getLocalizedMessage () );
        }

        return null;
    }


    public static String getAddUserToRoot ()
    {
        return ( "adduser ubuntu root" );
    }


    public static String getCreateSshFolder ()
    {
        return ( "mkdir /home/ubuntu/.ssh" );
    }


    public static String getCreateAppscaleFolder ()
    {
        return ( "mkdir /home/ubuntu/.appscale" );
    }


    public static String getExportHome ()
    {
        return ( "export HOME=/root" );
    }


    public static String getFixLocale ()
    {
        return ( "export LC_ALL=\"en_US.UTF-8\"" );
    }


    public static String getEditAppScalefile ()
    {
        return null;
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
        ret.add ( "service zookeeper stop" );
        ret.add ( "disableservice zookeeper" );
        return ret;
    }


    public static String getAppScaleStartCommand ()
    {
        return ( "/root/appscale-tools/appscale up" );
    }


    public static String getAppScaleStopCommand ()
    {
        return ( "/root/appscale-tools/appscale down" );
    }


    public static String getAppscaleInit ()
    {
        return ( "/root/appscale-tools/appscale init" );
    }


    public static String getAppscaleToolsBuild ()
    {
        return ( "bash /root/appscale-tools/debian/appscale_build.sh" );
    }


    public static String getAppscaleBuild ()
    {
        return ( "bash /root/appscale/debian/appscale_build.sh" );
    }


    public static String getReinstallKernel ()
    {
        return ( "apt-get install --reinstall 3.19.0.31.generic" );
    }


    /**
     * find out how to return completed job.
     *
     * @return
     */
    public static String getEditAppscaleInstallSH ()
    {
        try
        { // really like this new way
            List<String> lines = Files.readAllLines ( Paths.get (
                    "/root/appscale/debian/appscale_install.sh" ) );
            lines.stream ().filter ( (String line) -> ( line.contains (
                                                       "increaseconnection" ) || line.contains (
                                                       "installzookeer" ) || line.contains (
                                                       "postinstallzookeeper" ) ) )
                    .forEach ( (String line)
                            ->
                            {
                                line = "#" + line;
                    } );
            Files.write ( Paths.get (
                    "/root/appscale/debian/appscale_install.sh" ), lines );
        }
        catch ( IOException ex )
        {
            LOG.error (
                    ex.getLocalizedMessage () );
            return null;
        }

        return "Completed";
    }
}

