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


    public static String getInstallGit ()
    {
        return ( "sudo apt-get install -y git" );
    }


    /**
     * should be run in root folder
     *
     * @return
     */
    public static String getGitAppscale ()
    {
        return ( "sudo git -C '/home/ubuntu' clone git://github.com/AppScale/appscale.git" );
    }


    /**
     * should be run in root folder
     *
     * @return
     */
    public static String getGitAppscaleTools ()
    {
        return ( "sudo git -C '/home/ubuntu' clone git://github.com/AppScale/appscale-tools.git" );
    }


    /**
     * we have to install zookeeper manually and configure it.
     *
     * @return
     */
    public static String getInstallZookeeper ()
    {
        return ( "sudo apt-get install -y zookeeper zookeeperd zookeeper-bin" );
    }


    public static String getAddUbuntuUser ()
    {
        return ( "sudo useradd -m -p 'openssl passwd -1 a' ubuntu" );
    }


    public static String getChangeRootPasswd ()
    {
        return ( "sudo echo 'root:a' | sudo chpasswd" );
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
        return ( "sudo adduser ubuntu root" );
    }


    public static String getCreateSshFolder ()
    {
        return ( "sudo mkdir /home/ubuntu/.ssh" );
    }


    public static String getCreateAppscaleFolder ()
    {
        return ( "sudo mkdir /home/ubuntu/.appscale" );
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
        ret.add ( "sudo /etc/init.d/zookeeper stop" );
        ret.add ( "sudo disableservice zookeeper" );
        return ret;
    }


    public static String getAppScaleStartCommand ()
    {
        return ( "sudo /home/ubuntu/appscale-tools/bin/appscale up --yes" );
    }


    public static String getAppScaleStopCommand ()
    {
        return ( "sudo /home/ubuntu/appscale-tools/bin/appscale down" );
    }


    public static String getAppscaleInit ()
    {
        return ( "sudo /home/ubuntu/appscale-tools/bin/appscale init cluster" );
    }


    public static String getAppscaleToolsBuild ()
    {
        return ( "sudo bash /home/ubuntu/appscale-tools/debian/appscale_build.sh" );
    }


    public static String getAppscaleBuild ()
    {
        return ( "sudo bash /home/ubuntu/appscale/debian/appscale_build.sh" );
    }


    public static String getReinstallKernel ()
    {
        return ( "sudo apt-get install --reinstall 3.19.0.31.generic" );
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
                    "home/ubuntu/appscale/debian", "appscale_install.sh" ) );
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
                    "home/ubuntu/appscale/debian", "appscale_install.sh" ), lines );
        }
        catch ( IOException ex )
        {
            LOG.error ( "error in edit appscale_install.sh " + ex );
            return "ls";
        }

        return "cat home/ubuntu/appscale/debian/appscale_install.sh";
    }


    public static String getInstallWget ()
    {
        return ( "sudo apt-get install -y wget" );
    }


    public static String getUpdateAptGet ()
    {
        return ( "sudo apt-get update" );
    }


    public static String getRemoveSubutaiList ()
    {
        return ( "sudo rm -f /etc/apt/sources.list.d/subutai-repo.list" );
    }


    public static String getDpkgUpdate ()
    {
        return ( "sudo dpkg --configure -a" );
    }


}

