/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.appscale.impl;


import io.subutai.plugin.appscale.api.AppScaleConfig;


/**
 *
 * @author caveman
 * @author Beyazıt Kelçeoğlu
 */
public class Commands
{
    AppScaleConfig appScaleConfig;


    public Commands( AppScaleConfig appScaleConfig )
    {
        this.appScaleConfig = appScaleConfig;
    }


    public static String getInstallGit()
    {
        return ( "apt-get install -y git-core" );
    }


    /**
     * should be run in root folder
     *
     * @return
     */
    public static String getGitAppscale()
    {
        return ( "git clone git://github.com/AppScale/appscale.git" );
    }


    /**
     * should be run in root folder
     *
     * @return
     */
    public static String getGitAppscaleTools()
    {
        return ( "git clone git://github.com/AppScale/appscale-tools.git" );
    }


    public static String getAddUbuntuUser()
    {
        return ( "adduser ubuntu -p a" );
    }


    public static String getAddUserToRoot()
    {
        return ( "adduser ubuntu root" );
    }


    public static String getCreateSshFolder()
    {
        return ( "mkdir /home/ubuntu/.ssh" );
    }


    public static String getCreateAppscaleFolder()
    {
        return ( "mkdir /home/ubuntu/.appscale" );
    }


    public static String getEditAppScalefile()
    {
        return null;
    }


    public static String getEditZookeeperConf()
    {
        return null;
    }


    public static String getAppScaleStartCommand()
    {
        return ( "/root/appscale-tools/appscale up" );
    }


    public static String getAppScaleStopCommand()
    {
        return ( "/root/appscale-tools/appscale down" );
    }
}

