/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.appscale.impl;


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


    public static String getAppScaleStartCommand ()
    {
        return ( "sudo /root/up.sh" );
    }


    public static String getAppScaleStopCommand ()
    {
        return ( "sudo /root/appscale-tools/bin/appscale down" );
    }


    public static String getAppscaleInit ()
    {
        return ( "appscale init cluster" );
    }


    public static String getRemoveSubutaiList ()
    {
        return ( "rm -f /etc/apt/sources.list.d/subutai-repo.list" );
    }


    public static String getCreateLogDir ()
    {
        return ( "mkdir /var/log/appscale" );
    }


    public static String getRunShell ()
    {
        return ( "sudo /root/run.sh" );
    }


    public static String getPsAUX ()
    {
        return ( "cat /AppScalefile" );
    }


    public static String getChangeHostHame ()
    {
        return ( "echo 'domain.com' > /etc/hostname" );
    }


    public static String backUpSSH ()
    {
        return ( "cp -rf /root/.ssh /root/sshBACK" );
    }


    public static String backUpAppscale ()
    {
        return ( "cp -rf /root/.appscale /root/appBACK" );
    }


    public static String revertBackUpSSH ()
    {
        return ( "mv /root/sshBACK/* /root/.ssh" );
    }


    public static String revertBackupAppscale ()
    {
        return ( "mv /root/appBACK/* /root/.appscale" );
    }


}

