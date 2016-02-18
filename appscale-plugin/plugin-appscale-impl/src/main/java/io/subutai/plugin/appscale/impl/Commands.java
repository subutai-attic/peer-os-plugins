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
        return ( "sudo appscale up" );
    }


    public static String getAppScaleStopCommand ()
    {
        return ( "sudo appscale down" );
    }


    public static String getAppscaleInit ()
    {
        return ( "sudo appscale init cluster" );
    }


    public static String getRemoveSubutaiList ()
    {
        return ( "sudo rm -f /etc/apt/sources.list.d/subutai-repo.list" );
    }


    public static String getCreateLogDir ()
    {
        return ( "sudo mkdir /var/log/appscale" );
    }


    public static String getRunShell ()
    {
        return ( "sudo /root/run.sh 1" );
    }

}

