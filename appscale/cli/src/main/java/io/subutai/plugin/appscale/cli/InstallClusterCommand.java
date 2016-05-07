/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.appscale.cli;


import java.util.Arrays;
import java.util.UUID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;

import io.subutai.common.peer.Peer;
import io.subutai.common.tracker.OperationState;
import io.subutai.common.tracker.TrackerOperationView;
import io.subutai.core.plugincommon.api.NodeState;
import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.appscale.api.AppScaleConfig;
import io.subutai.plugin.appscale.api.AppScaleInterface;


/**
 *
 * @author caveman
 * @author Beyazıt Kelçeoğlu
 */
@Command ( scope = "appscale", name = "install-cluster", description = "Install Cluster" )
public class InstallClusterCommand extends OsgiCommandSupport
{

    @Argument ( index = 0, name = "clusterName", description = "name of cluster", required = true, multiValued = false )
    private String clusterName = null;
    @Argument ( index = 1, name = "domainName", description = "name of domain", required = true, multiValued = false )
    private String domainName = null;
    @Argument ( index = 2, name = "environmentId", description = "environment id", required = true, multiValued = false )
    private String environmentId;

    @Argument ( index = 3, name = "userDomain", description = "links to appscale console", required = true, multiValued = false )
    private String userDomain;

    @Argument ( index = 4, name = "zookeeperName", description = "name of zookeeper", required = false, multiValued = false )
    private String zookeeperName = null;
    @Argument ( index = 5, name = "cassandraName", description = "name of cassandraName", required = false, multiValued = false )
    private String cassandraName = null;

    @Argument ( index = 6, name = "scaleOption", description = "scale option, static / scale", required = false, multiValued = false )
    private String scaleOption = null;


    private AppScaleInterface appScaleInterface;
    private Tracker tracker;
    private Peer peer;


    private static final Logger LOG = LoggerFactory.getLogger ( InstallClusterCommand.class.getName () );


    public String getclusterName ()
    {
        return clusterName;
    }


    public void setclusterName ( String clusterName )
    {
        this.clusterName = clusterName;
    }


    public String getDomainName ()
    {
        return domainName;
    }


    public void setDomainName ( String domainName )
    {
        this.domainName = domainName;
    }


    public String getEnvironmentId ()
    {
        return environmentId;
    }


    public void setEnvironmentId ( String environmentId )
    {
        this.environmentId = environmentId;
    }


    public AppScaleInterface getAppScaleInterface ()
    {
        return appScaleInterface;
    }


    public void setAppScaleInterface ( AppScaleInterface appScaleInterface )
    {
        this.appScaleInterface = appScaleInterface;
    }


    public Tracker getTracker ()
    {
        return tracker;
    }


    public void setTracker ( Tracker tracker )
    {
        this.tracker = tracker;
    }


    public String getZookeeperName ()
    {
        return zookeeperName;
    }


    public void setZookeeperName ( String zookeeperName )
    {
        this.zookeeperName = zookeeperName;
    }


    public String getCassandraName ()
    {
        return cassandraName;
    }


    public void setCassandraName ( String cassandraName )
    {
        this.cassandraName = cassandraName;
    }


    public String getUserDomain ()
    {
        return userDomain;
    }


    public void setUserDomain ( String userDomain )
    {
        this.userDomain = userDomain;
    }


    public Peer getPeer ()
    {
        return peer;
    }


    public void setPeer ( Peer peer )
    {
        this.peer = peer;
    }


    public String getScaleOption ()
    {
        return scaleOption;
    }


    public void setScaleOption ( String scaleOption )
    {
        this.scaleOption = scaleOption;
    }


    @Override
    protected Object doExecute () throws Exception
    {
        LOG.info ( "info: " + "INSTALLING !" );
        AppScaleConfig appScaleConfig = new AppScaleConfig ();
        appScaleConfig.setEnvironmentId ( environmentId );
        appScaleConfig.setClusterName ( clusterName );
        appScaleConfig.setDomain ( userDomain );
        if ( scaleOption == null )
        {
            scaleOption = "static";
        }

        if ( zookeeperName != null )
        {
            appScaleConfig.setZookeeperNodes( Arrays.asList( zookeeperName.split( "," ) ) );
        }
        if ( cassandraName != null )
        {
            appScaleConfig.setCassandraNodes ( Arrays.asList( cassandraName.split( "," ) ) );
        }


        LOG.info ( "installing arguments: "
                + appScaleConfig.getClusterName () + " " + appScaleConfig.getDomain ()
                + " " + appScaleConfig.getEnvironmentId () );
        UUID installCluster = appScaleInterface.installCluster ( appScaleConfig );
        waitUntilOperationFinish ( tracker, installCluster );
        LOG.info ( " uuid " + installCluster );
        return null;
    }


    protected static NodeState waitUntilOperationFinish ( Tracker tracker, UUID uuid )
    {
        NodeState state = NodeState.UNKNOWN;
        long start = System.currentTimeMillis ();
        while ( !Thread.interrupted () )
        {
            TrackerOperationView po = tracker.getTrackerOperation ( AppScaleConfig.PRODUCT_NAME, uuid );
            if ( po != null )
            {
                if ( po.getState () != OperationState.RUNNING )
                {
                    if ( po.getLog ().toLowerCase ().contains ( NodeState.STOPPED.name ().toLowerCase () ) )
                    {
                        state = NodeState.STOPPED;
                    }
                    else if ( po.getLog ().toLowerCase ().contains ( NodeState.RUNNING.name ().toLowerCase () ) )
                    {
                        state = NodeState.RUNNING;
                    }

                    System.out.println ( po.getLog () );
                    break;
                }
            }
            try
            {
                Thread.sleep ( 1000 );
            }
            catch ( InterruptedException ex )
            {
                break;
            }
            if ( System.currentTimeMillis () - start > ( 30 + 3 ) * 1000 )
            {
                break;
            }
        }

        return state;
    }


}

