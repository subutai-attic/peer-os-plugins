/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.usergrid.cli;


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
import io.subutai.plugin.usergrid.api.UsergridConfig;
import io.subutai.plugin.usergrid.api.UsergridInterface;


/**
 *
 * @author caveman
 * @author Beyazıt Kelçeoğlu
 */
@Command ( scope = "usergrid", name = "install-cluster", description = "Install Cluster" )
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
    @Argument ( index = 4, name = "elasticsearchName", description = "name of elasticsearch", required = false, multiValued = false )
    private String elasticsearchName = null;
    @Argument ( index = 5, name = "cassandraName", description = "name of cassandraName", required = false, multiValued = false )
    private String cassandraName = null;

    private UsergridInterface userGridInterface;
    private Tracker tracker;
    private Peer peer;
    private static final Logger LOG = LoggerFactory.getLogger ( InstallClusterCommand.class.getName () );


    @Override
    protected Object doExecute () throws Exception
    {
        LOG.info ( "doexecute started..." );
        UsergridConfig config = new UsergridConfig ();
        config.setClusterName ( clusterName );
        config.setDomainName ( domainName );
        config.setEnvironmentId ( environmentId );
        config.setUserDomain ( userDomain );
        config.setCassandraName ( Arrays.asList ( cassandraName.split ( "," ) ) );
        config.setElasticSName ( Arrays.asList ( elasticsearchName.split ( "," ) ) );
        LOG.info ( "config set" );
        UUID installCluster = userGridInterface.installCluster ( config );
        LOG.info ( "install started..." );
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
            TrackerOperationView po = tracker.getTrackerOperation ( UsergridConfig.getPRODUCT_NAME (), uuid );
            if ( po != null )
            {
                LOG.info ( po.getState ().toString () );
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


    public String getUserDomain ()
    {
        return userDomain;
    }


    public void setUserDomain ( String userDomain )
    {
        this.userDomain = userDomain;
    }


    public String getElasticsearchName ()
    {
        return elasticsearchName;
    }


    public void setElasticsearchName ( String elasticsearchName )
    {
        this.elasticsearchName = elasticsearchName;
    }


    public String getCassandraName ()
    {
        return cassandraName;
    }


    public void setCassandraName ( String cassandraName )
    {
        this.cassandraName = cassandraName;
    }


    public UsergridInterface getUserGridInterface ()
    {
        return userGridInterface;
    }


    public void setUserGridInterface ( UsergridInterface userGridInterface )
    {
        this.userGridInterface = userGridInterface;
    }


    public Tracker getTracker ()
    {
        return tracker;
    }


    public void setTracker ( Tracker tracker )
    {
        this.tracker = tracker;
    }


    public Peer getPeer ()
    {
        return peer;
    }


    public void setPeer ( Peer peer )
    {
        this.peer = peer;
    }


    public String getClusterName ()
    {
        return clusterName;
    }


    public void setClusterName ( String clusterName )
    {
        this.clusterName = clusterName;
    }


}

