package io.subutai.plugin.mysql.impl.alert;


import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import io.subutai.common.command.CommandUtil;
import io.subutai.common.environment.Environment;
import io.subutai.common.metric.ContainerHostMetric;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.core.metric.api.AlertListener;
import io.subutai.plugin.mysql.api.MySQLClusterConfig;
import io.subutai.plugin.mysql.impl.MySQLCImpl;


public class MySQLAlertListener implements AlertListener
{

    private static final String MYSQL_ALERT_LISTENER = "MYSQL_ALERT_LISTENER";
    private MySQLCImpl mysql;


    public MySQLAlertListener( final MySQLCImpl mysql )
    {
        this.mysql = mysql;
    }


    @Override
    public void onAlert( final ContainerHostMetric containerHostMetric ) throws Exception
    {
        //find mysql cluster by environment id
        List<MySQLClusterConfig> clusters = mysql.getClusters();

        MySQLClusterConfig targetCluster = null;
        for ( MySQLClusterConfig cluster : clusters )
        {
            if ( cluster.getEnvironmentId().equals( containerHostMetric.getEnvironmentId() ) )
            {
                targetCluster = cluster;
                break;
            }
        }

        if ( targetCluster == null )
        {
            throw new Exception(
                    String.format( "Cluster not found by environment id %s", containerHostMetric.getEnvironmentId() ),
                    null );
        }

        //get cluster environment
        Environment environment =
                mysql.getEnvironmentManager().loadEnvironment( containerHostMetric.getEnvironmentId() );
        if ( environment == null )
        {
            throw new Exception(
                    String.format( "Environment not found by id %s", containerHostMetric.getEnvironmentId() ), null );
        }

        //get environment containers and find alert source host
        Set<EnvironmentContainerHost> containers = environment.getContainerHosts();

        ContainerHost sourceHost = null;
        for ( ContainerHost containerHost : containers )
        {
            if ( containerHost.getId().equals( containerHostMetric.getHostId() ) )
            {
                sourceHost = containerHost;
                break;
            }
        }

        if ( sourceHost == null )
        {
            throw new Exception(
                    String.format( "Alert source host %s not found in environment", containerHostMetric.getHost() ),
                    null );
        }

        //check if source host belongs to found mysql cluster
        if ( !targetCluster.getAllNodes().contains( sourceHost.getId() ) )
        {
            LOG.info( String.format( "Alert source host %s does not belong to mysql cluster",
                    containerHostMetric.getHost() ) );
            return;
        }

        notifyUser();
    }


    @Override
    public String getSubscriberId()
    {
        return MYSQL_ALERT_LISTENER;
    }


    protected void notifyUser()
    {
        //TODO implement
    }
}
