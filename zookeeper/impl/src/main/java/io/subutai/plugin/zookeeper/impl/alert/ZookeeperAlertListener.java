package io.subutai.plugin.zookeeper.impl.alert;


import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.subutai.common.environment.Environment;
import io.subutai.common.metric.QuotaAlertValue;
import io.subutai.common.peer.AlertHandlerException;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.peer.ExceededQuotaAlertHandler;
import io.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;
import io.subutai.plugin.zookeeper.impl.ZookeeperImpl;


public class ZookeeperAlertListener extends ExceededQuotaAlertHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger( ZookeeperAlertListener.class );
    private static final String HANDLER_ID = "DEFAULT_ZOOKEEPER_EXCEEDED_QUOTA_ALERT_HANDLER";
    private ZookeeperImpl zookeeper;


    public ZookeeperAlertListener( final ZookeeperImpl zookeeper )
    {
        this.zookeeper = zookeeper;
    }


    @Override
    public String getId()
    {
        return HANDLER_ID;
    }


    @Override
    public String getDescription()
    {
        return "Default zookeeper exceeded quota alert handler.";
    }


    @Override
    public void process( final Environment environment, final QuotaAlertValue alert ) throws AlertHandlerException
    {
        //find zookeeper cluster by environment id
        List<ZookeeperClusterConfig> clusters = zookeeper.getClusters();

        ZookeeperClusterConfig targetCluster = null;
        for ( ZookeeperClusterConfig cluster : clusters )
        {
            if ( cluster.getEnvironmentId().equals( environment.getId() ) )
            {
                targetCluster = cluster;
                break;
            }
        }

        if ( targetCluster == null )
        {
            throw new AlertHandlerException(
                    String.format( "Cluster not found by environment id %s", environment.getId() ) );
        }


        //get environment containers and find alert source host
        Set<EnvironmentContainerHost> containers = environment.getContainerHosts();

        ContainerHost sourceHost = null;
        final String hostId = alert.getValue().getHostId().getId();
        for ( ContainerHost containerHost : containers )
        {
            if ( containerHost.getId().equals( hostId ) )
            {
                sourceHost = containerHost;
                break;
            }
        }

        if ( sourceHost == null )
        {
            throw new AlertHandlerException( String.format( "Alert source host %s not found in environment", hostId ),
                    null );
        }

        //check if source host belongs to found zookeeper cluster
        if ( !targetCluster.getNodes().contains( sourceHost.getId() ) )
        {
            LOGGER.info( String.format( "Alert source host %s does not belong to Zookeeper cluster", hostId ) );
            return;
        }

        notifyUser();
    }


    private void notifyUser()
    {
        //TODO implement
    }
}
