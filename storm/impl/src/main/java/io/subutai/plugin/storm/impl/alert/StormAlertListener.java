package io.subutai.plugin.storm.impl.alert;


import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.subutai.common.environment.Environment;
import io.subutai.common.metric.QuotaAlertValue;
import io.subutai.common.peer.AlertHandlerException;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.peer.ExceededQuotaAlertHandler;
import io.subutai.plugin.storm.api.StormClusterConfiguration;
import io.subutai.plugin.storm.impl.StormImpl;


/**
 * Node resource threshold excess alert listener
 */
public class StormAlertListener extends ExceededQuotaAlertHandler
{
    private static final Logger LOG = LoggerFactory.getLogger( StormAlertListener.class.getName() );
    private static final String HANDLER_ID = "DEFAULT_PRESTO_EXCEEDED_QUOTA_ALERT_HANDLER";
    private StormImpl storm;


    public StormAlertListener( final StormImpl storm )
    {
        this.storm = storm;
    }


    @Override
    public void process( final Environment environment, final QuotaAlertValue quotaAlertValue )
            throws AlertHandlerException
    {
        //find storm cluster by environment id
        List<StormClusterConfiguration> clusters = storm.getClusters();

        StormClusterConfiguration targetCluster = null;
        for ( StormClusterConfiguration cluster : clusters )
        {
            if ( cluster.getEnvironmentId().equals( environment.getId() ) )
            {
                targetCluster = cluster;
                break;
            }
        }

        if ( targetCluster == null )
        {
            throwAlertException( String.format( "Cluster not found by environment id %s", environment.getId() ) );
        }

        //get environment containers and find alert's source host
        Set<EnvironmentContainerHost> containers = environment.getContainerHosts();

        EnvironmentContainerHost sourceHost = null;
        String hostId = quotaAlertValue.getValue().getHostId().getId();
        for ( EnvironmentContainerHost containerHost : containers )
        {
            if ( containerHost.getId().equals( hostId ) )
            {
                sourceHost = containerHost;
                break;
            }
        }

        if ( sourceHost == null )
        {
            throwAlertException( String.format( "Alert source host %s not found in environment", hostId ) );
        }

        //check if source host belongs to found storm cluster
        assert targetCluster != null;
        assert sourceHost != null;
        if ( !targetCluster.getAllNodes().contains( hostId ) )
        {
            LOG.info( String.format( "Alert source host %s does not belong to Storm cluster", hostId ) );
            return;
        }

        notifyUser();
    }


    private void throwAlertException( String context ) throws AlertHandlerException
    {
        LOG.error( context );
        throw new AlertHandlerException( context );
    }


    private void notifyUser()
    {
        //TODO implement
    }


    @Override
    public String getId()
    {
        return HANDLER_ID;
    }


    @Override
    public String getDescription()
    {
        return "Node resource exceeded threshold alert handler for storm cluster.";
    }
}

