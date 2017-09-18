package io.subutai.plugin.presto.impl.alert;


import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.subutai.common.environment.Environment;
import io.subutai.common.metric.QuotaAlertValue;
import io.subutai.common.peer.AlertHandlerException;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.peer.ExceededQuotaAlertHandler;
import io.subutai.plugin.presto.api.PrestoClusterConfig;
import io.subutai.plugin.presto.impl.PrestoImpl;


/**
 * Node resource threshold excess alert listener
 */
public class PrestoAlertListener extends ExceededQuotaAlertHandler
{
    private static final Logger LOG = LoggerFactory.getLogger( PrestoAlertListener.class.getName() );

    private static final String HANDLER_ID = "DEFAULT_PRESTO_EXCEEDED_QUOTA_ALERT_HANDLER";
    private PrestoImpl presto;


    public PrestoAlertListener( final PrestoImpl presto )
    {
        this.presto = presto;
    }


    private void throwAlertException( String context ) throws AlertHandlerException
    {
        LOG.error( context );
        throw new AlertHandlerException( context );
    }


    @Override
    public String getId()
    {
        return HANDLER_ID;
    }


    @Override
    public String getDescription()
    {
        return "Node resource exceeded threshold alert handler for presto cluster.";
    }


    @Override
    public void process( final Environment environment, final QuotaAlertValue quotaAlertValue )
            throws AlertHandlerException
    {
        //find cluster by environment id
        List<PrestoClusterConfig> clusters = presto.getClusters();

        PrestoClusterConfig targetCluster = null;
        for ( PrestoClusterConfig cluster : clusters )
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
            return;
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
            return;
        }

        //check if source host belongs to found cluster
        if ( !targetCluster.getAllNodes().contains( hostId ) )
        {
            LOG.info( String.format( "Alert source host %s does not belong to Presto cluster", hostId ) );
            return;
        }

        notifyUser();
    }


    private void notifyUser()
    {
        //TODO implement
    }
}
