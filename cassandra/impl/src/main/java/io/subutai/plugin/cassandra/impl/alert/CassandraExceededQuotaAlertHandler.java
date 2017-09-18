package io.subutai.plugin.cassandra.impl.alert;


import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.subutai.common.environment.Environment;
import io.subutai.common.metric.QuotaAlertValue;
import io.subutai.common.peer.AlertHandlerException;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.peer.ExceededQuotaAlertHandler;
import io.subutai.plugin.cassandra.api.CassandraClusterConfig;
import io.subutai.plugin.cassandra.impl.CassandraImpl;


/**
 * Node resource threshold excess alert listener
 */
public class CassandraExceededQuotaAlertHandler extends ExceededQuotaAlertHandler
{
    private static final Logger LOG = LoggerFactory.getLogger( CassandraExceededQuotaAlertHandler.class );
    private static final String HANDLER_ID = "DEFAULT_CASSANDRA_EXCEEDED_QUOTA_ALERT_HANDLER";
    private CassandraImpl cassandra;


    public CassandraExceededQuotaAlertHandler( final CassandraImpl cassandra )
    {
        this.cassandra = cassandra;
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
        return "Node resource threshold excess alert handler for cassandra cluster.";
    }


    @Override
    public void process( final Environment environment, final QuotaAlertValue alert ) throws AlertHandlerException
    {
        //find cluster by environment id
        List<CassandraClusterConfig> clusters = cassandra.getClusters();

        CassandraClusterConfig targetCluster = null;
        for ( CassandraClusterConfig cluster : clusters )
        {
            if ( cluster.getEnvironmentId().equals( environment.getId() ) )
            {
                targetCluster = cluster;
                break;
            }
        }

        if ( targetCluster == null )
        {
            throwAlertException(
                    String.format( "Cluster not found by environment id %s", environment.getEnvironmentId() ) );
        }

        //get environment containers and find alert's source host

        EnvironmentContainerHost sourceHost = getSourceHost();

        if ( sourceHost == null )
        {
            throwAlertException(
                    String.format( "Alert source host %s not found in environment", alert.getValue().getHostId() ) );
        }

        //check if source host belongs to found cluster
        assert targetCluster != null;
        assert sourceHost != null;
        if ( !targetCluster.getSeedNodes().contains( sourceHost.getId() ) )
        {
            LOG.info( String.format( "Alert source host %s does not belong to Cassandra cluster",
                    alert.getValue().getHostId() ) );
            return;
        }

        notifyUser();
    }


    private void notifyUser()
    {
        //TODO implement
    }
}
