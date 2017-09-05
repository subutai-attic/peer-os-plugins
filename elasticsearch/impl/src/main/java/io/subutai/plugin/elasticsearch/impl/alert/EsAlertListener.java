package io.subutai.plugin.elasticsearch.impl.alert;


import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.subutai.common.environment.Environment;
import io.subutai.common.metric.QuotaAlertValue;
import io.subutai.common.peer.AlertHandlerException;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.peer.ExceededQuotaAlertHandler;
import io.subutai.plugin.elasticsearch.api.ElasticsearchClusterConfiguration;
import io.subutai.plugin.elasticsearch.impl.ElasticsearchImpl;


public class EsAlertListener extends ExceededQuotaAlertHandler
{
    private static final Logger LOG = LoggerFactory.getLogger( EsAlertListener.class.getName() );

    private static final String HANDLER_ID = "DEFAULT_ELASTICSEARCH_EXCEEDED_QUOTA_ALERT_HANDLER";

    private ElasticsearchImpl elasticsearch;


    public EsAlertListener( final ElasticsearchImpl elasticsearch )
    {
        this.elasticsearch = elasticsearch;
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
    public String getDescription()
    {
        return "Node resource for elasticsearch exceeded defined threshold.";
    }


    @Override
    public void process( Environment environment, QuotaAlertValue quotaAlertValue ) throws AlertHandlerException
    {
        //find spark cluster by environment id
        List<ElasticsearchClusterConfiguration> clusters = elasticsearch.getClusters();

        ElasticsearchClusterConfiguration targetCluster = null;
        for ( ElasticsearchClusterConfiguration cluster : clusters )
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
            throwAlertException( String.format( "Alert source host %s not" + " found in environment", hostId ) );
            return;
        }

        //check if source host belongs to found cluster
        if ( targetCluster.getNodes() != null && !targetCluster.getNodes().contains( hostId ) )
        {
            LOG.info( String.format( "Alert source host %s does not belong to ES cluster", hostId ) );
            return;
        }

        notifyUser();
    }


    @Override
    public String getId()
    {
        return HANDLER_ID;
    }
}
