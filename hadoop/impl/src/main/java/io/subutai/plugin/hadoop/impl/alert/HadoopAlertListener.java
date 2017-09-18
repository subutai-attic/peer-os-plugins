package io.subutai.plugin.hadoop.impl.alert;


import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.subutai.common.environment.Environment;
import io.subutai.common.metric.QuotaAlertValue;
import io.subutai.common.peer.AlertHandlerException;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.peer.ExceededQuotaAlertHandler;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.hadoop.impl.HadoopImpl;


/**
 * Node resource threshold excess alert listener
 */
public class HadoopAlertListener extends ExceededQuotaAlertHandler
{
    private static final Logger LOG = LoggerFactory.getLogger( HadoopAlertListener.class );
    private static final String HANDLER_ID = "DEFAULT_HADOOP_QUOTA_EXCEEDED_ALERT_HANDLER";
    private HadoopImpl hadoop;


    public HadoopAlertListener( final HadoopImpl hadoop )
    {
        this.hadoop = hadoop;
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
        return "Node resource threshold excess default alert handler for hadoop.";
    }


    @Override
    public void process( final Environment environment, final QuotaAlertValue alertValue ) throws AlertHandlerException
    {
        //find hadoop cluster by environment id
        List<HadoopClusterConfig> clusters = hadoop.getClusters();

        String environmentId = environment.getId();
        String sourceHostId = alertValue.getValue().getHostId().getId();

        HadoopClusterConfig targetCluster = null;
        for ( HadoopClusterConfig cluster : clusters )
        {
            if ( cluster.getEnvironmentId().equals( environmentId ) )
            {
                targetCluster = cluster;
                break;
            }
        }

        if ( targetCluster == null )
        {
            throwAlertException( String.format( "Cluster not found by environment id %s", environmentId ) );
            return;
        }

        EnvironmentContainerHost sourceHost = null;

        //get environment containers and find alert's source host
        Set<EnvironmentContainerHost> containers = environment.getContainerHosts();

        for ( EnvironmentContainerHost containerHost : containers )
        {
            if ( containerHost.getId().equalsIgnoreCase( sourceHostId ) )
            {
                sourceHost = containerHost;
                break;
            }
        }


        if ( sourceHost == null )
        {
            throwAlertException( String.format( "Alert source host %s not found in environment",
                    alertValue.getValue().getHostId().getId() ) );
            return;
        }

        //check if source host belongs to found hadoop cluster
        if ( !targetCluster.getAllNodes().contains( sourceHost.getId() ) )
        {
            LOG.info( String.format( "Alert source host %s does not belong to Hadoop cluster",
                    alertValue.getValue().getHostId() ) );
            return;
        }

        notifyUser();
    }


    private void notifyUser()
    {
        //TODO implement
    }
}

