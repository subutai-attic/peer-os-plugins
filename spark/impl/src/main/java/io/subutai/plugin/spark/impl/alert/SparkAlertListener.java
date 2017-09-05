package io.subutai.plugin.spark.impl.alert;


import java.util.List;
import java.util.Set;

import io.subutai.common.environment.Environment;
import io.subutai.common.metric.QuotaAlertValue;
import io.subutai.common.peer.AlertHandlerException;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.peer.ExceededQuotaAlertHandler;
import io.subutai.plugin.spark.api.SparkClusterConfig;
import io.subutai.plugin.spark.impl.SparkImpl;


/**
 * Node resource threshold excess alert listener
 */
public class SparkAlertListener extends ExceededQuotaAlertHandler
{

    private SparkImpl spark;
    private static final String HANDLER_ID = "DEFAULT_SPARK_QUOTA_EXCEEDED_ALERT_HANDLER";


    public SparkAlertListener( final SparkImpl spark )
    {
        this.spark = spark;
    }


    @Override
    public String getId()
    {
        return HANDLER_ID;
    }


    @Override
    public String getDescription()
    {
        return "Node resource threshold excess default alert handler for spark.";
    }


    @Override
    public void process( final Environment environment, final QuotaAlertValue alert ) throws AlertHandlerException
    {
        ///find spark cluster by environment id
        List<SparkClusterConfig> clusters = spark.getClusters();

        SparkClusterConfig targetCluster = null;
        for ( SparkClusterConfig cluster : clusters )
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
                    String.format( "Cluster not found by environment id %s", environment.getId() ), null );
        }


        //get environment containers and find alert's source host
        Set<EnvironmentContainerHost> containers = environment.getContainerHosts();

        EnvironmentContainerHost sourceHost = null;
        final String hostId = alert.getValue().getHostId().getId();
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
            throw new AlertHandlerException( String.format( "Alert source host %s not found in environment", hostId ) );
        }

        //check if source host belongs to found zookeeper cluster
        if ( !targetCluster.getAllNodesIds().contains( hostId ) )
        {
            LOGGER.info( String.format( "Alert source host %s does not belong to Spark cluster", hostId ) );
            return;
        }

        notifyUser();
    }


    private void notifyUser()
    {
        //TODO implement
    }
}

