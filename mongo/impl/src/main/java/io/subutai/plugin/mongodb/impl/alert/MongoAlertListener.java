package io.subutai.plugin.mongodb.impl.alert;


import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.subutai.common.environment.Environment;
import io.subutai.common.metric.QuotaAlertValue;
import io.subutai.common.peer.AlertHandlerException;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.peer.ExceededQuotaAlertHandler;
import io.subutai.plugin.mongodb.api.MongoClusterConfig;
import io.subutai.plugin.mongodb.impl.MongoImpl;


public class MongoAlertListener extends ExceededQuotaAlertHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger( MongoAlertListener.class );
    private static final String HANDLER_ID = "DEFAULT_MONGO_EXCEEDED_QUOTA_ALERT_HANDLER";
    private MongoImpl mongo;


    public MongoAlertListener( final MongoImpl mongo )
    {
        this.mongo = mongo;
    }


    private void throwAlertException( String context ) throws AlertHandlerException
    {
        throw new AlertHandlerException( context );
    }


    @Override
    public void process( final Environment environment, final QuotaAlertValue quotaAlertValue )
            throws AlertHandlerException
    {
        //find mongo cluster by environment id
        List<MongoClusterConfig> clusters = mongo.getClusters();

        MongoClusterConfig targetCluster = null;
        for ( MongoClusterConfig cluster : clusters )
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

        //get environment containers and find alert source host
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
            throwAlertException( String.format( "Alert source host %s not found " + "in environment", hostId ) );
            return;
        }

        //check if source host belongs to found mongo cluster
        if ( !targetCluster.getAllNodes().contains( sourceHost.getId() ) )
        {
            LOGGER.info( String.format( "Alert source host %s does not belong " + "to Mongo cluster", hostId ) );
            return;
        }

        notifyUser();
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
        return "Node resource exceeded threshold alert handler for mongo cluster.";
    }
}
