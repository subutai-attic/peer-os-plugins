package io.subutai.plugin.oozie.impl.alert;


import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.subutai.common.environment.Environment;
import io.subutai.common.metric.QuotaAlertValue;
import io.subutai.common.peer.AlertHandlerException;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.peer.ExceededQuotaAlertHandler;
import io.subutai.plugin.oozie.api.OozieClusterConfig;
import io.subutai.plugin.oozie.impl.OozieImpl;


public class OozieAlertListener extends ExceededQuotaAlertHandler
{
    private static final Logger LOG = LoggerFactory.getLogger( OozieAlertListener.class );
    private OozieImpl oozie;
    private static final String HANDLER_ID = "DEFAULT_OOZIE_EXCEEDED_QUOTA_ALERT_HANDLER";


    OozieAlertListener( final OozieImpl oozie )
    {
        this.oozie = oozie;
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
        return "Node resource exceeded threshold alert handler for oozie cluster.";
    }


    @Override
    public void process( final Environment environment, final QuotaAlertValue quotaAlertValue )
            throws AlertHandlerException
    {
        //find oozie cluster by environment id
        List<OozieClusterConfig> clusters = oozie.getClusters();

        OozieClusterConfig targetCluster = null;
        for ( OozieClusterConfig cluster : clusters )
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

        //check if source host belongs to found oozie cluster
        if ( !targetCluster.getAllNodes().contains( sourceHost.getId() ) )
        {
            LOG.info( String.format( "Alert source host %s does not belong " + "to Oozie cluster", hostId ) );
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
