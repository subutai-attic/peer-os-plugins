package io.subutai.plugin.appscale.impl.handler;


import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.subutai.common.command.CommandUtil;
import io.subutai.common.environment.Environment;
import io.subutai.common.metric.QuotaAlertValue;
import io.subutai.common.peer.AlertHandlerException;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.peer.ExceededQuotaAlertHandler;
import io.subutai.plugin.appscale.api.AppScaleConfig;
import io.subutai.plugin.appscale.impl.AppScaleImpl;


/**
 * Node resource threshold excess alert listener
 */
public class AlertHandler extends ExceededQuotaAlertHandler
{
    private static final Logger LOG = LoggerFactory.getLogger( AlertHandler.class );
    private static final String HANDLER_ID = "DEFAULT_APPSCALE_QUOTA_EXCEEDED_ALERT_HANDLER";
    private AppScaleImpl appScale;


    public AlertHandler( final AppScaleImpl appScale )
    {
        this.appScale = appScale;
    }


    private void throwAlertException( String context, Exception e ) throws AlertHandlerException
    {
        LOG.error( context, e );
        throw new AlertHandlerException( context, e );
    }


    @Override
    public String getId()
    {
        return HANDLER_ID;
    }


    @Override
    public String getDescription()
    {
        return "Node resource threshold excess default alert handler for appScale.";
    }


    @Override
    public void process( final Environment environment, final QuotaAlertValue alertValue ) throws AlertHandlerException
    {
        LOG.debug( String.format( "%s", alertValue ) );
        //find appScale cluster by environment id
        final List<AppScaleConfig> clusters = appScale.getClusters();

        String environmentId = environment.getId();
        String sourceHostId = alertValue.getValue().getHostId().getId();

        AppScaleConfig targetCluster = null;
        for ( AppScaleConfig cluster : clusters )
        {
            if ( cluster.getEnvironmentId().equals( environmentId ) )
            {
                targetCluster = cluster;
                break;
            }
        }

        if ( targetCluster == null )
        {
            throwAlertException( String.format( "Cluster not found by environment id %s", environmentId ), null );
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
                    alertValue.getValue().getHostId().getId() ), null );
            return;
        }

        //check if source host belongs to found appScale cluster
        if ( !targetCluster.getNodes().contains( sourceHost.getId() ) )
        {
            LOG.info( String.format( "Alert source host %s does not belong to AppScale cluster",
                    alertValue.getValue().getHostId() ) );
            return;
        }
    }
}

