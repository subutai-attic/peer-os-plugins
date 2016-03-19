package io.subutai.plugin.appscale.impl.handler;


import java.math.BigDecimal;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CopyOnWriteArraySet;

import javax.security.auth.Subject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.subutai.common.environment.Environment;
import io.subutai.common.environment.NodeSchema;
import io.subutai.common.environment.Topology;
import io.subutai.common.metric.ExceededQuota;
import io.subutai.common.metric.QuotaAlertValue;
import io.subutai.common.peer.AlertHandlerException;
import io.subutai.common.peer.ContainerSize;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.peer.ExceededQuotaAlertHandler;
import io.subutai.common.quota.ContainerQuota;
import io.subutai.common.resource.ByteValueResource;
import io.subutai.common.resource.ContainerResourceType;
import io.subutai.common.resource.PeerGroupResources;
import io.subutai.common.resource.PeerResources;
import io.subutai.core.identity.api.model.Session;
import io.subutai.core.strategy.api.StrategyException;
import io.subutai.plugin.appscale.api.AppScaleConfig;
import io.subutai.plugin.appscale.impl.AppScaleImpl;
import io.subutai.plugin.appscale.impl.AppscalePlacementStrategy;


/**
 * Node resource threshold excess alert listener
 */
public class AppscaleAlertHandler extends ExceededQuotaAlertHandler
{
    private static final Logger LOG = LoggerFactory.getLogger ( AppscaleAlertHandler.class );
    public static final String HANDLER_ID = "DEFAULT_APPSCALE_QUOTA_EXCEEDED_ALERT_HANDLER";
    private AppScaleImpl appScale;
    private static Set<String> locks = new CopyOnWriteArraySet<> ();


    public AppscaleAlertHandler ( final AppScaleImpl appScale )
    {
        this.appScale = appScale;
    }


    private void throwAlertException ( String context, Exception e ) throws AlertHandlerException
    {
        LOG.error ( context, e );
        throw new AlertHandlerException ( context, e );
    }


    @Override
    public String getId ()
    {
        return HANDLER_ID;
    }


    @Override
    public String getDescription ()
    {
        return "Node resource threshold excess default alert handler for appScale.";
    }


    @Override
    public void process ( final Environment environment, final QuotaAlertValue alertValue ) throws AlertHandlerException
    {
        LOG.debug ( String.format ( "%s", alertValue ) );
        //find appScale cluster by environment id
        final List<AppScaleConfig> clusters = appScale.getClusters ();

        String environmentId = environment.getId ();

        AppScaleConfig targetCluster = null;
        for ( AppScaleConfig cluster : clusters )
        {
            if ( cluster.getEnvironmentId ().equals ( environmentId ) )
            {
                targetCluster = cluster;
                break;
            }
        }

        if ( targetCluster == null )
        {
            throwAlertException ( String.format ( "Cluster not found by environment id %s", environmentId ), null );
            return;
        }

        EnvironmentContainerHost sourceHost = getSourceHost ();

        if ( sourceHost == null )
        {
            throwAlertException ( String.format ( "Alert source host %s not found in environment",
                                                  alertValue.getValue ().getHostId ().getId () ), null );
            return;
        }

        //check if source host belongs to found appScale cluster
        //        if ( !targetCluster.getNodes().contains( sourceHost.getId() ) )
        //        {
        //            LOG.info( String.format( "Alert source host %s does not belong to AppScale cluster",
        //                    alertValue.getValue().getHostId() ) );
        //            return;
        //        }

        if ( isAppengineStressed ( alertValue.getValue () ) )
        {
            createAppEngineInstance ( environment, targetCluster, sourceHost );
        }

    }


    private void createAppEngineInstance ( Environment environment, AppScaleConfig config,
                                           EnvironmentContainerHost sourceHost )
    {
        if ( isLocked ( environment.getId () ) )
        {
            LOG.debug ( "Environment is locked. Skipping." );
        }

        try
        {
            lock ( environment.getId () );

            final PeerGroupResources peerGroupResources = appScale.getPeerManager ().getPeerGroupResources ();
            final Map<ContainerSize, ContainerQuota> quotas = appScale.getQuotaManager ().getDefaultQuotas ();

            final List<PeerResources> resources = new ArrayList<> ();
            final Set<String> preferredPeers = getPreferredPeers ( environment );

            for ( final PeerResources peerResources : peerGroupResources.getResources () )
            {
                if ( preferredPeers.contains ( peerResources.getPeerId () ) )
                {
                    resources.add ( peerResources );
                }
            }

            PeerGroupResources neighbors = new PeerGroupResources ( resources, peerGroupResources.getDistances () );

            final List<NodeSchema> nodes = new ArrayList<> ();
            // which template will use appengine node? while it is "master"
            nodes.add (
                    new NodeSchema ( "appengine-" + UUID.randomUUID ().toString (), ContainerSize.HUGE, "master", 0,
                                     0 ) );
            Topology topology = null;
            try
            {

                topology
                        = new AppscalePlacementStrategy ().distribute ( environment.getName (), nodes, neighbors, quotas );
            }
            catch ( StrategyException e )
            {
                // ignore
            }

            if ( topology == null )
            {
                topology = new AppscalePlacementStrategy ()
                        .distribute ( environment.getName (), nodes, peerGroupResources, quotas );
            }

            final Set<EnvironmentContainerHost>[] result = new Set[]
            {
                null
            };

//            final Session session = appScale.getIdentityManager().login( "internal", "secretSubutai" );
            final Session session = appScale.getIdentityManager ().login ( "admin", "secret" );
            final Topology finalTopology = topology;
            Subject.doAs ( session.getSubject (), new PrivilegedAction<Void> ()
                   {
                       @Override
                       public Void run ()
                       {
                           try
                           {
                               result[0] = appScale.getEnvironmentManager ()
                                       .growEnvironment ( environment.getId (), finalTopology, false );
                           }
                           catch ( Exception ex )
                           {
                               LOG.error ( ex.getMessage (), ex );
                           }
                           return null;
                       }
                   } );

            LOG.debug ( String.format ( "%s", result[0] ) );
            // grow succeeded
            //TODO: need config modified appscale
            Boolean modifiyConfig = modifiyConfig ( environment, config );
            if ( modifiyConfig )
            {
                LOG.info ( "Appscale is scaled up successfully" );
            }
            else
            {
                LOG.error ( "Appscale scale up failed" );
            }
        }
        catch ( Exception e )
        {
            LOG.error ( e.getMessage (), e );
        }
        finally
        {
            unlock ( environment.getId () );
        }
    }


    /**
     *
     * @param environment
     * @param config
     * @return
     *
     * actuall configuration for the config file in appscale
     *
     */
    private Boolean modifiyConfig ( Environment environment, AppScaleConfig targetCluster )
    {
        Boolean modifed = false;
        UUID addNode = appScale.addNode ( targetCluster.getAppengine () );
        return modifed;
    }


    private Set<String> getPreferredPeers ( final Environment environment )
    {
        final Set<String> result = new HashSet<> ();
        for ( EnvironmentContainerHost c : environment.getContainerHosts () )
        {
            result.add ( c.getPeerId () );
        }
        return result;
    }


    private void unlock ( final String environmentId )
    {
        locks.remove ( environmentId );
    }


    private void lock ( final String environmentId )
    {
        locks.add ( environmentId );
    }


    private boolean isLocked ( final String environmentId )
    {
        return locks.contains ( environmentId );
    }


    public boolean isAppengineStressed ( final ExceededQuota value )
    {
        EnvironmentContainerHost host = getSourceHost ();

        boolean isAppengine = isAppengineInstance ( host );
        if ( isAppengine && value.getContainerResourceType () == ContainerResourceType.HOME )
        {
            final ByteValueResource current = value.getCurrentValue ( ByteValueResource.class );
            final ByteValueResource quota = value.getQuotaValue ( ByteValueResource.class );
            if ( current == null || quota == null )
            {
                // invalid value
                return false;
            }

            boolean stressed = quota.getValue ().compareTo ( current.getValue () ) < 1
                    || quota.getValue ().multiply ( new BigDecimal ( "0.8" ) ).compareTo ( current.getValue () ) < 1;

            // appengine instance and stressed /home partition
            return stressed;
        }

        return false;
    }


    private boolean isAppengineInstance ( final EnvironmentContainerHost host )
    {
        // todo: is this host appengine instance?
        // while just return true
        return true;
    }
}

