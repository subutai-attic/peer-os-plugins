package io.subutai.plugin.appscale.impl.handler;


import java.math.BigDecimal;
import java.security.PrivilegedAction;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
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
import io.subutai.plugin.appscale.api.AppScaleConfig;
import io.subutai.plugin.appscale.impl.AppScaleImpl;
import io.subutai.plugin.appscale.impl.AppscalePlacementStrategy;


/**
 * Node resource threshold excess alert listener
 */
public class AppscaleAlertHandler extends ExceededQuotaAlertHandler
{
    private static final Logger LOG = LoggerFactory.getLogger( AppscaleAlertHandler.class );
    public static final String HANDLER_ID = "DEFAULT_APPSCALE_QUOTA_EXCEEDED_ALERT_HANDLER";
    private AppScaleImpl appScale;
    private static Set<String> locks = new CopyOnWriteArraySet<>();


    public AppscaleAlertHandler( final AppScaleImpl appScale )
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

        UUID newUuid = UUID.randomUUID(); // for the control if this is alert handled before;
        LOG.debug( String.format( "%s", alertValue ) );
        //find appScale cluster by environment id
        final List<AppScaleConfig> clusters = appScale.getClusters(); // this is already getting from db

        String environmentId = environment.getId();

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

        // let's check if alert handled
/*        String isUUID = targetCluster.getIsUUID ();
        if ( isUUID == null || !newUuid.toString ().equals ( isUUID ) )
        {
            // proceed
            EnvironmentContainerHost sourceHost = getSourceHost ();

            if ( sourceHost == null )
            {
                throwAlertException ( String.format ( "Alert source host %s not found in environment",
                                                      alertValue.getValue ().getHostId ().getId () ), null );
                return;
            }
            if ( isOptPartitionStressed ( alertValue.getValue () ) )
            {
                targetCluster.setIsUUID ( newUuid.toString () ); // let's make sure we entered new value
                createAppEngineInstance ( environment, targetCluster );
            }
        }
        else
        {
            LOG.error ( "this alert handled before" );
        }*/
    }


/*    public Boolean createAppEngineInstance ( Environment environment, AppScaleConfig config )
    {
        if ( isLocked ( environment.getId () ) )
        {
            LOG.debug ( "Environment is locked. Skipping." );
        }
        Boolean modifiyConfig = false;
        try
        {
            lock ( environment.getId () );

            final PeerGroupResources peerGroupResources = appScale.getPeerManager ().getPeerGroupResources ();
            final Map<ContainerSize, ContainerQuota> quotas = appScale.getQuotaManager ().getDefaultQuotas ();

            final List<PeerResources> resources = new ArrayList<> ();
            final List<String> preferredPeerList = getPreferredPeers ( environment, peerGroupResources );

            for ( String peerId : preferredPeerList )
            {
                PeerResources peerResources = findResource ( peerGroupResources.getResources (), peerId );
                resources.add ( peerResources );
            }

            PeerGroupResources preferredPeers = new PeerGroupResources ( resources );

            final List<NodeSchema> nodes = new ArrayList<> ();
            String newAppengineName = "appengine-" + UUID.randomUUID ().toString ();
            LOG.info ( "NEW APPENGINE NAME::::::::::" + newAppengineName );
            nodes.add ( new NodeSchema ( newAppengineName, ContainerSize.HUGE, "appscale271", 0, 0 ) );
            final AppscalePlacementStrategy appscalePlacementStrategy = new AppscalePlacementStrategy ();

            Topology topology
                    = appscalePlacementStrategy.distribute ( environment.getName (), nodes, preferredPeers, quotas );

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

            Iterator<EnvironmentContainerHost> iterator = result[0].iterator ();
            EnvironmentContainerHost next = iterator.next ();
            LOG.info ( "CONTAINER NAME: " + next.getHostname () );
            // grow succeeded
            //TODO: need config modified appscale

            List<String> appenList = config.getAppenList ();
            appenList.add ( next.getHostname () );
            config.setAppenList ( appenList ); // new appengine setted...
            config.setAppengine ( next.getHostname () ); // this is to indicate additional container
            modifiyConfig = modifiyConfig ( environment, config );

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
        return modifiyConfig;
    }*/


    private PeerResources findResource( final List<PeerResources> resources, final String peerId )
    {
        PeerResources result = null;
        Iterator<PeerResources> i = resources.iterator();
        while ( result == null && i.hasNext() )
        {
            PeerResources r = i.next();
            if ( peerId.equals( r.getPeerId() ) )
            {
                result = r;
            }
        }
        return result;
    }


/*    */


    /**
     * @return actuall configuration for the config file in appscale
     *//*
    private Boolean modifiyConfig ( Environment environment, AppScaleConfig targetCluster )
    {
        Boolean modifed = false;
        UUID addNode = appScale.addNode ( targetCluster );
        return modifed;
    }*/
    private List<String> getPreferredPeers( final Environment environment, final PeerGroupResources peerGroupResources )
    {
        final Set<String> usedPeers = new HashSet<>();

        for ( EnvironmentContainerHost c : environment.getContainerHosts() )
        {
            usedPeers.add( c.getPeerId() );
        }

        Set<String> notUsedPeers = new HashSet<>();
        for ( PeerResources resources : peerGroupResources.getResources() )
        {
            if ( !usedPeers.contains( resources.getPeerId() ) )
            {
                notUsedPeers.add( resources.getPeerId() );
            }
        }
        final List<String> result = new ArrayList<>();
        result.addAll( notUsedPeers );
        result.addAll( usedPeers );
        return result;
    }


    private void unlock( final String environmentId )
    {
        locks.remove( environmentId );
    }


    private void lock( final String environmentId )
    {
        locks.add( environmentId );
    }


    private boolean isLocked( final String environmentId )
    {
        return locks.contains( environmentId );
    }


    public boolean isOptPartitionStressed( final ExceededQuota value )
    {
        EnvironmentContainerHost host = getSourceHost();

        if ( value.getContainerResourceType() == ContainerResourceType.OPT )
        {
            final ByteValueResource current = value.getCurrentValue( ByteValueResource.class );
            final ByteValueResource quota = value.getQuotaValue( ByteValueResource.class );
            if ( current == null || quota == null )
            {
                // invalid value
                return false;
            }

            boolean stressed = quota.getValue().compareTo( current.getValue() ) < 1
                    || quota.getValue().multiply( new BigDecimal( "0.8" ) ).compareTo( current.getValue() ) < 1;

            return value.getPercentage() >= 80.0;
        }

        return false;
    }
}

