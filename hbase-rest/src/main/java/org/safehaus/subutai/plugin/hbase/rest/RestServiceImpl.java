package org.safehaus.subutai.plugin.hbase.rest;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import javax.ws.rs.core.Response;

import org.safehaus.subutai.common.tracker.OperationState;
import org.safehaus.subutai.common.tracker.TrackerOperationView;
import org.safehaus.subutai.common.util.CollectionUtil;
import org.safehaus.subutai.common.util.JsonUtil;
import org.safehaus.subutai.core.tracker.api.Tracker;
import org.safehaus.subutai.plugin.common.api.ClusterException;
import org.safehaus.subutai.plugin.hbase.api.HBase;
import org.safehaus.subutai.plugin.hbase.api.HBaseConfig;


public class RestServiceImpl implements RestService
{

    private HBase hbaseManager;
    private Tracker tracker;


    @Override
    public Response listClusters()
    {
        List<HBaseConfig> configs = hbaseManager.getClusters();
        List<String> clusterNames = new ArrayList<>();
        for ( HBaseConfig config : configs )
        {
            clusterNames.add( config.getClusterName() );
        }
        String clusters = JsonUtil.toJson( clusterNames );
        return Response.status( Response.Status.OK ).entity( clusters ).build();
    }


    @Override
    public Response getCluster( final String source )
    {
        HBaseConfig cluster = hbaseManager.getCluster( source );
        String clusterName = JsonUtil.toJson( cluster );
        return Response.status( Response.Status.OK ).entity( clusterName ).build();
    }


    @Override
    public Response configureCluster( final String config )
    {
        TrimmedHBaseConfig trimmedHBaseConfig = JsonUtil.fromJson( config, TrimmedHBaseConfig.class );
        HBaseConfig hbaseConfig = new HBaseConfig();
        hbaseConfig.setClusterName( trimmedHBaseConfig.getClusterName() );
        hbaseConfig.setHadoopClusterName( trimmedHBaseConfig.getHadoopClusterName() );
        hbaseConfig.setDomainName( trimmedHBaseConfig.getDomainName() );
        hbaseConfig.setEnvironmentId( UUID.fromString( trimmedHBaseConfig.getEnvironmentId() ) );
        hbaseConfig.setHbaseMaster( UUID.fromString( trimmedHBaseConfig.getHmaster() ) );

        if ( !CollectionUtil.isCollectionEmpty( trimmedHBaseConfig.getQuorumPeers() ) )
        {
            Set<UUID> slaveNodes = new HashSet<>();
            for ( String node : trimmedHBaseConfig.getQuorumPeers() )
            {
                slaveNodes.add( UUID.fromString( node ) );
            }
            hbaseConfig.getQuorumPeers().addAll( slaveNodes );
        }

        if ( !CollectionUtil.isCollectionEmpty( trimmedHBaseConfig.getRegionServers() ) )
        {
            Set<UUID> slaveNodes = new HashSet<>();
            for ( String node : trimmedHBaseConfig.getRegionServers() )
            {
                slaveNodes.add( UUID.fromString( node ) );
            }
            hbaseConfig.getRegionServers().addAll( slaveNodes );
        }

        if ( !CollectionUtil.isCollectionEmpty( trimmedHBaseConfig.getBackupMasters() ) )
        {
            Set<UUID> slaveNodes = new HashSet<>();
            for ( String node : trimmedHBaseConfig.getBackupMasters() )
            {
                slaveNodes.add( UUID.fromString( node ) );
            }
            hbaseConfig.getBackupMasters().addAll( slaveNodes );
        }

        UUID uuid = hbaseManager.installCluster( hbaseConfig );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response destroyCluster( final String clusterName )
    {
        UUID uuid = hbaseManager.uninstallCluster( clusterName );

        waitUntilOperationFinish( uuid );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response startCluster( final String clusterName )
    {
        UUID uuid = hbaseManager.startCluster( clusterName );

        waitUntilOperationFinish( uuid );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response stopCluster( final String clusterName )
    {
        UUID uuid = hbaseManager.stopCluster( clusterName );

        waitUntilOperationFinish( uuid );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response addNode( final String clusterName, final String lxcHostName, final String nodeType )
    {
        UUID uuid = hbaseManager.addNode( clusterName, nodeType );

        waitUntilOperationFinish( uuid );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response destroyNode( final String clusterName, final String containerId, final String nodeType )
    {
        UUID uuid = hbaseManager.destroyNode( clusterName, containerId );

        waitUntilOperationFinish( uuid );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response checkNode( final String clusterName, final String hostname )
    {
        // todo node type should be given
        UUID uuid = hbaseManager.checkNode( clusterName, hostname );

        waitUntilOperationFinish( uuid );
        OperationState state = waitUntilOperationFinish( uuid );
        return createResponse( uuid, state );
    }


    @Override
    public Response autoScaleCluster( final String clusterName, final boolean scale )
    {
        String message = "enabled";
        HBaseConfig config = hbaseManager.getCluster( clusterName );
        config.setAutoScaling( scale );
        try
        {
            hbaseManager.saveConfig( config );
        }
        catch ( ClusterException e )
        {
            e.printStackTrace();
        }
        if ( scale == false )
        {
            message = "disabled";
        }

        return Response.status( Response.Status.OK ).entity( "Auto scale is " + message + " successfully" ).build();
    }


    private Response createResponse( UUID uuid, OperationState state )
    {
        TrackerOperationView po = tracker.getTrackerOperation( HBaseConfig.PRODUCT_NAME, uuid );
        if ( state == OperationState.FAILED )
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).entity( po.getLog() ).build();
        }
        else if ( state == OperationState.SUCCEEDED )
        {
            return Response.status( Response.Status.OK ).entity( po.getLog() ).build();
        }
        else
        {
            return Response.status( Response.Status.INTERNAL_SERVER_ERROR ).entity( "Timeout" ).build();
        }
    }


    private OperationState waitUntilOperationFinish( UUID uuid )
    {
        OperationState state = null;
        long start = System.currentTimeMillis();
        while ( !Thread.interrupted() )
        {
            TrackerOperationView po = tracker.getTrackerOperation( HBaseConfig.PRODUCT_KEY, uuid );
            if ( po != null )
            {
                if ( po.getState() != OperationState.RUNNING )
                {
                    state = po.getState();
                    break;
                }
            }
            try
            {
                Thread.sleep( 1000 );
            }
            catch ( InterruptedException ex )
            {
                break;
            }
            if ( System.currentTimeMillis() - start > ( 60 * 1000 ) )
            {
                break;
            }
        }
        return state;
    }


    public HBase getHbaseManager()
    {
        return hbaseManager;
    }


    public void setHbaseManager( final HBase hbaseManager )
    {
        this.hbaseManager = hbaseManager;
    }


    public Tracker getTracker()
    {
        return tracker;
    }


    public void setTracker( final Tracker tracker )
    {
        this.tracker = tracker;
    }
}
