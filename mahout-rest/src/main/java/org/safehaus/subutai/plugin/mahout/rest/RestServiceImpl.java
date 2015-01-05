package org.safehaus.subutai.plugin.mahout.rest;


import java.util.List;
import java.util.UUID;

import javax.ws.rs.core.Response;
import org.safehaus.subutai.common.util.JsonUtil;
import org.safehaus.subutai.plugin.mahout.api.Mahout;
import org.safehaus.subutai.plugin.mahout.api.MahoutClusterConfig;
import org.safehaus.subutai.plugin.mahout.api.TrimmedMahoutClusterConfig;


/**
 * Created by bahadyr on 9/4/14.
 */
public class RestServiceImpl implements RestService
{

    private Mahout mahoutManager;


    public Mahout getMahoutManager()
    {
        return mahoutManager;
    }


    public void setMahoutManager( final Mahout mahoutManager )
    {
        this.mahoutManager = mahoutManager;
    }



    @Override
    public Response listClusters()
    {
        List<MahoutClusterConfig> clusters = mahoutManager.getClusters();
        String clusterNames = JsonUtil.toJson( clusters );
        return Response.status( Response.Status.OK ).entity( clusterNames ).build();
    }


    @Override
    public Response getCluster( final String clusterName )
    {
        MahoutClusterConfig mahoutConfig = mahoutManager.getCluster( clusterName );
        String cluster = JsonUtil.toJson( mahoutConfig );
        return Response.status( Response.Status.OK ).entity( cluster ).build();
    }


    @Override
    public Response createCluster( final String config )
    {
        TrimmedMahoutClusterConfig tmcc = JsonUtil.fromJson( config, TrimmedMahoutClusterConfig.class );

        MahoutClusterConfig mahoutConfig = new MahoutClusterConfig();
        mahoutConfig.setClusterName( tmcc.getClusterName() );

        for ( String node : tmcc.getNodes() )
        {

            mahoutConfig.getNodes().add( UUID.fromString( node ) );
        }

        UUID uuid = mahoutManager.installCluster( mahoutConfig );

        String operationId = wrapUUID( uuid );
        return Response.status( Response.Status.CREATED ).entity( operationId ).build();
    }


    @Override
    public Response destroyCluster( final String clusterName )
    {
        UUID uuid = mahoutManager.uninstallCluster( clusterName );
        String operationId = wrapUUID( uuid );
        return Response.status( Response.Status.OK ).entity( operationId ).build();
    }


    @Override
    public Response startCluster( final String clusterName )
    {
        return null;
    }


    @Override
    public Response stopCluster( final String clusterName )
    {
        return null;
    }


    @Override
    public Response addNode( final String clusterName, final String lxcHostname )
    {
        UUID uuid = mahoutManager.addNode( clusterName, lxcHostname );
        String operationId = wrapUUID( uuid );
        return Response.status( Response.Status.CREATED ).entity( operationId ).build();
    }


    @Override
    public Response destroyNode( final String clusterName, final String lxcHostname )
    {
        UUID uuid = mahoutManager.uninstalllNode( clusterName, lxcHostname );
        String operationId = wrapUUID( uuid );
        return Response.status( Response.Status.OK ).entity( operationId ).build();
    }


    @Override
    public Response checkNode( final String clusterName, final String lxcHostname )
    {
        UUID uuid = mahoutManager.checkNode( clusterName, lxcHostname );
        String operationId = wrapUUID( uuid );
        return Response.status( Response.Status.OK ).entity( operationId ).build();
    }


    private String wrapUUID( UUID uuid )
    {
        return JsonUtil.toJson( "OPERATION_ID", uuid );
    }
}
