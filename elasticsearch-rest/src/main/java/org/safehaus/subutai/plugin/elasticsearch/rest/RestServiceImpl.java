package org.safehaus.subutai.plugin.elasticsearch.rest;


import java.util.List;

import javax.ws.rs.core.Response;

import org.safehaus.subutai.common.util.JsonUtil;
import org.safehaus.subutai.plugin.elasticsearch.api.Elasticsearch;
import org.safehaus.subutai.plugin.elasticsearch.api.ElasticsearchClusterConfiguration;

import com.google.common.collect.Lists;


public class RestServiceImpl implements RestService
{
    private static final String OPERATION_ID = "OPERATION_ID";

    private Elasticsearch elasticsearch;


    public RestServiceImpl( final Elasticsearch elasticsearch )
    {
        this.elasticsearch = elasticsearch;
    }


    @Override
    public Response listClusters()
    {
        List<ElasticsearchClusterConfiguration> configList = elasticsearch.getClusters();
        List<String> clusterNames = Lists.newArrayList();

        for ( ElasticsearchClusterConfiguration config : configList )
        {
            clusterNames.add( config.getClusterName() );
        }

        String clusters = JsonUtil.GSON.toJson( clusterNames );
        return Response.status( Response.Status.OK ).entity( clusters ).build();
    }


    @Override
    public Response getClusterConfig( final String clusterName )
    {
        String cluster = JsonUtil.GSON.toJson( elasticsearch.getCluster( clusterName ) );
        return Response.status( Response.Status.OK ).entity( cluster ).build();
    }


    @Override
    public Response installCluster( final String config )
    {
        TrimmedClusterConfig trimmedClusterConfig = JsonUtil.fromJson( config, TrimmedClusterConfig.class );

        ElasticsearchClusterConfiguration fullConfig = new ElasticsearchClusterConfiguration();
        fullConfig.setEnvironmentId( trimmedClusterConfig.getEnvironmentId() );
        fullConfig.setClusterName( trimmedClusterConfig.getClusterName() );
        fullConfig.getNodes().addAll( trimmedClusterConfig.getNodes() );


        String operationId = JsonUtil.toJson( OPERATION_ID, elasticsearch.installCluster( fullConfig ) );

        return Response.status( Response.Status.ACCEPTED ).entity( operationId ).build();
    }


    @Override
    public Response uninstallCluster( final String clusterName )
    {
        String operationId = JsonUtil.toJson( OPERATION_ID, elasticsearch.uninstallCluster( clusterName ) );
        return Response.status( Response.Status.OK ).entity( operationId ).build();
    }


    @Override
    public Response checkNode( final String clusterName, final String node )
    {
        String operationId = JsonUtil.toJson( OPERATION_ID, elasticsearch.checkNode( clusterName, node ) );
        return Response.status( Response.Status.OK ).entity( operationId ).build();
    }


    @Override
    public Response startNode( final String clusterName, final String node )
    {
        String operationId = JsonUtil.toJson( OPERATION_ID, elasticsearch.startNode( clusterName, node ) );
        return Response.status( Response.Status.OK ).entity( operationId ).build();
    }


    @Override
    public Response stopNode( final String clusterName, final String node )
    {
        String operationId = JsonUtil.toJson( OPERATION_ID, elasticsearch.stopNode( clusterName, node ) );
        return Response.status( Response.Status.OK ).entity( operationId ).build();
    }


    @Override
    public Response addNode( final String clusterName, final String node )
    {
        String operationId = JsonUtil.toJson( OPERATION_ID, elasticsearch.addNode( clusterName, node ) );
        return Response.status( Response.Status.OK ).entity( operationId ).build();
    }


    @Override
    public Response destroyNode( final String clusterName, final String node )
    {
        String operationId = JsonUtil.toJson( OPERATION_ID, elasticsearch.destroyNode( clusterName, node ) );
        return Response.status( Response.Status.OK ).entity( operationId ).build();
    }
}
