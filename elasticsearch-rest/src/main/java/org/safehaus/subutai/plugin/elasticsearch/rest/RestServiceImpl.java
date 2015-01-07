package org.safehaus.subutai.plugin.elasticsearch.rest;


import javax.ws.rs.core.Response;

import org.safehaus.subutai.plugin.elasticsearch.api.Elasticsearch;


public class RestServiceImpl implements RestService
{

    private Elasticsearch elasticsearch;


    public Elasticsearch getElasticsearch()
    {
        return elasticsearch;
    }


    public void setElasticsearch( final Elasticsearch elasticsearch )
    {
        this.elasticsearch = elasticsearch;
    }


    @Override
    public Response listClusters()
    {
        return null;
    }


    @Override
    public Response installCluster( final String clusterName, final int numberOfNodes )
    {
        return null;
    }


    @Override
    public Response uninstallCluster( final String clusterName )
    {
        return null;
    }


    @Override
    public Response checkAllNodes( final String clusterName )
    {
        return null;
    }


    @Override
    public Response startAllNodes( final String clusterName )
    {
        return null;
    }


    @Override
    public Response stopAllNodes( final String clusterName )
    {
        return null;
    }


    @Override
    public Response addNode( final String clusterName, final String node )
    {
        return null;
    }


    @Override
    public Response destroyNode( final String clusterName, final String node )
    {
        return null;
    }
}
