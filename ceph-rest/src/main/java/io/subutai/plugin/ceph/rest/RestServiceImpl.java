package io.subutai.plugin.ceph.rest;


import javax.ws.rs.core.Response;

import io.subutai.plugin.ceph.api.CephInterface;


public class RestServiceImpl implements RestService
{
    private CephInterface cephInterface;


    @Override
    public Response configureCluster( final String environmentId, final String lxcHostName, final String clusterName )
    {
        String str = cephInterface.configureCluster( environmentId, lxcHostName, clusterName );
        return Response.status( Response.Status.OK ).entity(str).build();
    }


    public void setCephInterface( final CephInterface cephInterface )
    {
        this.cephInterface = cephInterface;
    }
}
