package io.subutai.plugin.ceph.impl;


import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.plugin.ceph.api.CephInterface;


public class CephImpl implements CephInterface
{
    private EnvironmentManager environmentManager;

    @Override
    public String configureCluster( final String environmentId, final String lxcHostName, final String clusterName )
    {
        ClusterOperationHandler operationHandler = new ClusterOperationHandler(environmentManager, environmentId, lxcHostName, clusterName );
        return operationHandler.execute();
    }


    public void setEnvironmentManager( final EnvironmentManager environmentManager )
    {
        this.environmentManager = environmentManager;
    }
}
