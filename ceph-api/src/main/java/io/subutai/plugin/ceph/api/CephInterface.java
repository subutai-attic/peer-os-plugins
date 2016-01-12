package io.subutai.plugin.ceph.api;


public interface CephInterface
{
    String configureCluster( String environmentId, String lxcHostName, String clusterName );
}
