package org.safehaus.subutai.plugin.elasticsearch.api;


import java.util.UUID;

import org.safehaus.subutai.plugin.common.api.ApiBase;
import org.safehaus.subutai.plugin.common.api.ClusterException;


public interface Elasticsearch extends ApiBase<ElasticsearchClusterConfiguration>
{

    /**
     * Checks nodes if Elasticsearch is running or not.
     * @param clusterName cluster name
     * @param lxcHostname container hostname
     * @return
     */
    public UUID checkNode( String clusterName, String lxcHostname );

    /**
     * Starts Elasticsearch on given container.
     * @param clusterName cluster name
     * @param lxcHostname container hostname
     * @return
     */
    public UUID startNode( String clusterName, String lxcHostname );

    /**
     * Stops Elasticsearch on given container.
     * @param clusterName cluster name
     * @param lxcHostname container hostname
     * @return
     */
    public UUID stopNode( String clusterName, String lxcHostname );

    /**
     * Destroys container.
     * @param clusterName cluster name
     * @param lxcHostname container hostname
     * @return
     */
    public UUID destroyNode( String clusterName, String lxcHostname );

    /**
     * Removes cluster config object from database.
     * @param clusterName cluster name
     * @return uuid
     */
    public UUID removeCluster( String clusterName );


    /**
     * Start all nodes in cluster
     * @param clusterName cluster name
     * @return
     */
    public UUID startCluster( String clusterName );


    /**
     * Stop all nodes in cluster
     * @param clusterName cluster name
     * @return
     */
    public UUID stopCluster( String clusterName );


    /**
     * Saves/Updates cluster config in database
     *
     * @param config - config to update
     */
    public void saveConfig( ElasticsearchClusterConfiguration config ) throws ClusterException;

    /**
     * Deletes cluster config in database
     *
     * @param config config to be deleted
     * @throws ClusterException
     */
    public void deleteConfig( ElasticsearchClusterConfiguration config ) throws ClusterException;
}
