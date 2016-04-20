package io.subutai.plugin.elasticsearch.api;


import java.util.UUID;

import io.subutai.core.plugincommon.api.ApiBase;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.webui.api.WebuiModule;


public interface Elasticsearch extends ApiBase<ElasticsearchClusterConfiguration>
{

    /**
     * Checks nodes if Elasticsearch is running or not.
     * @param clusterName cluster name
     * @param hostID container id
     * @return
     */
    UUID checkNode( String clusterName, String hostID );

    /**
     * Starts Elasticsearch on given container.
     * @param clusterName cluster name
     * @param hostID container id
     * @return
     */
    UUID startNode( String clusterName, String hostID );

    /**
     * Stops Elasticsearch on given container.
     * @param clusterName cluster name
     * @param hostID container id
     * @return
     */
    UUID stopNode( String clusterName, String hostID );

    /**
     * Destroys container.
     * @param clusterName cluster name
     * @param hostID container id
     * @return
     */
    UUID destroyNode( String clusterName, String hostID );

    /**
     * Removes cluster config object from database.
     * @param clusterName cluster name
     * @return uuid
     */
    UUID removeCluster( String clusterName );


    /**
     * Start all nodes in cluster
     * @param clusterName cluster name
     * @return
     */
    UUID startCluster( String clusterName );


    /**
     * Stop all nodes in cluster
     * @param clusterName cluster name
     * @return
     */
    UUID stopCluster( String clusterName );


    /**
     * Check all nodes in cluster
     * @param clusterName cluster name
     * @return
     */
    UUID checkCluster( String clusterName );



    /**
     * Saves/Updates cluster config in database
     *
     * @param config - config to update
     */
    void saveConfig( ElasticsearchClusterConfiguration config ) throws ClusterException;

    /**
     * Deletes cluster config in database
     *
     * @param config config to be deleted
     * @throws ClusterException
     */
    void deleteConfig( ElasticsearchClusterConfiguration config ) throws ClusterException;

    public WebuiModule getWebModule();

    public void setWebModule( final WebuiModule webModule );

}
