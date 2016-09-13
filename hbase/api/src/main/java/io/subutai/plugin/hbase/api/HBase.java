/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.hbase.api;


import java.util.UUID;

import io.subutai.core.plugincommon.api.ApiBase;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.webui.api.WebuiModule;


public interface HBase extends ApiBase<HBaseConfig>
{
    /**
     * Install HBase cluster using ready environments.
     *
     * @param config - cluster configuration object
     */
    public UUID installCluster( HBaseConfig config );


    /**
     * Destroys hbase node (regionserver) from hbase cluster. Note that this method just supports removing region
     * servers.
     *
     * @param clusterName cluster name
     * @param hostname hostname to be removed from cluster
     */
    public UUID destroyNode( final String clusterName, final String hostname );


    /**
     * Stops HBase cluster
     *
     * @param clusterName - cluster name
     */
    public UUID stopCluster( String clusterName );


    /**
     * Starts HBase cluster
     *
     * @param clusterName - cluster name
     */
    public UUID startCluster( String clusterName );


    /**
     * Checks hbase services on given node
     *
     * @param clusterName cluster name
     * @param hostname hostname of node to be checked
     */
    public UUID checkNode( String clusterName, String hostname );


    /**
     * Adds new node to given cluster
     *
     * @param clusterName cluster name
     */
    public UUID addNode( String clusterName );


    /**
     * Saves configuration object into database
     *
     * @param config HBase cluster configuration object
     */
    public void saveConfig( final HBaseConfig config ) throws ClusterException;

    public WebuiModule getWebModule();

    public void setWebModule( final WebuiModule webModule );


    /**
     * Starts the specified node
     *
     * @param clusterName - name of cluster
     * @param lxcHostName - hostname of node
     * @param master - specifies if this commands affects master or slave running on this node true - master, false -
     * regionserver
     *
     * @return - UUID of operation to track
     */
    UUID startNode( String clusterName, String lxcHostName, boolean master );

    /**
     * Stops the specified node
     *
     * @param clusterName - name of cluster
     * @param lxcHostName - hostname of node
     * @param master - specifies if this commands affects master or slave running on this node true - master, false -
     * regionserver
     *
     * @return - UUID of operation to track
     */
    UUID stopNode( String clusterName, String lxcHostName, boolean master );
}
