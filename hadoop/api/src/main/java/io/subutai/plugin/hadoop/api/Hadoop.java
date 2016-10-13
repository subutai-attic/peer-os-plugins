package io.subutai.plugin.hadoop.api;


import java.util.UUID;


import io.subutai.common.environment.Blueprint;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.core.plugincommon.api.ApiBase;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.core.plugincommon.api.ClusterSetupException;
import io.subutai.webui.api.WebuiModule;


public interface Hadoop extends ApiBase<HadoopClusterConfig>
{

    /**
     * Uninstall cluster
     *
     * @param config hadoop cluster configuration object
     */
    public UUID uninstallCluster( HadoopClusterConfig config );


    /**
     * This just removes cluster configuration from DB, NOT destroys hadoop containers.
     *
     * @param clusterName cluster name
     *
     * @return uuid of operation
     */
    public UUID removeCluster( String clusterName );

    /**
     * Starts namenode along with data nodes and it sends "service hadoop-dfs start" command to namenode container.
     *
     * @param clusterName hadoop cluster
     * @param hostName hostname of namenode to start
     */
    public UUID startNameNode( String clusterName, String hostName );


    /**
     * Stops namenode along with data nodes and it sends "service hadoop-dfs stop" command to namenode container.
     *
     * @param clusterName hadoop cluster
     * @param hostName hostname of namenode to stop
     */
    public UUID stopNameNode( String clusterName, String hostName );


    /**
     * Checks namenode along with data nodes and it sends "service hadoop-dfs status" command to namenode container.
     *
     * @param clusterName hadoop cluster
     * @param hostName hostname of namenode to check status
     */
    public UUID statusNameNode( String clusterName, String hostName );


    /**
     * Starts data datanode and it sends "hadoop-daemon.sh start datanode" command to datanode container.
     *
     * @param hadoopClusterConfig hadoop cluster configuration object
     */
    public UUID startDataNode( HadoopClusterConfig hadoopClusterConfig, String hostname );


    /**
     * Stops data datanode and it sends "hadoop-daemon.sh stop datanode" command to datanode container.
     *
     * @param hadoopClusterConfig hadoop cluster configuration object
     */
    public UUID stopDataNode( HadoopClusterConfig hadoopClusterConfig, String hostname );


    /**
     * Checks data datanode and it sends "service hadoop-dfs status" command to datanode container.
     *
     * @param clusterName hadoop cluster
     * @param hostName hostname of datanode to check status
     */
    public UUID statusDataNode( String clusterName, String hostName );


    /**
     * Adds new node to cluster
     *
     * @param clusterName cluster name
     * @param nodeCount number of nodes to be added to cluster
     */
    public UUID addNode( String clusterName, int nodeCount );


    /**
     * Adds just one new node to cluster
     *
     * @param clusterName cluster name
     */
    public UUID addNode( String clusterName );


    /**
     * @param hadoopClusterConfig hadoop cluster configuration object
     * @param hostname container host name
     */
    public UUID destroyNode( HadoopClusterConfig hadoopClusterConfig, String hostname );


    /**
     * Excludes data node from cluster
     *
     * @param hadoopClusterConfig hadoop cluster configuration object
     * @param hostname container host name to be excluded
     */
    public UUID excludeNode( HadoopClusterConfig hadoopClusterConfig, String hostname );


    /**
     * Includes data node to cluster
     *
     * @param hadoopClusterConfig hadoop cluster configuration object
     * @param hostname container host name to be excluded
     */
    public UUID includeNode( HadoopClusterConfig hadoopClusterConfig, String hostname );

    public Blueprint getDefaultEnvironmentBlueprint( final HadoopClusterConfig config ) throws ClusterSetupException;


    /**
     * Saves/Updates cluster config in database
     *
     * @param config - config to update
     */
    public void saveConfig( HadoopClusterConfig config ) throws ClusterException;


    public WebuiModule getWebModule();

    public void setWebModule( final WebuiModule webModule );
}
