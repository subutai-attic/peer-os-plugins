package org.safehaus.subutai.plugin.hadoop.api;


import java.util.UUID;

import org.safehaus.subutai.common.environment.Blueprint;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.plugin.common.api.ApiBase;
import org.safehaus.subutai.plugin.common.api.ClusterException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;


public interface Hadoop extends ApiBase<HadoopClusterConfig>
{

    /**
     * Uninstall cluster
     * @param config hadoop cluster configuration object
     * @return
     */
    public UUID uninstallCluster( HadoopClusterConfig config );


    /**
     * This just removes cluster configuration from DB,
     * NOT destroys hadoop containers.
     * @param clusterName cluster name
     * @return uuid of operation
     */
    public UUID removeCluster( String clusterName );

    /**
     * Starts namenode along with data nodes and it sends
     * "service hadoop-dfs start" command to namenode container.
     *
     * @param hadoopClusterConfig hadoop cluster configuration object
     * @return
     */
    public UUID startNameNode( HadoopClusterConfig hadoopClusterConfig );


    /**
     * Stops namenode along with data nodes and it sends
     * "service hadoop-dfs stop" command to namenode container.
     *
     * @param hadoopClusterConfig hadoop cluster configuration object
     * @return
     */
    public UUID stopNameNode( HadoopClusterConfig hadoopClusterConfig );


    /**
     * Checks namenode along with data nodes and it sends
     * "service hadoop-dfs status" command to namenode container.
     *
     * @param hadoopClusterConfig hadoop cluster configuration object
     * @return
     */
    public UUID statusNameNode( HadoopClusterConfig hadoopClusterConfig );


    /**
     * Checks secondary namenode machine and it sends
     * "service hadoop-dfs status" command to secondary namenode container.
     *
     * @param hadoopClusterConfig hadoop cluster configuration object
     * @return
     */
    public UUID statusSecondaryNameNode( HadoopClusterConfig hadoopClusterConfig );


    /**
     * Starts data datanode and it sends
     * "hadoop-daemon.sh start datanode" command to datanode container.
     *
     * @param hadoopClusterConfig hadoop cluster configuration object
     * @return
     */
    public UUID startDataNode( HadoopClusterConfig hadoopClusterConfig, String hostname );


    /**
     * Stops data datanode and it sends
     * "hadoop-daemon.sh stop datanode" command to datanode container.
     *
     * @param hadoopClusterConfig hadoop cluster configuration object
     * @return
     */
    public UUID stopDataNode( HadoopClusterConfig hadoopClusterConfig, String hostname );


    /**
     * Checks data datanode and it sends
     * "service hadoop-dfs status" command to datanode container.
     *
     * @param hadoopClusterConfig hadoop cluster configuration object
     * @return
     */
    public UUID statusDataNode( HadoopClusterConfig hadoopClusterConfig, String hostname );


    /**
     * Stars jobtracker along with task trackers and it sends
     * "service hadoop-mapred start" command to jobtracker container.
     *
     * @param hadoopClusterConfig hadoop cluster configuration object
     * @return
     */
    public UUID startJobTracker( HadoopClusterConfig hadoopClusterConfig );


    /**
     * Stops jobtracker along with task trackers and it sends
     * "service hadoop-mapred stop" command to jobtracker container.
     *
     * @param hadoopClusterConfig hadoop cluster configuration object
     * @return
     */
    public UUID stopJobTracker( HadoopClusterConfig hadoopClusterConfig );


    /**
     * Checks jobtracker along with task trackers and it sends
     * "service hadoop-mapred status" command to jobtracker container.
     *
     * @param hadoopClusterConfig hadoop cluster configuration object
     * @return
     */
    public UUID statusJobTracker( HadoopClusterConfig hadoopClusterConfig );


    /**
     * Starts task tracker and it sends
     * "hadoop-daemon.sh start tasktracker" command to tasktracker container.
     *
     * @param hadoopClusterConfig hadoop cluster configuration object
     * @return
     */
    public UUID startTaskTracker( HadoopClusterConfig hadoopClusterConfig, String hostname );


    /**
     * Stops task tracker and it sends
     * "hadoop-daemon.sh stop tasktracker" command to tasktracker container.
     *
     * @param hadoopClusterConfig hadoop cluster configuration object
     * @return
     */
    public UUID stopTaskTracker( HadoopClusterConfig hadoopClusterConfig, String hostname );


    /**
     * Checks task tracker and it sends
     * "service hadoop-mapred status" command to tasktracker container.
     *
     * @param hadoopClusterConfig hadoop cluster configuration object
     * @return
     */
    public UUID statusTaskTracker( HadoopClusterConfig hadoopClusterConfig, String hostname );


    /**
     * Adds new node to cluster
     *
     * @param clusterName cluster name
     * @param nodeCount number of nodes to be added to cluster
     * @return
     */
    public UUID addNode( String clusterName, int nodeCount );


    /**
     * Adds just one new node to cluster
     * @param clusterName cluster name
     * @return
     */
    public UUID addNode( String clusterName );


    /**
     *
     * @param hadoopClusterConfig hadoop cluster configuration object
     * @param hostname container host name
     * @return
     */
    public UUID destroyNode( HadoopClusterConfig hadoopClusterConfig, String hostname );

    /**
     * Checks decommission status of data nodes
     *
     * @param hadoopClusterConfig hadoop cluster configuration object
     * @return
     */
    public UUID checkDecomissionStatus( HadoopClusterConfig hadoopClusterConfig );


    /**
     * Excludes data node from cluster
     *
     * @param hadoopClusterConfig hadoop cluster configuration object
     * @param hostname container host name to be excluded
     *
     * @return
     */
    public UUID excludeNode( HadoopClusterConfig hadoopClusterConfig, String hostname );


    /**
     * Includes data node to cluster
     *
     * @param hadoopClusterConfig hadoop cluster configuration object
     * @param hostname container host name to be excluded
     * @return
     */
    public UUID includeNode( HadoopClusterConfig hadoopClusterConfig, String hostname );

    public Blueprint getDefaultEnvironmentBlueprint( final HadoopClusterConfig config ) throws ClusterSetupException;


    /**
     * Saves/Updates cluster config in database
     *
     * @param config - config to update
     */
    public void saveConfig( HadoopClusterConfig config ) throws ClusterException;

}
