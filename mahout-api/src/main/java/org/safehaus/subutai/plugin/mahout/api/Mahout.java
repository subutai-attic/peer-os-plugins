/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.safehaus.subutai.plugin.mahout.api;


import java.util.UUID;

import org.safehaus.subutai.common.protocol.EnvironmentBuildTask;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.plugin.common.api.ApiBase;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;


public interface Mahout extends ApiBase<MahoutClusterConfig>
{
    public UUID installCluster( MahoutClusterConfig config, HadoopClusterConfig hadoopConfig );

    public UUID uninstallCluster( MahoutClusterConfig config );

    public UUID uninstalllNode( String clusterName, String lxcHostname );

    public UUID addNode( String clusterName, String lxcHostname );

    public UUID checkNode( String clustername, String lxchostname );

    public UUID stopCluster( String clusterName );

    public UUID startCluster( String clusterName );

    public ClusterSetupStrategy getClusterSetupStrategy( Environment environment, MahoutClusterConfig config,
                                                  TrackerOperation po );
}
