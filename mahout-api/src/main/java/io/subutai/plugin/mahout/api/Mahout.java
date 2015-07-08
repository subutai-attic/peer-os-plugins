/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.mahout.api;


import java.util.UUID;

import io.subutai.common.tracker.TrackerOperation;
import io.subutai.plugin.common.api.ApiBase;
import io.subutai.plugin.common.api.ClusterException;
import io.subutai.plugin.common.api.ClusterSetupStrategy;


public interface Mahout extends ApiBase<MahoutClusterConfig>
{

    public UUID uninstallNode( String clusterName, String lxcHostname );

    public UUID addNode( String clusterName, String lxcHostname );

    public UUID checkNode( String clustername, String lxchostname );

    public ClusterSetupStrategy getClusterSetupStrategy( MahoutClusterConfig config, TrackerOperation po );

    public void saveConfig( final MahoutClusterConfig config ) throws ClusterException;

    public void deleteConfig( final MahoutClusterConfig config ) throws ClusterException;
}
