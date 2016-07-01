/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.usergrid.api;


import java.util.List;
import java.util.UUID;

import io.subutai.common.environment.Environment;
import io.subutai.core.plugincommon.api.ApiBase;
import io.subutai.core.plugincommon.api.ClusterException;


public interface UsergridInterface extends ApiBase<UsergridConfig>
{
    List<String> getClusterList ( Environment name );


    public void saveConfig ( UsergridConfig ac ) throws ClusterException;


    public UsergridConfig getConfig ( String clusterName );


    UUID startCluster ( String clusterName );


    UUID stopCluster ( String clusterName );


    UUID restartCluster ( String clusterName );


    UUID statusCluster ( String clusterName );


    UUID startService ( String clusterName, String hostName );


    UUID stopService ( String clusterName, String hostName );


    UUID statusService ( String clusterName, String hostName );


    UUID addNode ( String clusterName );


    UUID oneClickInstall ( UsergridConfig usergridConfig );

}

