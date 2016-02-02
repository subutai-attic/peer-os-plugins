/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.solr.api;


import java.util.UUID;

import io.subutai.common.environment.Blueprint;
import io.subutai.common.environment.Environment;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.plugin.common.api.ApiBase;
import io.subutai.plugin.common.api.ClusterSetupStrategy;


/**
 * @author dilshat
 */
public interface Solr extends ApiBase<SolrClusterConfig>
{

    public UUID startNode( String clusterName, String lxcHostname );

    public UUID stopNode( String clusterName, String lxcHostname );

    public UUID checkNode( String clusterName, String lxcHostname );

    public UUID addNode( String clusterName, String lxcHostname );

    public UUID destroyNode( String clusterName, String lxcHostname );

    public UUID uninstallCluster( SolrClusterConfig config );

    public ClusterSetupStrategy getClusterSetupStrategy( final Environment environment, final SolrClusterConfig config,
                                                         final TrackerOperation po );

    public Blueprint getDefaultEnvironmentBlueprint( SolrClusterConfig config );

    UUID configureEnvironmentCluster( SolrClusterConfig config );
}
