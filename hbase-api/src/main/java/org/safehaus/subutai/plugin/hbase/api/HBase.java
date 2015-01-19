/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.safehaus.subutai.plugin.hbase.api;


import java.util.UUID;

import org.safehaus.subutai.plugin.common.api.ApiBase;

public interface HBase extends ApiBase<HBaseConfig>
{

    public UUID installCluster( HBaseConfig config );

    public UUID destroyNode( final String clusterName, final String hostname );

    public UUID stopCluster( String clusterName );

    public UUID startCluster( String clusterName );

    public UUID checkNode( String clusterName, String hostname );

}
