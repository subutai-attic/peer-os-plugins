package io.subutai.plugin.accumulo.impl;


import java.util.List;
import java.util.Set;
import java.util.UUID;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import io.subutai.common.environment.Environment;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.core.environment.api.EnvironmentEventListener;
import io.subutai.core.plugincommon.api.AbstractOperationHandler;
import io.subutai.core.plugincommon.api.ClusterException;
import io.subutai.core.plugincommon.api.ClusterOperationType;
import io.subutai.plugin.accumulo.api.Accumulo;
import io.subutai.plugin.accumulo.api.AccumuloClusterConfig;


public class AccumuloImpl implements Accumulo, EnvironmentEventListener
{
    @Override
    public UUID installCluster( final AccumuloClusterConfig accumuloClusterConfig )
    {
        return null;
    }


    @Override
    public UUID uninstallCluster( final String clustrName )
    {
        return null;
    }


    @Override
    public List<AccumuloClusterConfig> getClusters()
    {
        return null;
    }


    @Override
    public AccumuloClusterConfig getCluster( final String clustrName )
    {
        return null;
    }


    @Override
    public UUID addNode( final String clusterName, final String hostId )
    {
        return null;
    }


    @Override
    public UUID startNode( final String clusterName, final String hostId, final boolean isMaster )
    {
        return null;
    }


    @Override
    public UUID stopNode( final String clusterName, final String hostId, final boolean isMaster )
    {
        return null;
    }


    @Override
    public UUID checkNode( final String clusterName, final String hostId, final boolean isMaster )
    {
        return null;
    }


    @Override
    public UUID destroyNode( final String clusterName, final String hostId )
    {
        return null;
    }


    @Override
    public void saveConfig( final AccumuloClusterConfig config ) throws ClusterException
    {

    }


    @Override
    public void deleteConfig( final AccumuloClusterConfig config ) throws ClusterException
    {

    }


    @Override
    public void onEnvironmentCreated( final Environment environment )
    {

    }


    @Override
    public void onEnvironmentGrown( final Environment environment, final Set<EnvironmentContainerHost> set )
    {

    }


    @Override
    public void onContainerDestroyed( final Environment environment, final String s )
    {

    }


    @Override
    public void onEnvironmentDestroyed( final String s )
    {

    }
}
