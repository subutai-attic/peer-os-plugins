package io.subutai.plugin.accumulo.impl.handler;


import io.subutai.core.plugincommon.api.AbstractOperationHandler;
import io.subutai.core.plugincommon.api.ClusterOperationHandlerInterface;
import io.subutai.core.plugincommon.api.ClusterOperationType;
import io.subutai.plugin.accumulo.api.AccumuloClusterConfig;
import io.subutai.plugin.accumulo.impl.AccumuloImpl;


public class ClusterOperationHandler extends AbstractOperationHandler<AccumuloImpl, AccumuloClusterConfig>
        implements ClusterOperationHandlerInterface
{
    public ClusterOperationHandler( final AccumuloImpl manager, final AccumuloClusterConfig config )
    {
        super( manager, config );
    }


    @Override
    public void runOperationOnContainers( final ClusterOperationType clusterOperationType )
    {

    }


    @Override
    public void setupCluster()
    {

    }


    @Override
    public void destroyCluster()
    {

    }


    @Override
    public void run()
    {

    }
}
