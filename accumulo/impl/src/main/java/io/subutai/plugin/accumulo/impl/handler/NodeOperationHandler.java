package io.subutai.plugin.accumulo.impl.handler;


import io.subutai.core.plugincommon.api.AbstractOperationHandler;
import io.subutai.plugin.accumulo.api.AccumuloClusterConfig;
import io.subutai.plugin.accumulo.impl.AccumuloImpl;


public class NodeOperationHandler extends AbstractOperationHandler<AccumuloImpl, AccumuloClusterConfig>
{
    public NodeOperationHandler( final AccumuloImpl manager, final AccumuloClusterConfig config )
    {
        super( manager, config );
    }


    @Override
    public void run()
    {

    }
}
