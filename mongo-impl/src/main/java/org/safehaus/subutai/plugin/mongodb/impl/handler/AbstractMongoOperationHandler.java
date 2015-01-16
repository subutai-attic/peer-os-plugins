package org.safehaus.subutai.plugin.mongodb.impl.handler;


import java.util.List;

import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.tracker.OperationState;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.plugin.common.api.ApiBase;
import org.safehaus.subutai.plugin.common.api.ConfigBase;

import com.google.common.base.Preconditions;


/**
 * Created by talas on 1/16/15.
 */
public abstract class AbstractMongoOperationHandler<T extends ApiBase, V extends ConfigBase>
        extends org.safehaus.subutai.plugin.common.api.AbstractOperationHandler<T, V>
{
    public AbstractMongoOperationHandler( final T manager, final V config )
    {
        super( manager, config );
    }


    /**
     * @deprecated
     */
    public AbstractMongoOperationHandler( final T manager, final String clusterName )
    {
        super( manager, clusterName );
    }


    public void logResults( TrackerOperation po, List<CommandResult> commandResultList )
    {
        Preconditions.checkNotNull( commandResultList );
        for ( CommandResult commandResult : commandResultList )
        {
            po.addLog( commandResult.getStdOut() );
        }
        if ( po.getState() == OperationState.FAILED )
        {
            po.addLogFailed( "" );
        }
        else
        {
            po.addLogDone( "" );
        }
    }
}
