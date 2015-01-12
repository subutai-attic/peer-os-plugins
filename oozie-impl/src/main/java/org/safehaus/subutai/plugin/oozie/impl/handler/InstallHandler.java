package org.safehaus.subutai.plugin.oozie.impl.handler;


import org.safehaus.subutai.common.protocol.EnvironmentBlueprint;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.environment.api.exception.EnvironmentBuildException;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.plugin.common.api.AbstractOperationHandler;
import org.safehaus.subutai.plugin.common.api.ClusterException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.oozie.api.OozieClusterConfig;
import org.safehaus.subutai.plugin.oozie.api.SetupType;
import org.safehaus.subutai.plugin.oozie.impl.OozieImpl;

import java.util.UUID;


public class InstallHandler extends AbstractOperationHandler<OozieImpl, OozieClusterConfig>
{

    private final TrackerOperation trackerOperation;
    private OozieClusterConfig config;
    private HadoopClusterConfig hadoopConfig;


    public InstallHandler( final OozieImpl manager, final OozieClusterConfig config )
    {
        super( manager, config.getClusterName() );
        this.config = config;
        trackerOperation = manager.getTracker().createTrackerOperation( OozieClusterConfig.PRODUCT_KEY,
                String.format( "Setting up %s cluster...", config.getClusterName() ) );
    }


    @Override
    public UUID getTrackerId()
    {
        return trackerOperation.getId();
    }

    public void setHadoopConfig( HadoopClusterConfig hadoopConfig )
    {
        this.hadoopConfig = hadoopConfig;
    }


    @Override
    public void run()
    {
        try
        {
            Environment env = null;

        if ( config.getSetupType() == SetupType.WITH_HADOOP )
        {

            if ( hadoopConfig == null )
            {
                trackerOperation.addLogFailed( "No Hadoop configuration specified" );
                return;
            }

            trackerOperation.addLog( "Preparing environment..." );
            hadoopConfig.setTemplateName( OozieClusterConfig.PRODUCT_NAME_SERVER );
            try
            {
                EnvironmentBlueprint eb = manager.getHadoopManager().getDefaultEnvironmentBlueprint( hadoopConfig );
                env = manager.getEnvironmentManager().buildEnvironment( eb );
            }
            catch ( ClusterSetupException ex )
            {
                trackerOperation.addLogFailed( "Failed to prepare environment: " + ex.getMessage() );
                return;
            }
            catch ( EnvironmentBuildException ex )
            {
                trackerOperation.addLogFailed( "Failed to build environment: " + ex.getMessage() );
                return;
            }
            trackerOperation.addLog( "Environment preparation completed" );
        }
        else
        {
            env = manager.getEnvironmentManager().getEnvironmentByUUID( hadoopConfig.getEnvironmentId() );
            if ( env == null )
            {
                throw new ClusterException( String.format( "Could not find environment of Hadoop cluster by id %s",
                        hadoopConfig.getEnvironmentId() ) );
            }
        }


        ClusterSetupStrategy s = manager.getClusterSetupStrategy( trackerOperation, config, env );
        try
        {
            if ( s == null )
            {
                throw new ClusterSetupException( "No setup strategy" );
            }

            s.setup();
            trackerOperation.addLogDone( "Done" );
        }
        catch ( ClusterSetupException ex )
        {
            trackerOperation.addLogFailed( "Failed to setup cluster: " + ex.getMessage() );
        }
        }
        catch ( ClusterException e )
        {
            trackerOperation.addLogFailed( String.format( "Could not start all nodes : %s", e.getMessage() ) );
        }

    }
}
