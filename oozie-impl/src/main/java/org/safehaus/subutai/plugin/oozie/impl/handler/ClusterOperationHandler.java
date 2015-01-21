package org.safehaus.subutai.plugin.oozie.impl.handler;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.environment.api.exception.EnvironmentBuildException;
import org.safehaus.subutai.core.environment.api.exception.EnvironmentDestroyException;
import org.safehaus.subutai.plugin.common.api.*;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.oozie.api.OozieClusterConfig;
import org.safehaus.subutai.plugin.oozie.api.SetupType;
import org.safehaus.subutai.plugin.oozie.impl.Commands;
import org.safehaus.subutai.plugin.oozie.impl.OozieImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Created by ermek on 1/12/15.
 */
public class ClusterOperationHandler extends AbstractOperationHandler<OozieImpl, OozieClusterConfig> implements
        ClusterOperationHandlerInterface
{
    private static final Logger LOG = LoggerFactory.getLogger(ClusterOperationHandler.class.getName());
    private ClusterOperationType operationType;
    private OozieClusterConfig config;
    private HadoopClusterConfig hadoopConfig;
    private ExecutorService executor = Executors.newCachedThreadPool();

    public ClusterOperationHandler(final OozieImpl manager, final OozieClusterConfig config, final ClusterOperationType
            operationType)
    {
        super(manager, config);
        this.operationType = operationType;
        this.config = config;
        trackerOperation = manager.getTracker().createTrackerOperation(OozieClusterConfig.PRODUCT_KEY,
                String.format("Creating %s tracker object...", clusterName));
    }

    @Override
    public void runOperationOnContainers(ClusterOperationType clusterOperationType)
    {

    }

    @Override
    public void setupCluster()
    {
        if ( Strings.isNullOrEmpty(config.getClusterName()) )
        {
            trackerOperation.addLogFailed( "Malformed configuration" );
            return;
        }

        if ( manager.getCluster( clusterName ) != null )
        {
            trackerOperation.addLogFailed( String.format( "Cluster with name '%s' already exists", clusterName ) );
            return;
        }

        try
        {
            Environment env = null;
            if ( config.getSetupType() != SetupType.OVER_HADOOP /*&& config.getSetupType() != SetupType.OVER_ENVIRONMENT */)
            {
                env = manager.getEnvironmentManager()
                        .buildEnvironment( manager.getDefaultEnvironmentBlueprint( config ) );
            }


            ClusterSetupStrategy clusterSetupStrategy =
                    manager.getClusterSetupStrategy( env, config, trackerOperation );
            clusterSetupStrategy.setup();

            trackerOperation.addLogDone( String.format( "Cluster %s set up successfully", clusterName ) );
        }
        catch ( EnvironmentBuildException | ClusterSetupException e )
        {
            trackerOperation.addLogFailed(
                    String.format( "Failed to setup %s cluster %s : %s", config.getProductKey(),
                            clusterName, e.getMessage() ) );
        }

    }

    @Override
    public void destroyCluster()
    {
        OozieClusterConfig config = manager.getCluster(clusterName);
        if (config == null)
        {
            trackerOperation.addLogFailed(
                    String.format("Cluster with name %s does not exist. Operation aborted", clusterName));
            return;
        }

        try
        {
            trackerOperation.addLog("Destroying environment...");
            manager.getEnvironmentManager().destroyEnvironment(config.getEnvironmentId());
            manager.getPluginDao().deleteInfo(OozieClusterConfig.PRODUCT_KEY, config.getClusterName());
            trackerOperation.addLogDone("Cluster destroyed");
        } catch (EnvironmentDestroyException e)
        {
            trackerOperation.addLogFailed(String.format("Error running command, %s", e.getMessage()));
            LOG.error(e.getMessage(), e);
        }
    }

    private void uninstallCluster()
    {
        TrackerOperation po = trackerOperation;
        po.addLog("Uninstalling Oozie client...");

        OozieClusterConfig config = manager.getCluster( clusterName );
        if ( config == null )
        {
            trackerOperation.addLogFailed(
                    String.format( "Cluster with name %s does not exist. Operation aborted", clusterName ) );
            return;
        }

        for (UUID uuid : config.getClients())
        {
            ContainerHost containerHost =
                    manager.getEnvironmentManager().getEnvironmentByUUID(config.getEnvironmentId())
                            .getContainerHostById(uuid);
            try
            {
                CommandResult result = containerHost.execute(Commands.getUninstallClientsCommand());
                if (!result.hasSucceeded())
                {
                    po.addLog(result.getStdErr());
                    po.addLogFailed("Uninstallation of oozie client failed");
                    return;
                }
            } catch (CommandException e)
            {
                LOG.error(e.getMessage(), e);
            }
        }

        po.addLog("Uninstalling Oozie server...");

        ContainerHost containerHost = manager.getEnvironmentManager().getEnvironmentByUUID(config.getEnvironmentId()).getContainerHostById(config.getServer());
        try
        {
            CommandResult result = containerHost.execute(Commands.getUninstallServerCommand());
            if (!result.hasSucceeded())
            {
                po.addLog(result.getStdErr());
                po .addLogFailed("Uninstallation of oozie server failed");
                return;
            }
        }
        catch (CommandException e)
        {
            LOG.error(e.getMessage(),e);
        }


        po.addLog("Updating db...");
        manager.getPluginDao().deleteInfo(OozieClusterConfig.PRODUCT_KEY, config.getClusterName());
        po.addLogDone("Cluster info deleted from DB\nDone");
    }


    @Override
    public void run()
    {
        Preconditions.checkNotNull(config, "Configuration is null !!!");
        switch (operationType)
        {
            case INSTALL:
                setupCluster();
                break;
            case UNINSTALL:
                uninstallCluster();
                break;
        }

    }
}
