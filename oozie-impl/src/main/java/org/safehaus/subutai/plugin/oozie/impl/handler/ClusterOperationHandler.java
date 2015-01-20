package org.safehaus.subutai.plugin.oozie.impl.handler;

import com.google.common.base.Preconditions;
import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.environment.api.exception.EnvironmentDestroyException;
import org.safehaus.subutai.plugin.common.api.*;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.oozie.api.OozieClusterConfig;
import org.safehaus.subutai.plugin.oozie.impl.CommandType;
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
        try
        {
            ClusterSetupStrategy s = manager.getClusterSetupStrategy(trackerOperation, config);
            try
            {
                trackerOperation.addLog( "Installing cluster..." );
                s.setup();
                trackerOperation.addLogDone( "Installing cluster completed" );
            }
            catch (ClusterSetupException ex)
            {
                throw new ClusterException("Failed to setup cluster: " + ex.getMessage());
            }
        }
        catch (ClusterException e)
        {
            trackerOperation.addLogFailed(String.format("Could not start all nodes : %s", e.getMessage()));
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
        po.addLog("Uninstalling Flume...");

        for (UUID uuid : config.getNodes())
        {
            ContainerHost containerHost =
                    manager.getEnvironmentManager().getEnvironmentByUUID(config.getEnvironmentId())
                            .getContainerHostById(uuid);
            try
            {
                CommandResult result = containerHost.execute(new RequestBuilder(Commands.make(CommandType.PURGE)));
                if (!result.hasSucceeded())
                {
                    po.addLog(result.getStdErr());
                    po.addLogFailed("Uninstallation failed");
                    return;
                }
            } catch (CommandException e)
            {
                LOG.error(e.getMessage(), e);
            }
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
            case DESTROY:
                uninstallCluster();
                break;
        }

    }
}
