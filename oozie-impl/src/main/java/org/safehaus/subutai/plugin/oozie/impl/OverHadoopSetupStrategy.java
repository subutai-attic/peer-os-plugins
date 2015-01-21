package org.safehaus.subutai.plugin.oozie.impl;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.plugin.common.api.ClusterSetupException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.oozie.api.OozieClusterConfig;
import org.safehaus.subutai.plugin.oozie.api.SetupType;

import java.util.*;

public class OverHadoopSetupStrategy implements ClusterSetupStrategy
{

    private final OozieClusterConfig oozieClusterConfig;
    private final TrackerOperation po;
    private final OozieImpl manager;
    private Environment environment;


    public OverHadoopSetupStrategy(final Environment environment,
                                   final OozieClusterConfig oozieClusterConfig,
                                   final TrackerOperation po, final OozieImpl oozieManager)
    {
        Preconditions.checkNotNull(oozieClusterConfig, "Cluster config is null");
        Preconditions.checkNotNull(po, "Product operation tracker is null");
        Preconditions.checkNotNull(oozieManager, "ZK manager is null");

        this.oozieClusterConfig = oozieClusterConfig;
        this.po = po;
        this.manager = oozieManager;
        this.environment = environment;
    }


    @Override
    public OozieClusterConfig setup() throws ClusterSetupException
    {
        // CHECKING for oozie - clients
// =====================================================================================================================
        if (Strings.isNullOrEmpty(oozieClusterConfig.getClusterName()) ||
                oozieClusterConfig.getClients() == null || oozieClusterConfig.getClients().isEmpty())
        {
            throw new ClusterSetupException("Malformed configuration");
        }

        if (manager.getCluster(oozieClusterConfig.getClusterName()) != null)
        {
            throw new ClusterSetupException(
                    String.format("Cluster with name '%s' already exists", oozieClusterConfig.getClusterName()));
        }

        if (oozieClusterConfig.getSetupType() == SetupType.OVER_HADOOP)
        {
            environment = manager.getEnvironmentManager().getEnvironmentByUUID(
                    manager.getHadoopManager().getCluster(oozieClusterConfig
                            .getHadoopClusterName()).getEnvironmentId());
        }
        Set<ContainerHost> oozieClientNodes = environment.getContainerHostsByIds(oozieClusterConfig.getClients());
        //check if node agent is connected
        for (ContainerHost node : oozieClientNodes)
        {
            if (environment.getContainerHostByHostname(node.getHostname()) == null)
            {
                throw new ClusterSetupException(String.format("Node %s is not connected", node.getHostname()));
            }
        }

        HadoopClusterConfig hadoopClusterConfig =
                manager.getHadoopManager().getCluster(oozieClusterConfig.getHadoopClusterName());
        if (hadoopClusterConfig == null)
        {
            throw new ClusterSetupException(
                    String.format("Hadoop cluster %s not found", oozieClusterConfig.getHadoopClusterName()));
        }

        if (!hadoopClusterConfig.getAllNodes().containsAll(oozieClusterConfig.getClients()))
        {
            throw new ClusterSetupException(String.format("Not all specified OozieClient nodes belong to %s Hadoop " +
                            "cluster",
                    hadoopClusterConfig.getClusterName()));
        }


        po.addLog("Checking prerequisites...");


        //check installed subutai packages
        List<CommandResult> commandResultList = runCommandOnContainers(Commands.make(CommandType.STATUS),
                oozieClientNodes);
        if (getFailedCommandResults(commandResultList).size() != 0)
        {
            throw new ClusterSetupException("Failed to check presence of installed subutai packages");
        }

        Iterator<ContainerHost> iterator = oozieClientNodes.iterator();
        int nodeIndex = 0;
        while (iterator.hasNext())
        {
            ContainerHost host = iterator.next();
            CommandResult result = commandResultList.get(nodeIndex++);

            if (result.getStdOut().contains(Common.PACKAGE_PREFIX + OozieClusterConfig.PRODUCT_NAME_CLIENT))
            {
                throw new ClusterSetupException(
                        String.format("Node %s already has OozieClient installed", host.getHostname()));
            } else if (!result.getStdOut().contains(Common.PACKAGE_PREFIX + HadoopClusterConfig.PRODUCT_NAME))
            {
                throw new ClusterSetupException(
                        String.format("Node %s has no Hadoop installed", host.getHostname()));
            }
        }
//======================================================================================================================

        // CHECKING for oozie - server
//======================================================================================================================
        if (Strings.isNullOrEmpty(oozieClusterConfig.getClusterName()) ||
                oozieClusterConfig.getServer() == null || oozieClusterConfig.getServer() == null)
        {
            throw new ClusterSetupException("Malformed configuration");
        }

        if (manager.getCluster(oozieClusterConfig.getClusterName()) != null)
        {
            throw new ClusterSetupException(
                    String.format("Cluster with name '%s' already exists", oozieClusterConfig.getClusterName()));
        }

        if (oozieClusterConfig.getSetupType() == SetupType.OVER_HADOOP)
        {
            environment = manager.getEnvironmentManager().getEnvironmentByUUID(
                    manager.getHadoopManager().getCluster(oozieClusterConfig
                            .getHadoopClusterName()).getEnvironmentId());
        }
        ContainerHost oozieServerNode = environment.getContainerHostById(oozieClusterConfig.getServer());
        Set<ContainerHost> oozieServerNodes = new HashSet<>();
        oozieClientNodes.add(oozieServerNode);


        //check if node agent is connected
        for (ContainerHost node : oozieClientNodes)
        {
            if (environment.getContainerHostByHostname(node.getHostname()) == null)
            {
                throw new ClusterSetupException(String.format("Node %s is not connected", node.getHostname()));
            }
        }


        if (!hadoopClusterConfig.getAllNodes().contains(oozieClusterConfig.getServer()))
        {
            throw new ClusterSetupException(String.format("Not all specified OozieServer nodes belong to %s Hadoop " +
                            "cluster",
                    hadoopClusterConfig.getClusterName()));
        }



        //check installed subutai packages
        List<CommandResult> commandResultList2 = runCommandOnContainers(Commands.make(CommandType.STATUS),
                oozieClientNodes);
        if (getFailedCommandResults(commandResultList2).size() != 0)
        {
            throw new ClusterSetupException("Failed to check presence of installed subutai packages");
        }

        Iterator<ContainerHost> iterator2 = oozieServerNodes.iterator();
        int nodeIndex2 = 0;
        while (iterator2.hasNext())
        {
            ContainerHost host = iterator2.next();
            CommandResult result = commandResultList2.get(nodeIndex2++);

            if (result.getStdOut().contains(Common.PACKAGE_PREFIX + OozieClusterConfig.PRODUCT_NAME_SERVER))
            {
                throw new ClusterSetupException(
                        String.format("Node %s already has OozieServer installed", host.getHostname()));
            } else if (!result.getStdOut().contains(Common.PACKAGE_PREFIX + HadoopClusterConfig.PRODUCT_NAME))
            {
                throw new ClusterSetupException(
                        String.format("Node %s has no Hadoop installed", host.getHostname()));
            }
        }
//======================================================================================================================


        po.addLog(String.format("Installing Oozie Server and Oozie Client..."));

        //install
        commandResultList2 = runCommandOnContainers(Commands.make(CommandType.INSTALL_SERVER), oozieServerNodes);
        commandResultList = runCommandOnContainers(Commands.make(CommandType.INSTALL_CLIENT), oozieClientNodes);

        if ((getFailedCommandResults(commandResultList2).size() == 0) && (getFailedCommandResults(commandResultList)
                .size() == 0))
        {
            po.addLog("Installation succeeded\nConfiguring cluster...");


            try
            {
                new ClusterConfiguration(manager, po).configureCluster(oozieClusterConfig, environment);
            } catch (ClusterConfigurationException e)
            {
                throw new ClusterSetupException(e.getMessage());
            }

            po.addLog("Saving cluster information to database...");


            oozieClusterConfig.setEnvironmentId(environment.getId());

            manager.getPluginDao()
                    .saveInfo(OozieClusterConfig.PRODUCT_KEY, oozieClusterConfig.getClusterName(),
                            oozieClusterConfig);
            po.addLog("Cluster information saved to database");
        } else
        {
            StringBuilder stringBuilder = new StringBuilder();
            for (CommandResult commandResult : getFailedCommandResults(commandResultList2))
            {
                stringBuilder.append(commandResult.getStdErr());
            }

            throw new ClusterSetupException(
                    String.format("Installation failed, %s", stringBuilder));
        }

        return oozieClusterConfig;
    }


    private List<CommandResult> runCommandOnContainers(String command, final Set<ContainerHost> oozieNodes)
    {
        List<CommandResult> commandResults = new ArrayList<>();
        for (ContainerHost containerHost : oozieNodes)
        {
            try
            {
                commandResults.add(containerHost.execute(new RequestBuilder(command).withTimeout(1800)));
            } catch (CommandException e)
            {
                e.printStackTrace();
            }
        }
        return commandResults;
    }


    public List<CommandResult> getFailedCommandResults(final List<CommandResult> commandResultList)
    {
        List<CommandResult> failedCommands = new ArrayList<>();
        for (CommandResult commandResult : commandResultList)
        {
            if (!commandResult.hasSucceeded())
                failedCommands.add(commandResult);
        }
        return failedCommands;
    }
}
