package org.safehaus.subutai.plugin.oozie.impl;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.plugin.common.api.ClusterConfigurationException;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.oozie.api.OozieClusterConfig;

import java.util.*;

/**
 * Created by ermek on 1/18/15.
 */
public class ClusterConfiguration
{

    private OozieImpl manager;
    private TrackerOperation po;

    public ClusterConfiguration(final OozieImpl manager, final TrackerOperation po)
    {
        this.manager = manager;
        this.po = po;
    }


    public void configureCluster(final OozieClusterConfig config, Environment environment) throws
            ClusterConfigurationException
    {
        HadoopClusterConfig hadoopClusterConfig = manager.getHadoopManager().getCluster(config.getHadoopClusterName());
        Set<UUID> nodeUUIDs = new HashSet<>(hadoopClusterConfig.getAllNodes());
        Set<ContainerHost> containerHosts = environment.getContainerHostsByIds(nodeUUIDs);
        Iterator<ContainerHost> iterator = containerHosts.iterator();

        List<CommandResult> commandsResultList = new ArrayList<>();
        List<CommandResult> commandsResultList2 = new ArrayList<>();

        while (iterator.hasNext())
        {
            ContainerHost hadoopNode = environment.getContainerHostById(iterator.next().getId());
            RequestBuilder requestBuilder = Commands.getConfigureRootHostsCommand(hadoopNode.getIpByInterfaceName("eth0"));
            RequestBuilder requestBuilder2 = Commands.getConfigureRootGroupsCommand();
            CommandResult commandResult = null;
            CommandResult commandResult2 = null;
            try
            {
                commandResult = hadoopNode.execute(requestBuilder.withTimeout(60));
                commandResult2 = hadoopNode.execute(requestBuilder2.withTimeout(60));
            }
            catch (CommandException e)
            {
                po.addLogFailed("Could not run command " + "configureRootHostsCommand" + ": " + e);
                e.printStackTrace();
            }
            commandsResultList.add(commandResult);
            commandsResultList2.add(commandResult2);
        }

        boolean isSuccesful = true;
        for (CommandResult aCommandsResultList : commandsResultList)
        {
            if (!aCommandsResultList.hasSucceeded())
            {
                isSuccesful = false;
            }
        }

        boolean isSuccesful2 = true;
        for (CommandResult aCommandsResultList : commandsResultList2)
        {
            if (!aCommandsResultList.hasSucceeded())
            {
                isSuccesful2 = false;
            }
        }

        if (isSuccesful && isSuccesful2)
        {
            po.addLog("Cluster configured\n");
        }
        else
        {

            throw new ClusterConfigurationException(String.format("Cluster configuration failed"));
        }
    }
}


