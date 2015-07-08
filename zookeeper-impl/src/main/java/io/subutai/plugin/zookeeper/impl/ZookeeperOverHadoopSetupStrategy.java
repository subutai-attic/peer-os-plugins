package io.subutai.plugin.zookeeper.impl;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.CommandUtil;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.peer.Host;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.metric.api.MonitorException;
import io.subutai.plugin.common.api.ClusterConfigurationException;
import io.subutai.plugin.common.api.ClusterSetupException;
import io.subutai.plugin.common.api.ClusterSetupStrategy;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.zookeeper.api.SetupType;
import io.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;


/**
 * ZK cluster setup strategy over an existing Hadoop cluster
 */
public class ZookeeperOverHadoopSetupStrategy implements ClusterSetupStrategy
{

    private final ZookeeperClusterConfig zookeeperClusterConfig;
    private final TrackerOperation po;
    private final ZookeeperImpl manager;
    private Environment environment;
    private CommandUtil commandUtil;


    public ZookeeperOverHadoopSetupStrategy( final Environment environment,
                                             final ZookeeperClusterConfig zookeeperClusterConfig,
                                             final TrackerOperation po, final ZookeeperImpl zookeeperManager )
    {
        Preconditions.checkNotNull( zookeeperClusterConfig, "Cluster config is null" );
        Preconditions.checkNotNull( po, "Product operation tracker is null" );
        Preconditions.checkNotNull( zookeeperManager, "ZK manager is null" );

        this.zookeeperClusterConfig = zookeeperClusterConfig;
        this.po = po;
        this.manager = zookeeperManager;
        this.environment = environment;
        this.commandUtil = new CommandUtil();
    }


    @Override
    public ZookeeperClusterConfig setup() throws ClusterSetupException
    {
        if ( Strings.isNullOrEmpty( zookeeperClusterConfig.getClusterName() ) ||
                zookeeperClusterConfig.getNodes() == null || zookeeperClusterConfig.getNodes().isEmpty() )
        {
            throw new ClusterSetupException( "Malformed configuration" );
        }

        if ( manager.getCluster( zookeeperClusterConfig.getClusterName() ) != null )
        {
            throw new ClusterSetupException(
                    String.format( "Cluster with name '%s' already exists", zookeeperClusterConfig.getClusterName() ) );
        }

        if ( zookeeperClusterConfig.getSetupType() == SetupType.OVER_HADOOP )
        {
            UUID envId = manager.getHadoopManager().getCluster( zookeeperClusterConfig.getHadoopClusterName() )
                                .getEnvironmentId();
            try
            {
                environment = manager.getEnvironmentManager().findEnvironment( envId );
            }
            catch ( EnvironmentNotFoundException e )
            {
                throw new ClusterSetupException(
                        String.format( "Couldn't get environment with id: %s", envId.toString() ) );
            }
        }
        Set<ContainerHost> zookeeperNodes;
        try
        {
            zookeeperNodes = environment.getContainerHostsByIds( zookeeperClusterConfig.getNodes() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            throw new ClusterSetupException(
                    String.format( "Nodes with ids not found: %s", zookeeperClusterConfig.getNodes().toString() ) );
        }
        //check if node agent is connected
        for ( ContainerHost node : zookeeperNodes )
        {
            try
            {
                if ( environment.getContainerHostByHostname( node.getHostname() ) == null )
                {
                    throw new ClusterSetupException( String.format( "Node %s is not connected", node.getHostname() ) );
                }
            }
            catch ( ContainerHostNotFoundException e )
            {
                throw new ClusterSetupException(
                        String.format( "Couldn't find container host with name: %s", node.getHostname() ) );
            }
        }

        HadoopClusterConfig hadoopClusterConfig =
                manager.getHadoopManager().getCluster( zookeeperClusterConfig.getHadoopClusterName() );
        if ( hadoopClusterConfig == null )
        {
            throw new ClusterSetupException(
                    String.format( "Hadoop cluster %s not found", zookeeperClusterConfig.getHadoopClusterName() ) );
        }

        if ( !hadoopClusterConfig.getAllNodes().containsAll( zookeeperClusterConfig.getNodes() ) )
        {
            throw new ClusterSetupException( String.format( "Not all specified ZK nodes belong to %s Hadoop cluster",
                    hadoopClusterConfig.getClusterName() ) );
        }

        po.addLog( "Checking prerequisites..." );


        //check installed subutai packages
        String checkInstalledCommand = Commands.getZooNHadoopStatusCommand();
        List<CommandResult> commandResultList = runCommandOnContainers( checkInstalledCommand, zookeeperNodes );
        if ( getFailedCommandResults( commandResultList ).size() != 0 )
        {
            throw new ClusterSetupException( "Failed to check presence of installed subutai packages" );
        }

        Iterator<ContainerHost> iterator = zookeeperNodes.iterator();
        int nodeIndex = 0;
        while ( iterator.hasNext() )
        {
            ContainerHost host = iterator.next();
            CommandResult result = commandResultList.get( nodeIndex++ );
            //Here we check zookeeper service is not configured as a service
            //this is the only way to correctly differentiate if zoo plugin is
            //not installed and not configured, because in case with hadoop this piece
            //doesn't work
            String commandOutput = result.getStdOut() + "\n" + result.getStdErr();
            if ( commandOutput.contains( "Zookeeper Server is running" ) )
            {
                throw new ClusterSetupException(
                        String.format( "Node %s already has Zookeeper installed", host.getHostname() ) );
            }
            else if ( commandOutput.contains( "hadoop-all: unrecognized service" ) )
            {
                throw new ClusterSetupException(
                        String.format( "Node %s has no Hadoop installed", host.getHostname() ) );
            }
        }

        po.addLog( String.format( "Installing Zookeeper..." ) );

        //install
        Set<Host> hostSet = getHosts( zookeeperClusterConfig.getNodes(), environment );
        try
        {
            RequestBuilder installRequest = new RequestBuilder( Commands.getInstallCommand() );
            installRequest.withTimeout( 360 );
            Map<Host, CommandResult> resultMap =
                    commandUtil.executeParallel( installRequest, hostSet );
            if ( isAllSuccessful( resultMap, hostSet ) ){
                po.addLog( "Zookeeper is installed on all nodes successfully" );
                try
                {
                    new ClusterConfiguration( manager, po ).configureCluster( zookeeperClusterConfig, environment );

                    po.addLog( "Saving cluster information to database..." );

                    zookeeperClusterConfig.setEnvironmentId( environment.getId() );

                    manager.getPluginDAO()
                           .saveInfo( ZookeeperClusterConfig.PRODUCT_KEY, zookeeperClusterConfig.getClusterName(),
                                   zookeeperClusterConfig );
                    po.addLog( "Cluster information saved to database" );

                    manager.subscribeToAlerts( environment );
                }
                catch ( ClusterConfigurationException | MonitorException e )
                {
                    e.printStackTrace();
                }
            }
            else {
                po.addLogFailed( "Zookeeper is NOT installed on all nodes successfully !!!");
            }

        }
        catch ( CommandException e )
        {
            e.printStackTrace();
        }
        return zookeeperClusterConfig;
    }


    public static Set<Host> getHosts( Set<UUID> uuids, Environment environment ){
        Set<Host> hostSet = new HashSet<>();
        for ( UUID uuid : uuids ){
            try
            {
                hostSet.add( environment.getContainerHostById( uuid ) );
            }
            catch ( ContainerHostNotFoundException e )
            {
                e.printStackTrace();
            }
        }
        return hostSet;
    }


    public static boolean isAllSuccessful( Map<Host, CommandResult> resultMap, Set<Host> hosts )
    {
        boolean allSuccess = true;
        for ( Host host : hosts )
        {
            if ( ! resultMap.get( host ).hasSucceeded() )
            {
                allSuccess = false;
            }
        }
        return allSuccess;
    }


    private List<CommandResult> runCommandOnContainers( String command, final Set<ContainerHost> zookeeperNodes )
    {
        List<CommandResult> commandResults = new ArrayList<>();
        for ( ContainerHost containerHost : zookeeperNodes )
        {
            try
            {
                commandResults.add( containerHost.execute( new RequestBuilder( command ).withTimeout( 1800 ) ) );
            }
            catch ( CommandException e )
            {
                e.printStackTrace();
            }
        }
        return commandResults;
    }


    public List<CommandResult> getFailedCommandResults( final List<CommandResult> commandResultList )
    {
        List<CommandResult> failedCommands = new ArrayList<>();
        for ( CommandResult commandResult : commandResultList )
        {
            if ( !commandResult.hasSucceeded() )
            {
                failedCommands.add( commandResult );
            }
        }
        return failedCommands;
    }
}
