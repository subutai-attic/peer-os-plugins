package io.subutai.plugin.oozie.impl;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.host.HostInterface;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.common.util.CollectionUtil;
import io.subutai.core.plugincommon.api.ClusterConfigurationException;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.oozie.api.OozieClusterConfig;


public class ClusterConfiguration
{

    private OozieImpl manager;
    private TrackerOperation po;


    public ClusterConfiguration( final OozieImpl manager, final TrackerOperation po )
    {
        this.manager = manager;
        this.po = po;
    }


    public void configureCluster( final OozieClusterConfig config, Environment environment )
            throws ClusterConfigurationException
    {
        HadoopClusterConfig hadoopClusterConfig =
                manager.getHadoopManager().getCluster( config.getHadoopClusterName() );
        Set<String> nodeIds = new HashSet<>( hadoopClusterConfig.getAllNodes() );
        Set<EnvironmentContainerHost> containerHosts = null;
        try
        {
            containerHosts = environment.getContainerHostsByIds( nodeIds );
        }
        catch ( ContainerHostNotFoundException e )
        {
            e.printStackTrace();
        }
        if ( CollectionUtil.isCollectionEmpty( containerHosts ) )
        {
            throw new ClusterConfigurationException( "Node nodes found in environment" );
        }
        Iterator<EnvironmentContainerHost> iterator = containerHosts.iterator();

        List<CommandResult> commandsResultList = new ArrayList<>();
        List<CommandResult> commandsResultList2 = new ArrayList<>();

        while ( iterator.hasNext() )
        {
            EnvironmentContainerHost hadoopNode;
            try
            {
                hadoopNode = environment.getContainerHostById( iterator.next().getId() );
            }
            catch ( ContainerHostNotFoundException e )
            {
                throw new ClusterConfigurationException( e );
            }
			HostInterface hostInterface = hadoopNode.getInterfaceByName ("eth0");
            RequestBuilder requestBuilder =
                    Commands.getConfigureRootHostsCommand( hostInterface.getIp () );
            RequestBuilder requestBuilder2 = Commands.getConfigureRootGroupsCommand();
            CommandResult commandResult = null;
            CommandResult commandResult2 = null;
            try
            {
                commandResult = hadoopNode.execute( requestBuilder );
                commandResult2 = hadoopNode.execute( requestBuilder2 );
            }
            catch ( CommandException e )
            {
                po.addLogFailed( "Could not run command " + "configureRootHostsCommand" + ": " + e );
                e.printStackTrace();
            }
            commandsResultList.add( commandResult );
            commandsResultList2.add( commandResult2 );
        }

//        boolean isSuccesful = true;
//        for ( CommandResult aCommandsResultList : commandsResultList )
//        {
//            if ( !aCommandsResultList.hasSucceeded() )
//            {
//                isSuccesful = false;
//            }
//        }
//
//        boolean isSuccesful2 = true;
//        for ( CommandResult aCommandsResultList : commandsResultList2 )
//        {
//            if ( !aCommandsResultList.hasSucceeded() )
//            {
//                isSuccesful2 = false;
//            }
//        }
//
//        if ( isSuccesful && isSuccesful2 )
//        {
            po.addLog( "Cluster configured\n" );
//        }
//        else
//        {
//
//            throw new ClusterConfigurationException( String.format( "Cluster configuration failed" ) );
//        }
    }
}


