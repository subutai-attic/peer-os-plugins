package org.safehaus.subutai.plugin.oozie.impl;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.common.settings.Common;
import org.safehaus.subutai.common.tracker.TrackerOperation;
import org.safehaus.subutai.plugin.common.api.ClusterConfigurationException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupException;
import org.safehaus.subutai.plugin.common.api.ClusterSetupStrategy;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.oozie.api.OozieClusterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;


public class OverHadoopSetupStrategy implements ClusterSetupStrategy
{

    private static final Logger LOG = LoggerFactory.getLogger( OverHadoopSetupStrategy.class );
    private final OozieClusterConfig oozieClusterConfig;
    private final TrackerOperation po;
    private final OozieImpl manager;
    private Environment environment;
    private ContainerHost server;
    private Set<ContainerHost> clients;


    public OverHadoopSetupStrategy( final OozieClusterConfig oozieClusterConfig,
                                    final TrackerOperation po, final OozieImpl oozieManager )
    {
        Preconditions.checkNotNull( oozieClusterConfig, "Cluster config is null" );
        Preconditions.checkNotNull( po, "Product operation tracker is null" );
        Preconditions.checkNotNull( oozieManager, "ZK manager is null" );

        this.oozieClusterConfig = oozieClusterConfig;
        this.po = po;
        this.manager = oozieManager;
    }


    @Override
    public OozieClusterConfig setup() throws ClusterSetupException
    {
        // CHECKING for oozie - clients and server
        // =====================================================================================================================
        if ( Strings.isNullOrEmpty( oozieClusterConfig.getClusterName() ) ||
                oozieClusterConfig.getServer() == null || oozieClusterConfig.getClients().isEmpty() )
        {
            throw new ClusterSetupException( "Malformed configuration" );
        }

        if ( manager.getCluster( oozieClusterConfig.getClusterName() ) != null )
        {
            throw new ClusterSetupException(
                    String.format( "Cluster with name '%s' already exists", oozieClusterConfig.getClusterName() ) );
        }

        HadoopClusterConfig hadoopClusterConfig =
                manager.getHadoopManager().getCluster( oozieClusterConfig.getHadoopClusterName() );
        if ( hadoopClusterConfig == null )
        {
            throw new ClusterSetupException(
                    String.format( "Hadoop cluster %s not found", oozieClusterConfig.getHadoopClusterName() ) );
        }

        if ( !hadoopClusterConfig.getAllNodes().containsAll( oozieClusterConfig.getAllNodes() ) )
        {
            throw new ClusterSetupException(
                    String.format( "Not all specified Oozie Client and Server nodes belong to %s Hadoop " + "cluster",
                            hadoopClusterConfig.getClusterName() ) );
        }


        try
        {
            environment = manager.getEnvironmentManager().findEnvironment( hadoopClusterConfig.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            LOG.error( "Error getting environment by id: " + hadoopClusterConfig.getEnvironmentId().toString(), e );
        }

        if ( environment == null )
        {
            throw new ClusterSetupException( "Hadoop environment not found" );
        }


        po.addLog( "Checking prerequisites..." );

        Set<ContainerHost> oozieNodes = null;
        try
        {
            oozieNodes = environment.getContainerHostsByIds( oozieClusterConfig.getAllNodes() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            LOG.error( "Container hosts not found", e );
            po.addLogFailed( "Container hosts not found" );
        }
        if ( oozieNodes.size() < oozieClusterConfig.getAllNodes().size() )
        {
            throw new ClusterSetupException(
                    String.format( "Only %d nodes found in environment whereas %d expected", oozieNodes.size(),
                            oozieClusterConfig.getAllNodes().size() ) );
        }


        for ( ContainerHost hiveNode : oozieNodes )
        {
            if ( !hiveNode.isConnected() )
            {
                throw new ClusterSetupException( String.format( "Node %s is not connected", hiveNode.getHostname() ) );
            }
        }

        try
        {
            server = environment.getContainerHostById( oozieClusterConfig.getServer() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            LOG.error( "Container host not found", e );
            po.addLogFailed( "Container host not found" );
        }

        try
        {
            clients = environment.getContainerHostsByIds( oozieClusterConfig.getClients() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            LOG.error( "Container hosts not found", e );
            po.addLogFailed( "Container hosts not found" );
        }


        //check installed subutai packages
        List<CommandResult> commandResultList = runCommandOnContainers( Commands.make( CommandType.STATUS ), clients );

        if ( getFailedCommandResults( commandResultList ).size() != 0 )
        {
            throw new ClusterSetupException( "Failed to check presence of installed subutai packages" );
        }

        Iterator<ContainerHost> iterator = clients.iterator();
        int nodeIndex = 0;
        while ( iterator.hasNext() )
        {
            ContainerHost host = iterator.next();
            CommandResult result = commandResultList.get( nodeIndex++ );

            if ( result.getStdOut().contains( Common.PACKAGE_PREFIX + OozieClusterConfig.PRODUCT_NAME_CLIENT ) )
            {
                throw new ClusterSetupException(
                        String.format( "Node %s already has OozieClient installed", host.getHostname() ) );
            }
            else if ( !result.getStdOut().contains( Common.PACKAGE_PREFIX + HadoopClusterConfig.PRODUCT_NAME ) )
            {
                throw new ClusterSetupException(
                        String.format( "Node %s has no Hadoop installed", host.getHostname() ) );
            }
        }
        //======================================================================================================================
        // CHECKING for oozie - server
        //======================================================================================================================
               po.addLog( "Configuring cluster..." );
     Set<ContainerHost> oozieServerNodes = new HashSet<>();
        oozieServerNodes.add( server );

        //check installed subutai packages
        List<CommandResult> commandResultList2 =
                runCommandOnContainers( Commands.make( CommandType.STATUS ), oozieServerNodes );

        if ( getFailedCommandResults( commandResultList2 ).size() != 0 )
        {
            throw new ClusterSetupException( "Failed to check presence of installed subutai packages" );
        }

        Iterator<ContainerHost> iterator2 = oozieServerNodes.iterator();
        int nodeIndex2 = 0;
        while ( iterator2.hasNext() )
        {
            ContainerHost host = iterator2.next();
            CommandResult result = commandResultList2.get( nodeIndex2++ );

            if ( result.getStdOut().contains( Common.PACKAGE_PREFIX + OozieClusterConfig.PRODUCT_NAME_SERVER ) )
            {
                throw new ClusterSetupException(
                        String.format( "Node %s already has OozieServer installed", host.getHostname() ) );
            }
        }
        //======================================================================================================================

        po.addLog( String.format( "Installing Oozie Server and Oozie Client..." ) );

        //install
        commandResultList2 = runCommandOnContainers( Commands.make( CommandType.INSTALL_SERVER ), oozieServerNodes );
        commandResultList = runCommandOnContainers( Commands.make( CommandType.INSTALL_CLIENT ), clients );

        if ( ( getFailedCommandResults( commandResultList2 ).size() == 0 ) && (
                getFailedCommandResults( commandResultList ).size() == 0 ) )
        {
            po.addLog( "Installation succeeded\nConfiguring cluster..." );

            try
            {
                new ClusterConfiguration( manager, po ).configureCluster( oozieClusterConfig, environment );
            }
            catch ( ClusterConfigurationException e )
            {
                throw new ClusterSetupException( e.getMessage() );
            }

            po.addLog( "Saving cluster information to database..." );


            oozieClusterConfig.setEnvironmentId( environment.getId() );

            manager.getPluginDao()
                   .saveInfo( OozieClusterConfig.PRODUCT_KEY, oozieClusterConfig.getClusterName(), oozieClusterConfig );
            po.addLog( "Cluster information saved to database" );
        }
        else
        {
            StringBuilder stringBuilder = new StringBuilder();
            for ( CommandResult commandResult : getFailedCommandResults( commandResultList2 ) )
            {
                stringBuilder.append( commandResult.getStdErr() );
            }

            throw new ClusterSetupException( String.format( "Installation failed, %s", stringBuilder ) );
        }

        return oozieClusterConfig;
    }


    private List<CommandResult> runCommandOnContainers( String command, final Set<ContainerHost> oozieNodes )
    {
        List<CommandResult> commandResults = new ArrayList<>();
        for ( ContainerHost containerHost : oozieNodes )
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
