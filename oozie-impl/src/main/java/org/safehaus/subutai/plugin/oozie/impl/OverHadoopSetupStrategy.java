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


    public OverHadoopSetupStrategy( final OozieClusterConfig oozieClusterConfig, final TrackerOperation po,
                                    final OozieImpl oozieManager )
    {
        Preconditions.checkNotNull( oozieClusterConfig, "Cluster config is null" );
        Preconditions.checkNotNull( po, "Product operation tracker is null" );
        Preconditions.checkNotNull( oozieManager, "Oozie manager is null" );

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


        for ( ContainerHost oozieNode : oozieNodes )
        {
            if ( !oozieNode.isConnected() )
            {
                throw new ClusterSetupException( String.format( "Node %s is not connected", oozieNode.getHostname() ) );
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

        List<ContainerHost> servers = new ArrayList<>( oozieServerNodes );
        List<ContainerHost> clientNodes = new ArrayList<>( clients );
        //install
        commandResultList2 = runCommandOnContainers( Commands.make( CommandType.INSTALL_SERVER ), oozieServerNodes );
        checkInstalled( servers, commandResultList2, Commands.SERVER_PACKAGE_NAME );
        commandResultList = runCommandOnContainers( Commands.make( CommandType.INSTALL_CLIENT ), clients );
        checkInstalled( clientNodes, commandResultList, Commands.CLIENT_PACKAGE_NAME );

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


    public void checkInstalled( List<ContainerHost> hosts, List<CommandResult> resultList, String packageName )
            throws ClusterSetupException
    {
        String okString = "install ok installed";
        boolean status = true;
        CommandResult currentResult = null;
        ContainerHost currentContainer = null;
        //nodes which already oozie installed on.
        List<ContainerHost> nodes = new ArrayList<>();
        for ( int i = 0; i < resultList.size(); i++ )
        {
            currentResult = resultList.get( i );
            currentContainer = hosts.get( i );
            CommandResult statusResult;
            try
            {
                statusResult = hosts.get( i ).execute( Commands.getCheckInstalledCommand( packageName ) );
            }
            catch ( CommandException e )
            {
                status = false;
                break;
            }

            if ( !( resultList.get( i ).hasSucceeded() && statusResult.getStdOut().contains( okString ) ) )
            {
                List<ContainerHost> node = new ArrayList<>();
                node.add( currentContainer );
                uninstallProductOnNode( node, packageName );
                status = false;
                break;
            }
            nodes.add( hosts.get( i ) );
        }
        if ( !status )
        {
            uninstallProductOnNode( nodes, packageName );
            po.addLogFailed(
                    String.format( "Couldn't install product on container %s:", currentContainer.getHostname() ) );
            throw new ClusterSetupException( String.format( "Error on container %s: %s", currentContainer.getHostname(),
                    currentResult.hasCompleted() ? currentResult.getStdErr() : "Command timed out" ) );
        }
    }


    private void uninstallProductOnNode( List<ContainerHost> hosts, String packageName )
    {
        RequestBuilder uninstallCommand = packageName.contains( "server" ) ? Commands.getUninstallServerCommand() :
                                          Commands.getUninstallClientsCommand();

        for ( ContainerHost host : hosts )
        {
            try
            {
                host.execute( Commands.getConfigureCommand() );
                host.execute( uninstallCommand );
            }
            catch ( CommandException e )
            {
                po.addLog( String.format( "Unable to execute uninstall command on node %s", host.getHostname() ) );
            }
        }
    }
}
