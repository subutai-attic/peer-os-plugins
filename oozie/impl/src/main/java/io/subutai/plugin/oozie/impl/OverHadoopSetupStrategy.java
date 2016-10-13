package io.subutai.plugin.oozie.impl;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.settings.Common;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.core.plugincommon.api.ClusterConfigurationException;
import io.subutai.core.plugincommon.api.ClusterSetupException;
import io.subutai.core.plugincommon.api.ClusterSetupStrategy;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.oozie.api.OozieClusterConfig;


public class OverHadoopSetupStrategy implements ClusterSetupStrategy
{

    private static final Logger LOG = LoggerFactory.getLogger( OverHadoopSetupStrategy.class );
    private final OozieClusterConfig oozieClusterConfig;
    private final TrackerOperation po;
    private final OozieImpl manager;
    private Environment environment;
    private EnvironmentContainerHost server;
    private Set<EnvironmentContainerHost> clients;


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
        if ( Strings.isNullOrEmpty( oozieClusterConfig.getClusterName() ) || oozieClusterConfig.getServer() == null )
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
            environment = manager.getEnvironmentManager().loadEnvironment( hadoopClusterConfig.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            LOG.error( "Error getting environment by id: " + hadoopClusterConfig.getEnvironmentId(), e );
        }

        if ( environment == null )
        {
            throw new ClusterSetupException( "Hadoop environment not found" );
        }


        po.addLog( "Checking prerequisites..." );

        Set<EnvironmentContainerHost> oozieNodes;
        try
        {
            oozieNodes = environment.getContainerHostsByIds( oozieClusterConfig.getAllNodes() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            LOG.error( "Container hosts not found", e );
            po.addLogFailed( "Container hosts not found" );
            throw new ClusterSetupException( e );
        }
        if ( oozieNodes.size() < oozieClusterConfig.getAllNodes().size() )
        {
            throw new ClusterSetupException(
                    String.format( "Only %d nodes found in environment whereas %d expected", oozieNodes.size(),
                            oozieClusterConfig.getAllNodes().size() ) );
        }


        for ( EnvironmentContainerHost oozieNode : oozieNodes )
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


        //======================================================================================================================
        // CHECKING for oozie - server
        //======================================================================================================================

        po.addLog( "Installing Oozie Server..." );

        //install
        runCommandOnContainer( Commands.getAptUpdate(), server );
        CommandResult result = runCommandOnContainer( Commands.getInstallServerCommand(), server );
        checkInstalled( server, result );

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

        return oozieClusterConfig;
    }


    private CommandResult runCommandOnContainer( RequestBuilder command, EnvironmentContainerHost node )
    {
        CommandResult result = null;
        try
        {
            result = node.execute( command );
        }
        catch ( CommandException e )
        {
            LOG.error( "Error executin command on container", e.getMessage() );
            po.addLogFailed( "Error executin command on container" );
            e.printStackTrace();
        }
        return result;
    }


    public void checkInstalled( EnvironmentContainerHost host, CommandResult result ) throws ClusterSetupException
    {
        CommandResult statusResult;
        try
        {
            statusResult = host.execute( Commands.getCheckInstalledCommand() );
        }
        catch ( CommandException e )
        {
            throw new ClusterSetupException( String.format( "Error on container %s:", host.getHostname() ) );
        }

        if ( !( result.hasSucceeded() && ( statusResult.getStdOut().contains( Commands.SERVER_PACKAGE_NAME ) ) ) )
        {
            po.addLogFailed( String.format( "Error on container %s:", host.getHostname() ) );
            throw new ClusterSetupException( String.format( "Error on container %s: %s", host.getHostname(),
                    result.hasCompleted() ? result.getStdErr() : "Command timed out" ) );
        }
    }
}
