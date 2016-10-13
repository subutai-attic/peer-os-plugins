package io.subutai.plugin.oozie.impl;


import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
    private static final Logger LOG = LoggerFactory.getLogger( ClusterConfiguration.class );

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
        try
        {
            EnvironmentContainerHost namenode = environment.getContainerHostById( config.getServer() );

            namenode.execute( new RequestBuilder( Commands.getStopDfsCommand() ) );
            namenode.execute( new RequestBuilder( Commands.getStopYarnCommand() ) );
            namenode.execute( Commands.getConfigureRootHostsCommand() );
            namenode.execute( Commands.getConfigureRootGroupsCommand() );
            namenode.execute( new RequestBuilder( Commands.getStartDfsCommand() ) );
            namenode.execute( new RequestBuilder( Commands.getStartYarnCommand() ) );
            namenode.execute( Commands.getBuildWarCommand() );
            namenode.execute( Commands.getCopyToHdfsCommand( namenode.getHostname() ) );
            namenode.execute( Commands.getStartServerCommand() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            LOG.error( "Container host not found", e );
            po.addLogFailed( "Container host not found" );
            e.printStackTrace();
        }
        catch ( CommandException e )
        {
            LOG.error( "Error executin command on container", e.getMessage() );
            po.addLogFailed( "Error executin command on container" );
            e.printStackTrace();
        }
    }
}


