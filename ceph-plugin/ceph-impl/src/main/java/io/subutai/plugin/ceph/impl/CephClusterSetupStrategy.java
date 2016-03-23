package io.subutai.plugin.ceph.impl;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.tracker.TrackerOperation;
import io.subutai.plugin.ceph.api.CephClusterConfig;
import io.subutai.core.plugincommon.api.ClusterSetupException;
import io.subutai.core.plugincommon.api.ClusterSetupStrategy;


public class CephClusterSetupStrategy implements ClusterSetupStrategy
{
    private static final Logger LOG = LoggerFactory.getLogger( CephClusterSetupStrategy.class );
    private CephClusterConfig config;
    private TrackerOperation po;
    private CephImpl manager;
    private Environment environment;
    private ContainerHost radosHost;
    private RequestBuilder requestBuilder;


    public CephClusterSetupStrategy( final CephClusterConfig config, final TrackerOperation po, final CephImpl manager )
    {
        Preconditions.checkNotNull( config, "Cluster config is null" );
        Preconditions.checkNotNull( po, "Product operation tracker is null" );
        Preconditions.checkNotNull( manager, "Ceph manager is null" );

        this.config = config;
        this.po = po;
        this.manager = manager;
    }


    @Override
    public CephClusterConfig setup() throws ClusterSetupException
    {
        try
        {
            environment = manager.getEnvironmentManager().loadEnvironment( config.getEnvironmentId() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            LOG.error( "Error getting environment by id: " + config.getEnvironmentId(), e );
            po.addLogFailed( "Error getting environment" );
        }

        try
        {
            radosHost = environment.getContainerHostByHostname( config.getRadosGW() );
        }
        catch ( ContainerHostNotFoundException e )
        {
            LOG.error( "Container hosts not found", e );
            po.addLogFailed( "Container hosts not found" );
        }

        requestBuilder = new RequestBuilder( "sudo chmod +x /etc/ceph/ceph.sh" );
        CommandResult result = null;
        try
        {
            radosHost.execute( requestBuilder );
            po.addLog( "Executing ceph.sh script..." );
            requestBuilder = new RequestBuilder( "sudo bash /etc/ceph/ceph.sh" ).withTimeout( 2000 );
            result = radosHost.execute( requestBuilder );

            if ( result.hasSucceeded() )
            {
                requestBuilder = new RequestBuilder( "sudo chmod +x /etc/ceph/radosgw.sh" );
                radosHost.execute( requestBuilder );

                po.addLog( "Executing radosgw.sh script..." );
                requestBuilder = new RequestBuilder( "sudo bash /etc/ceph/radosgw.sh" ).withTimeout( 2000 );
                result = radosHost.execute( requestBuilder );

                if ( result.hasSucceeded() )
                {
                    requestBuilder = new RequestBuilder( "sudo chmod +x /etc/ceph/getuser.sh" );
                    radosHost.execute( requestBuilder );

                    po.addLog( "Executing getuser.sh script..." );
                    requestBuilder = new RequestBuilder( "sudo bash /etc/ceph/getuser.sh" ).withTimeout( 2000 );
                    result = radosHost.execute( requestBuilder );
                    po.addLog( result.getStdOut() );

                    if ( result.hasSucceeded() )
                    {
                        po.addLog( "Cluster configured\n" );

                        manager.getPluginDAO()
                               .saveInfo( CephClusterConfig.PRODUCT_KEY, config.getClusterName(), config );
                        po.addLog( "Cluster information saved to database" );
                    }
                }
                else
                {
                    po.addLogFailed( "Error executing radosgw.sh script." );
                }
            }
            else
            {
                po.addLogFailed( "Error executing ceph.sh script." );
            }
        }
        catch ( CommandException e )
        {
            e.printStackTrace();
        }

        return config;
    }
}
