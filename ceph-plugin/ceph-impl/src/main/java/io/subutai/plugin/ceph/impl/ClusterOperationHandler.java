package io.subutai.plugin.ceph.impl;


import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.common.peer.ContainerHost;
import io.subutai.core.environment.api.EnvironmentManager;


public class ClusterOperationHandler
{
    private RequestBuilder requestBuilder;
    private ContainerHost host;


    public ClusterOperationHandler( final EnvironmentManager environmentManager, String environmentId,
                                    String lxcHostName, final String clusterName )
    {
        try
        {
            Environment environment = environmentManager.loadEnvironment( environmentId );
            host = environment.getContainerHostByHostname( lxcHostName );
        }
        catch ( EnvironmentNotFoundException | ContainerHostNotFoundException e )
        {
            e.printStackTrace();
        }
    }


    protected String execute()
    {
        requestBuilder = new RequestBuilder( "sudo chmod +x /etc/ceph/ceph.sh" );
        CommandResult result = null;
        try
        {
            host.execute( requestBuilder );
            requestBuilder = new RequestBuilder( "sudo bash /etc/ceph/ceph.sh" ).withTimeout( 2000 );
            result = host.execute( requestBuilder );

            if ( result.hasSucceeded() )
            {
                requestBuilder = new RequestBuilder( "sudo chmod +x /etc/ceph/radosgw.sh" );
                host.execute( requestBuilder );
                requestBuilder = new RequestBuilder( "sudo bash /etc/ceph/radosgw.sh" ).withTimeout( 2000 );
                result = host.execute( requestBuilder );

                if ( result.hasSucceeded() )
                {
                    requestBuilder = new RequestBuilder( "sudo chmod +x /etc/ceph/getuser.sh" );
                    host.execute( requestBuilder );
                    requestBuilder = new RequestBuilder( "sudo bash /etc/ceph/getuser.sh" ).withTimeout( 2000 );
                    result = host.execute( requestBuilder );
                }
            }
        }
        catch ( CommandException e )
        {
            e.printStackTrace();
        }

        return result.getStdOut();
    }
}
