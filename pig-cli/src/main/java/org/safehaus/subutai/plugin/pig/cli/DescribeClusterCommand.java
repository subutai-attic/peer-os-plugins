package org.safehaus.subutai.plugin.pig.cli;


import java.util.UUID;

import org.safehaus.subutai.common.environment.ContainerHostNotFoundException;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.environment.EnvironmentNotFoundException;
import org.safehaus.subutai.core.env.api.EnvironmentManager;
import org.safehaus.subutai.plugin.pig.api.Pig;
import org.safehaus.subutai.plugin.pig.api.PigConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;


/**
 * sample command : pig:describe-cluster test \ {cluster name}
 */
@Command( scope = "pig", name = "describe-cluster", description = "Shows the details of the Pig cluster." )
public class DescribeClusterCommand extends OsgiCommandSupport
{

    @Argument( index = 0, name = "clusterName", description = "The name of the cluster.", required = true,
            multiValued = false )
    String clusterName = null;
    private Pig pigManager;
    private EnvironmentManager environmentManager;
    private static final Logger LOG = LoggerFactory.getLogger( DescribeClusterCommand.class.getName() );


    protected Object doExecute()
    {
        PigConfig config = pigManager.getCluster( clusterName );
        try
        {
            Environment environment = environmentManager.findEnvironment( config.getEnvironmentId() );
            StringBuilder sb = new StringBuilder();
            sb.append( "Cluster name: " ).append( config.getClusterName() ).append( "\n" );
            sb.append( "Nodes:" ).append( "\n" );
            for ( UUID containerId : config.getNodes() )
            {
                try
                {
                    sb.append( "   Container Hostname: " ).
                            append( environment.getContainerHostById( containerId ).getHostname() ).append( "\n" );
                }
                catch ( ContainerHostNotFoundException e )
                {
                    LOG.error( "Could not find container host !!!" );
                    e.printStackTrace();
                }
            }
            System.out.println( sb.toString() );
        }
        catch ( EnvironmentNotFoundException e )
        {
            LOG.error( "Could not find environment !!! " );
            e.printStackTrace();
        }

        return null;
    }


    public Pig getPigManager()
    {
        return pigManager;
    }


    public void setPigManager( final Pig pigManager )
    {
        this.pigManager = pigManager;
    }


    public EnvironmentManager getEnvironmentManager()
    {
        return environmentManager;
    }


    public void setEnvironmentManager( final EnvironmentManager environmentManager )
    {
        this.environmentManager = environmentManager;
    }
}
