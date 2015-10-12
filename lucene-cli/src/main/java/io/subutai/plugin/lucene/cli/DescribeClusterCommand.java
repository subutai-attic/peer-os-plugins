package io.subutai.plugin.lucene.cli;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;

import io.subutai.common.environment.ContainerHostNotFoundException;
import io.subutai.common.environment.Environment;
import io.subutai.common.environment.EnvironmentNotFoundException;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.plugin.lucene.api.Lucene;
import io.subutai.plugin.lucene.api.LuceneConfig;


/**
 * sample command : lucene:describe-cluster test \ {cluster name}
 */
@Command( scope = "lucene", name = "describe-cluster", description = "Shows the details of the Lucene cluster." )
public class DescribeClusterCommand extends OsgiCommandSupport
{

    @Argument( index = 0, name = "clusterName", description = "The name of the cluster.", required = true,
            multiValued = false )
    String clusterName = null;
    private Lucene luceneManager;
    private EnvironmentManager environmentManager;
    private static final Logger LOG = LoggerFactory.getLogger( DescribeClusterCommand.class.getName() );


    protected Object doExecute()
    {
        LuceneConfig config = luceneManager.getCluster( clusterName );
        try
        {
            Environment environment = environmentManager.loadEnvironment( config.getEnvironmentId() );
            StringBuilder sb = new StringBuilder();
            sb.append( "Cluster name: " ).append( config.getClusterName() ).append( "\n" );
            sb.append( "Nodes:" ).append( "\n" );
            for ( String containerId : config.getNodes() )
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


    public Lucene getLuceneManager()
    {
        return luceneManager;
    }


    public void setLuceneManager( final Lucene luceneManager )
    {
        this.luceneManager = luceneManager;
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
