package io.subutai.plugin.oozie.cli;


import java.io.IOException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import io.subutai.core.tracker.api.Tracker;
import io.subutai.plugin.hadoop.api.Hadoop;
import io.subutai.plugin.oozie.api.Oozie;
import io.subutai.plugin.oozie.api.OozieClusterConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.karaf.shell.commands.Argument;
import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;


/**
 * sample command : oozie:install-cluster test \ {cluster name} test \ { hadoop cluster name } hadoop1 \ { server } [
 * hadoop1, hadoop2 ] \ { list of client machines }
 */
@Command( scope = "oozie", name = "install-cluster", description = "Command to install Oozie cluster" )
public class InstallClusterCommand extends OsgiCommandSupport
{

    @Argument( index = 0, name = "clusterName", description = "The name of the cluster.", required = true,
            multiValued = false )
    String clusterName = null;

    @Argument( index = 1, name = "hadoopClusterName", description = "The name of hadoop cluster.", required = true,
            multiValued = false )
    String hadoopClusterName = null;

    @Argument( index = 2, name = "server", description = "The hostname of server container", required = true,
            multiValued = false )
    String server = null;

    @Argument( index = 3, name = "clients", description = "The hostname list of client nodes", required = true,
            multiValued = false )
    String clients[] = null;

    private static final Logger LOG = LoggerFactory.getLogger( InstallClusterCommand.class.getName() );
    private Oozie oozieManager;
    private Hadoop hadoopManager;
    private Tracker tracker;


    protected Object doExecute() throws IOException
    {
        OozieClusterConfig config = new OozieClusterConfig();
        config.setClusterName( clusterName );
        config.setHadoopClusterName( hadoopClusterName );
        config.setEnvironmentId( hadoopManager.getCluster( hadoopClusterName ).getEnvironmentId() );
        config.setServer( UUID.fromString( server ) );

        Set<UUID> nodeSet = new HashSet<>();
        for ( String uuid : clients )
        {
            nodeSet.add( UUID.fromString( uuid ) );
        }
        config.setClients( nodeSet );

        System.out.println( "Installing oozie cluster..." );
        UUID uuid = oozieManager.installCluster( config );
        System.out.println( "Install operation is " + StartClusterCommand.waitUntilOperationFinish( tracker, uuid ) );

        return null;
    }


    public Oozie getOozieManager()
    {
        return oozieManager;
    }


    public void setOozieManager( final Oozie oozieManager )
    {
        this.oozieManager = oozieManager;
    }


    public Tracker getTracker()
    {
        return tracker;
    }


    public void setTracker( final Tracker tracker )
    {
        this.tracker = tracker;
    }


    public Hadoop getHadoopManager()
    {
        return hadoopManager;
    }


    public void setHadoopManager( final Hadoop hadoopManager )
    {
        this.hadoopManager = hadoopManager;
    }
 }