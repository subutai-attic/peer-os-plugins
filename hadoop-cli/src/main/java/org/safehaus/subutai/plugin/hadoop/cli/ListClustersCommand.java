package org.safehaus.subutai.plugin.hadoop.cli;


import java.util.List;

import org.safehaus.subutai.plugin.hadoop.api.Hadoop;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;

import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;


/**
 * sample command :
 *      hadoop:list-cluster
 */
@Command( scope = "hadoop", name = "list-clusters", description = "Shows the list of Hadoop clusters" )
public class ListClustersCommand extends OsgiCommandSupport
{
    private Hadoop hadoopManager;

    @Override
    protected Object doExecute()
    {

        List<HadoopClusterConfig> hadoopClusterConfigList = hadoopManager.getClusters();
        if ( !hadoopClusterConfigList.isEmpty() )
        {
            for ( HadoopClusterConfig hadoopClusterConfig : hadoopClusterConfigList )
            {
                System.out.println( hadoopClusterConfig.getClusterName() );
            }
        }
        else
        {
            System.out.println( "No Hadoop cluster" );
        }

        return null;
    }


    public Hadoop getHadoopManager()
    {
        return hadoopManager;
    }


    public void setHadoopManager( Hadoop hadoopManager )
    {
        this.hadoopManager = hadoopManager;
    }
}
