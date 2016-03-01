package io.subutai.plugin.hbase.cli;


import java.util.List;

import io.subutai.plugin.hbase.api.HBase;
import io.subutai.plugin.hbase.api.HBaseConfig;

import org.apache.karaf.shell.commands.Command;
import org.apache.karaf.shell.console.OsgiCommandSupport;


@Command( scope = "hbase", name = "list-clusters", description = "Lists clusters" )
public class ListHBaseClustersCommand extends OsgiCommandSupport
{

    private HBase hbaseManager;


    protected Object doExecute()
    {
        List<HBaseConfig> configs = hbaseManager.getClusters();
        StringBuilder sb = new StringBuilder();

        for ( HBaseConfig config : configs )
        {
            sb.append( config.getClusterName() ).append( "\n" );
        }

        System.out.println( sb.toString() );

        return null;
    }


    public HBase getHbaseManager()
    {
        return hbaseManager;
    }


    public void setHbaseManager( HBase hbaseManager )
    {
        this.hbaseManager = hbaseManager;
    }
}
