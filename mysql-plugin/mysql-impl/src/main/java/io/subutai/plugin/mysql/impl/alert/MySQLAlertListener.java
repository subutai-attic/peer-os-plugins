package io.subutai.plugin.mysql.impl.alert;


import java.util.List;
import java.util.Set;
import java.util.logging.Logger;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.CommandUtil;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.Environment;
import io.subutai.common.metric.ContainerHostMetric;
import io.subutai.common.metric.ProcessResourceUsage;
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.core.metric.api.AlertListener;
import io.subutai.core.metric.api.MonitoringSettings;
import io.subutai.core.plugincommon.api.NodeType;
import io.subutai.plugin.mysql.api.MySQLClusterConfig;
import io.subutai.plugin.mysql.impl.MySQLCImpl;
import io.subutai.plugin.mysql.impl.common.Commands;


public class MySQLAlertListener implements AlertListener
{

    private static final Logger LOG = Logger.getLogger( MySQLAlertListener.class.toString() );
    private static final String MYSQL_ALERT_LISTENER = "MYSQL_ALERT_LISTENER";
    //@formatter:off
    private static double MAX_RAM_QUOTA_MB = 3072;
    private static int RAM_QUOTA_INCREMENT_PERCENTAGE = 25;
    private static int MAX_CPU_QUOTA_PERCENT = 100;
    private static int CPU_QUOTA_INCREMENT_PERCENT = 15;
    //@formatter:on
    private MySQLCImpl mysql;
    private CommandUtil commandUtil = new CommandUtil();


    public MySQLAlertListener( final MySQLCImpl mysql )
    {
        this.mysql = mysql;
    }


    @Override
    public void onAlert( final ContainerHostMetric containerHostMetric ) throws Exception
    {
        //find mysql cluster by environment id
        List<MySQLClusterConfig> clusters = mysql.getClusters();

        MySQLClusterConfig targetCluster = null;
        for ( MySQLClusterConfig cluster : clusters )
        {
            if ( cluster.getEnvironmentId().equals( containerHostMetric.getEnvironmentId() ) )
            {
                targetCluster = cluster;
                break;
            }
        }

        if ( targetCluster == null )
        {
            throw new Exception(
                    String.format( "Cluster not found by environment id %s", containerHostMetric.getEnvironmentId() ),
                    null );
        }

        //get cluster environment
        Environment environment =
                mysql.getEnvironmentManager().loadEnvironment( containerHostMetric.getEnvironmentId() );
        if ( environment == null )
        {
            throw new Exception(
                    String.format( "Environment not found by id %s", containerHostMetric.getEnvironmentId() ), null );
        }

        //get environment containers and find alert source host
        Set<EnvironmentContainerHost> containers = environment.getContainerHosts();

        ContainerHost sourceHost = null;
        for ( ContainerHost containerHost : containers )
        {
            if ( containerHost.getId().equals( containerHostMetric.getHostId() ) )
            {
                sourceHost = containerHost;
                break;
            }
        }

        if ( sourceHost == null )
        {
            throw new Exception(
                    String.format( "Alert source host %s not found in environment", containerHostMetric.getHost() ),
                    null );
        }

        //check if source host belongs to found mysql cluster
        if ( !targetCluster.getAllNodes().contains( sourceHost.getId() ) )
        {
            LOG.info( String.format( "Alert source host %s does not belong to mysql cluster",
                    containerHostMetric.getHost() ) );
            return;
        }

        // Set 80 percent of the available ram capacity of the resource host
        // to maximum ram quota limit assignable to the container
        MAX_RAM_QUOTA_MB = sourceHost.getAvailableRamQuota() * 0.8;

        //figure out mysql  process pid
        int mysqlPid;
        try
        {
            CommandResult result = commandUtil.execute( new RequestBuilder( Commands.getPidCommand ), sourceHost );
            mysqlPid = parsePid( result.getStdOut() );
        }
        catch ( NumberFormatException | CommandException e )
        {
            throw new Exception( "Error obtaining mysql process PID", e );
        }

        ProcessResourceUsage processResourceUsage = mysql.getMonitor().getProcessResourceUsage( sourceHost.getContainerId(), mysqlPid );

        //confirm that mysql is causing the stress, otherwise no-op
        MonitoringSettings thresholds = mysql.getAlertSettings();
        double ramLimit = containerHostMetric.getTotalRam() * ( thresholds.getRamAlertThreshold() / 100 ); // 0.8
        double redLine = 0.7;
        boolean cpuStressedByMysql = false;
        boolean ramStressedByMysql = false;

        if ( processResourceUsage.getUsedRam() >= ramLimit * redLine )
        {
            ramStressedByMysql = true;
        }
        if ( processResourceUsage.getUsedCpu() >= thresholds.getCpuAlertThreshold() * redLine )
        {
            cpuStressedByMysql = true;
        }

        if ( !ramStressedByMysql && !cpuStressedByMysql )
        {
            LOG.info( "mysql cluster runs ok" );
            return;
        }


        //auto-scaling is enabled -> scale cluster
        if ( targetCluster.isAutoScaling() )
        {
            // check if a quota limit increase does it
            boolean quotaIncreased = false;

            if ( ramStressedByMysql )
            {
                //read current RAM quota
                int ramQuota = sourceHost.getRamQuota();


                if ( ramQuota < MAX_RAM_QUOTA_MB )
                {
                    // if available quota on resource host is greater than 10 % of calculated increase amount,
                    // increase quota, otherwise scale horizontally
                    int newRamQuota = ramQuota * ( 100 + RAM_QUOTA_INCREMENT_PERCENTAGE ) / 100;

                    if ( MAX_RAM_QUOTA_MB > newRamQuota )
                    {
                        LOG.info( String.format( "Increasing ram quota of %s from %s MB to %s MB.",
                                sourceHost.getHostname(), sourceHost.getRamQuota(), newRamQuota ) );

                        //we can increase RAM quota
                        sourceHost.setRamQuota( newRamQuota );
                        quotaIncreased = true;
                    }
                }
            }
            if ( cpuStressedByMysql )
            {

                //read current CPU quota
                int cpuQuota = sourceHost.getCpuQuota();

                if ( cpuQuota < MAX_CPU_QUOTA_PERCENT )
                {
                    int newCpuQuota = Math.min( MAX_CPU_QUOTA_PERCENT, cpuQuota + CPU_QUOTA_INCREMENT_PERCENT );
                    //we can increase CPU quota
                    sourceHost.setCpuQuota( newCpuQuota );

                    quotaIncreased = true;
                }
            }

            //quota increase is made, return
            if ( quotaIncreased )
            {
                return;
            }


            // add new node
            MySQLClusterConfig MySQLClusterConfig = mysql.getCluster( targetCluster.getClusterName() );
            if ( MySQLClusterConfig == null )
            {
                throw new Exception( String.format( "mysql cluster %s not found", targetCluster.getClusterName() ),
                        null );
            }

            boolean isDataNode = MySQLClusterConfig.getDataNodes().contains( sourceHost.getId() );


            //no available nodes -> notify user
            if ( !isDataNode )
            {
                notifyUser();
            }
            //add first available node
            else
            {
                //launch node addition process
                mysql.addNode( targetCluster.getClusterName(), NodeType.DATANODE );
            }
        }
        else
        {
            notifyUser();
        }
    }


    protected int parsePid( String output ) throws Exception
    {
        int pid;
        output = output.replaceAll( "\n", "" );
        pid = Integer.parseInt( output );
        if ( pid == 0 )
        {
            throw new CommandException( "Couldn't parse pid" );
        }
        return pid;
    }


    @Override
    public String getSubscriberId()
    {
        return MYSQL_ALERT_LISTENER;
    }


    protected void notifyUser()
    {
        //TODO implement me when user identity management is complete and we can figure out user email
    }
}
