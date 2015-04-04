package org.safehaus.subutai.plugin.zookeeper.impl.alert;


import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.CommandUtil;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.metric.ProcessResourceUsage;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.core.metric.api.AlertListener;
import org.safehaus.subutai.core.metric.api.ContainerHostMetric;
import org.safehaus.subutai.core.metric.api.MonitoringSettings;
import org.safehaus.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;
import org.safehaus.subutai.plugin.zookeeper.impl.Commands;
import org.safehaus.subutai.plugin.zookeeper.impl.ZookeeperImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class ZookeeperAlertListener implements AlertListener
{
    private static final Logger LOGGER = LoggerFactory.getLogger( ZookeeperAlertListener.class );
    private ZookeeperImpl zookeeper;
    public static final String ZOOKEEPER_ALERT_LISTENER = "ZOOKEEPER_ALERT_LISTENER";
    private CommandUtil commandUtil = new CommandUtil();
    private static double MAX_RAM_QUOTA_MB;
    private static int RAM_QUOTA_INCREMENT_PERCENTAGE = 25;
    private static int MAX_CPU_QUOTA_PERCENT = 100;
    private static int CPU_QUOTA_INCREMENT_PERCENT = 15;


    public ZookeeperAlertListener( final ZookeeperImpl zookeeper )
    {
        this.zookeeper = zookeeper;
    }


    @Override
    public void onAlert( final ContainerHostMetric containerHostMetric ) throws Exception
    {
        //find zookeeper cluster by environment id
        List<ZookeeperClusterConfig> clusters = zookeeper.getClusters();

        ZookeeperClusterConfig targetCluster = null;
        for ( ZookeeperClusterConfig cluster : clusters )
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
                zookeeper.getEnvironmentManager().findEnvironment( containerHostMetric.getEnvironmentId() );
        if ( environment == null )
        {
            throw new Exception(
                    String.format( "Environment not found by id %s", containerHostMetric.getEnvironmentId() ), null );
        }

        //get environment containers and find alert source host
        Set<ContainerHost> containers = environment.getContainerHosts();

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

        //check if source host belongs to found zookeeper cluster
        if ( !targetCluster.getNodes().contains( sourceHost.getId() ) )
        {
            LOGGER.info( String.format( "Alert source host %s does not belong to Zookeeper cluster",
                    containerHostMetric.getHost() ) );
            return;
        }

        // Set 80 percent of the available ram capacity of the resource host
        // to maximum ram quota limit assignable to the container
        MAX_RAM_QUOTA_MB = sourceHost.getAvailableRamQuota() * 0.8;

        //figure out Zookeeper process pid
        int zookeeperPid = 0;
        try
        {
            CommandResult result = commandUtil.execute( new RequestBuilder( Commands.getStatusCommand() ), sourceHost );
            zookeeperPid = parsePid( result.getStdOut() );
        }
        catch ( NumberFormatException | CommandException e )
        {
            throw new Exception( "Error obtaining Zookeeper process PID", e );
        }

        //get Zookeeper process resource usage by Zookeeper pid
        ProcessResourceUsage processResourceUsage =
                zookeeper.getMonitor().getProcessResourceUsage( sourceHost, zookeeperPid );

        //confirm that Zookeeper is causing the stress, otherwise no-op
        MonitoringSettings thresholds = zookeeper.getAlertSettings();
        double ramLimit = containerHostMetric.getTotalRam() * ( thresholds.getRamAlertThreshold() / 100 ); // 0.8
        double redLine = 0.4;
        boolean isCpuStressedByZookeeper = false;
        boolean isRamStressedByZookeeper = false;

        if ( processResourceUsage.getUsedRam() >= ramLimit * redLine )
        {
            isRamStressedByZookeeper = true;
        }
        if ( processResourceUsage.getUsedCpu() >= thresholds.getCpuAlertThreshold() * redLine )
        {
            isCpuStressedByZookeeper = true;
        }

        if ( !( isRamStressedByZookeeper || isCpuStressedByZookeeper ) )
        {
            LOGGER.info( "Zookeeper cluster runs ok" );
            return;
        }


        //auto-scaling is enabled -> scale cluster
        if ( targetCluster.isAutoScaling() )
        {
            // check if a quota limit increase does it
            boolean quotaIncreased = false;

            if ( isRamStressedByZookeeper )
            {
                //read current RAM quota
                int ramQuota = zookeeper.getQuotaManager().getRamQuota( sourceHost.getId() );


                if ( ramQuota < MAX_RAM_QUOTA_MB )
                {
                    // if available quota on resource host is greater than 10 % of calculated increase amount,
                    // increase quota, otherwise scale horizontally
                    int newRamQuota = ramQuota * ( 100 + RAM_QUOTA_INCREMENT_PERCENTAGE ) / 100;
                    if ( MAX_RAM_QUOTA_MB > newRamQuota )
                    {
                        LOGGER.info( "Increasing ram quota of {} from {} MB to {} MB.", sourceHost.getHostname(),
                                sourceHost.getRamQuota(), newRamQuota );

                        //we can increase RAM quota
                        sourceHost.setRamQuota( newRamQuota );
                        quotaIncreased = true;
                    }
                }
            }
            if ( isCpuStressedByZookeeper )
            {

                //read current CPU quota
                int cpuQuota = sourceHost.getCpuQuota();

                if ( cpuQuota < MAX_CPU_QUOTA_PERCENT )
                {
                    int newCpuQuota = Math.min( MAX_CPU_QUOTA_PERCENT, cpuQuota + CPU_QUOTA_INCREMENT_PERCENT );
                    LOGGER.info( "Increasing cpu quota of {} from {}% to {}%.", sourceHost.getHostname(), cpuQuota,
                            newCpuQuota );
                    //we can increase CPU quota
                    sourceHost.setCpuQuota( newCpuQuota );

                    quotaIncreased = true;
                }
            }

            //quota increase is made, return
            if ( quotaIncreased )
            {
                //TODO adding the following line for testing purposes, remove it after that
                zookeeper.addNode( targetCluster.getClusterName() );
                return;
            }

            // add new node
            LOGGER.info( "Adding new node to {} zookeeper cluster", targetCluster.getClusterName() );

            //launch node addition process
            zookeeper.addNode( targetCluster.getClusterName() );
        }
        else
        {
            notifyUser();
        }
    }


    @Override
    public String getSubscriberId()
    {
        return ZOOKEEPER_ALERT_LISTENER;
    }


    protected int parsePid( String output ) throws Exception
    {
        Pattern p = Pattern.compile( "pid\\s*:\\s*(\\d+)", Pattern.CASE_INSENSITIVE );

        Matcher m = p.matcher( output );

        if ( m.find() )
        {
            return Integer.parseInt( m.group( 1 ) );
        }
        else
        {
            throw new Exception( String.format( "Could not parse PID from %s", output ), null );
        }
    }


    protected void notifyUser()
    {
        //TODO implement me when user identity management is complete and we can figure out user email
    }
}
