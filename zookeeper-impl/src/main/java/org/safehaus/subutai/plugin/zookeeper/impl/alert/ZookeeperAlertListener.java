package org.safehaus.subutai.plugin.zookeeper.impl.alert;


import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.safehaus.subutai.common.command.CommandException;
import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.metric.ProcessResourceUsage;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.core.metric.api.AlertListener;
import org.safehaus.subutai.core.metric.api.ContainerHostMetric;
import org.safehaus.subutai.core.metric.api.MonitoringSettings;
import org.safehaus.subutai.core.peer.api.CommandUtil;
import org.safehaus.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;
import org.safehaus.subutai.plugin.zookeeper.impl.Commands;
import org.safehaus.subutai.plugin.zookeeper.impl.ZookeeperImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Created by talas on 1/15/15.
 */
public class ZookeeperAlertListener implements AlertListener
{
    private static final Logger LOGGER = LoggerFactory.getLogger( ZookeeperAlertListener.class );
    private ZookeeperImpl zookeeper;
    public static final String ZOOKEEPER_ALERT_LISTENER = "ZOOKEEPER_ALERT_LISTENER";
    private CommandUtil commandUtil = new CommandUtil();
    private static int MAX_RAM_QUOTA_MB = 2048;
    private static int RAM_QUOTA_INCREMENT_MB = 512;
    private static int MAX_CPU_QUOTA_PERCENT = 80;
    private static int CPU_QUOTA_INCREMENT_PERCENT = 10;


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
        double redLine = 0.9;
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
                    //we can increase RAM quota
                    zookeeper.getQuotaManager().setRamQuota( sourceHost.getId(),
                            Math.min( MAX_RAM_QUOTA_MB, ramQuota + RAM_QUOTA_INCREMENT_MB ) );

                    quotaIncreased = true;
                }
            }
            if ( isCpuStressedByZookeeper )
            {

                //read current CPU quota
                int cpuQuota = zookeeper.getQuotaManager().getCpuQuota( sourceHost.getId() );

                if ( cpuQuota < MAX_CPU_QUOTA_PERCENT )
                {
                    //we can increase CPU quota
                    zookeeper.getQuotaManager().setCpuQuota( sourceHost.getId(),
                            Math.min( MAX_CPU_QUOTA_PERCENT, cpuQuota + CPU_QUOTA_INCREMENT_PERCENT ) );

                    quotaIncreased = true;
                }
            }

            //quota increase is made, return
            if ( quotaIncreased )
            {
                return;
            }

            // add new node
            ZookeeperClusterConfig zookeeperClusterConfig = zookeeper.getCluster( targetCluster.getClusterName() );
            if ( zookeeperClusterConfig == null )
            {
                throw new Exception( String.format( "Zookeeper cluster %s not found", targetCluster.getClusterName() ),
                        null );
            }

            Set<UUID> availableNodes = zookeeperClusterConfig.getNodes();
            availableNodes.removeAll( targetCluster.getNodes() );

            //no available nodes -> notify user
            if ( availableNodes.isEmpty() )
            {
                notifyUser();
            }
            //add first available node
            else
            {
                UUID newNodeId = availableNodes.iterator().next();
                String newNodeHostName = null;
                for ( ContainerHost containerHost : containers )
                {
                    if ( containerHost.getId().equals( newNodeId ) )
                    {
                        newNodeHostName = containerHost.getHostname();
                        break;
                    }
                }

                if ( newNodeHostName == null )
                {
                    throw new Exception(
                            String.format( "Could not obtain available spark node from environment by id %s",
                                    newNodeId ), null );
                }

                //launch node addition process
                zookeeper.addNode( targetCluster.getClusterName(), newNodeHostName );
            }
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
