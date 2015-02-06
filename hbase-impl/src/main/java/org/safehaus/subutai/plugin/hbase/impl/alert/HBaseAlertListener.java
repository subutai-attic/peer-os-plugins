package org.safehaus.subutai.plugin.hbase.impl.alert;


import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.CommandUtil;
import org.safehaus.subutai.common.environment.Environment;
import org.safehaus.subutai.common.metric.ProcessResourceUsage;
import org.safehaus.subutai.common.peer.ContainerHost;
import org.safehaus.subutai.core.metric.api.AlertListener;
import org.safehaus.subutai.core.metric.api.ContainerHostMetric;
import org.safehaus.subutai.core.metric.api.MonitoringSettings;
import org.safehaus.subutai.plugin.common.api.NodeType;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.hbase.api.HBaseConfig;
import org.safehaus.subutai.plugin.hbase.impl.Commands;
import org.safehaus.subutai.plugin.hbase.impl.HBaseImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Node resource threshold excess alert listener
 */
public class HBaseAlertListener implements AlertListener
{
    private static final Logger LOG = LoggerFactory.getLogger( HBaseAlertListener.class.getName() );

    public static final String HBASE_ALERT_LISTENER = "HBASE_ALERT_LISTENER";
    private HBaseImpl hbase;
    private CommandUtil commandUtil = new CommandUtil();
    private static int MAX_RAM_QUOTA_MB = 2048;
    private static int RAM_QUOTA_INCREMENT_MB = 512;
    private static int MAX_CPU_QUOTA_PERCENT = 80;
    private static int CPU_QUOTA_INCREMENT_PERCENT = 10;


    public HBaseAlertListener( final HBaseImpl hbase )
    {
        this.hbase = hbase;
    }


    private void throwAlertException( String context, Exception e ) throws AlertException
    {
        LOG.error( context, e );
        throw new AlertException( context, e );
    }


    @Override
    public void onAlert( final ContainerHostMetric metric ) throws Exception
    {
        //find hbase cluster by environment id
        List<HBaseConfig> clusters = hbase.getClusters();

        HBaseConfig targetCluster = null;
        for ( HBaseConfig cluster : clusters )
        {
            if ( cluster.getEnvironmentId().equals( metric.getEnvironmentId() ) )
            {
                targetCluster = cluster;
                break;
            }
        }

        if ( targetCluster == null )
        {
            throwAlertException( String.format( "Cluster not found by environment id %s", metric.getEnvironmentId() ),
                    null );
            return;
        }

        //get cluster environment
        Environment environment = hbase.getEnvironmentManager().findEnvironment( metric.getEnvironmentId() );
        if ( environment == null )
        {
            throwAlertException( String.format( "Environment not found by id %s", metric.getEnvironmentId() ), null );
            return;
        }

        //get environment containers and find alert's source host
        Set<ContainerHost> containers = environment.getContainerHosts();

        ContainerHost sourceHost = null;
        for ( ContainerHost containerHost : containers )
        {
            if ( containerHost.getHostname().equalsIgnoreCase( metric.getHost() ) )
            {
                sourceHost = containerHost;
                break;
            }
        }

        if ( sourceHost == null )
        {
            throwAlertException( String.format( "Alert source host %s not found in environment", metric.getHost() ),
                    null );
            return;
        }

        //check if source host belongs to found hbase cluster
        if ( !targetCluster.getAllNodes().contains( sourceHost.getId() ) )
        {
            LOG.info( String.format( "Alert source host %s does not belong to HBase cluster", metric.getHost() ) );
            return;
        }


        List<NodeType> nodeRoles = targetCluster.getNodeRoles( targetCluster, sourceHost );

        double totalRamUsage = 0;
        double totalCpuUsage = 0;
        double redLine = 0.9;

        // confirm that Hadoop is causing the stress, otherwise no-op
        MonitoringSettings thresholds = hbase.getAlertSettings();
        double ramLimit = metric.getTotalRam() * ( thresholds.getRamAlertThreshold() / 100 );
        HashMap<NodeType, Integer> ramConsumption = new HashMap<>();
        HashMap<NodeType, Integer> cpuConsumption = new HashMap<>();

        for ( NodeType nodeType : nodeRoles )
        {
            int pid;
            switch ( nodeType )
            {
                case HMASTER:
                    CommandResult result = commandUtil.execute( Commands.getStatusCommand(), sourceHost );
                    pid = parsePid( parseService( result.getStdOut(), nodeType.name().toLowerCase() ) );
                    ProcessResourceUsage processResourceUsage =
                            hbase.getMonitor().getProcessResourceUsage( sourceHost, pid );
                    ramConsumption.put( NodeType.HMASTER, processResourceUsage.getUsedRam() );
                    cpuConsumption.put( NodeType.HMASTER, processResourceUsage.getUsedCpu() );
                    break;
                case HREGIONSERVER:
                    result = commandUtil.execute( Commands.getStatusCommand(), sourceHost );
                    pid = parsePid( parseService( result.getStdOut(), nodeType.name().toLowerCase() ) );
                    processResourceUsage = hbase.getMonitor().getProcessResourceUsage( sourceHost, pid );
                    ramConsumption.put( NodeType.HREGIONSERVER, processResourceUsage.getUsedRam() );
                    cpuConsumption.put( NodeType.HREGIONSERVER, processResourceUsage.getUsedCpu() );
                    break;
                case HQUORUMPEER:
                    result = commandUtil.execute( Commands.getStatusCommand(), sourceHost );
                    pid = parsePid( parseService( result.getStdOut(), nodeType.name().toLowerCase() ) );
                    processResourceUsage = hbase.getMonitor().getProcessResourceUsage( sourceHost, pid );
                    ramConsumption.put( NodeType.HQUORUMPEER, processResourceUsage.getUsedRam() );
                    cpuConsumption.put( NodeType.HQUORUMPEER, processResourceUsage.getUsedCpu() );
                    break;
            }
        }

        for ( NodeType nodeType : nodeRoles )
        {
            totalRamUsage = +ramConsumption.get( nodeType );
            totalCpuUsage = +cpuConsumption.get( nodeType );
        }


        boolean isCPUStressedByHBase = false;
        boolean isRAMStressedByHBase = false;

        if ( totalRamUsage >= ramLimit * redLine )
        {
            isRAMStressedByHBase = true;
        }
        else if ( totalCpuUsage >= thresholds.getCpuAlertThreshold() * redLine )
        {
            isCPUStressedByHBase = true;
        }

        if ( !( isRAMStressedByHBase || isCPUStressedByHBase ) )
        {
            LOG.info( "HBase cluster ok" );
            return;
        }


        //auto-scaling is enabled -> scale cluster
        if ( targetCluster.isAutoScaling() )
        {
            // check if a quota limit increase does it
            boolean quotaIncreased = false;

            if ( isRAMStressedByHBase )
            {
                //read current RAM quota
                int ramQuota = sourceHost.getRamQuota();


                if ( ramQuota < MAX_RAM_QUOTA_MB )
                {
                    //we can increase RAM quota
                    sourceHost.setRamQuota( Math.min( MAX_RAM_QUOTA_MB, ramQuota + RAM_QUOTA_INCREMENT_MB ) );

                    quotaIncreased = true;
                }
            }
            else if ( isCPUStressedByHBase )
            {

                //read current CPU quota
                int cpuQuota = sourceHost.getCpuQuota();

                if ( cpuQuota < MAX_CPU_QUOTA_PERCENT )
                {
                    //we can increase CPU quota
                    sourceHost.setCpuQuota( Math.min( MAX_CPU_QUOTA_PERCENT, cpuQuota + CPU_QUOTA_INCREMENT_PERCENT ) );

                    quotaIncreased = true;
                }
            }

            //quota increase is made, return
            if ( quotaIncreased )
            {
                return;
            }

            // add new node
            HadoopClusterConfig hadoopClusterConfig =
                    hbase.getHadoopManager().getCluster( targetCluster.getHadoopClusterName() );
            if ( hadoopClusterConfig == null )
            {
                throwAlertException(
                        String.format( "Hadoop cluster %s not found", targetCluster.getHadoopClusterName() ), null );
            }

            List<UUID> availableNodes = hadoopClusterConfig.getAllNodes();
            availableNodes.removeAll( targetCluster.getAllNodes() );

            // no available nodes
            if ( availableNodes.isEmpty() )
            {
                notifyUser();
            }
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
                    throwAlertException(
                            String.format( "Could not obtain available hadoop node from environment by id %s",
                                    newNodeId ), null );
                }

                /**
                 * If one of region servers consume most resources on machines,
                 * we can scale horizontally, otherwise do nothing.
                 *
                 * We can just add regions servers to HBase cluster:
                 * http://wiki.apache.org/hadoop/Hbase/FAQ_Operations#A8
                 */
                if ( isSourceNodeUnderStressBySlaveNodes( ramConsumption, cpuConsumption ) )
                {
                    // add new nodes to hbase cluster (horizontal scaling)
                    hbase.addNode( targetCluster.getClusterName(), NodeType.HREGIONSERVER.name() );
                }
            }
        }
    }


    private boolean isSourceNodeUnderStressBySlaveNodes( HashMap<NodeType, Integer> ramConsumption,
                                                         HashMap<NodeType, Integer> cpuConsumption )
    {
        Map.Entry<NodeType, Integer> maxEntryInRamConsumption = null;
        for ( Map.Entry<NodeType, Integer> entry : ramConsumption.entrySet() )
        {
            if ( maxEntryInRamConsumption == null
                    || entry.getValue().compareTo( maxEntryInRamConsumption.getValue() ) > 0 )
            {
                maxEntryInRamConsumption = entry;
            }
        }

        Map.Entry<NodeType, Integer> maxEntryInCPUConsumption = null;
        for ( Map.Entry<NodeType, Integer> entry : cpuConsumption.entrySet() )
        {
            if ( maxEntryInCPUConsumption == null
                    || entry.getValue().compareTo( maxEntryInCPUConsumption.getValue() ) > 0 )
            {
                maxEntryInCPUConsumption = entry;
            }
        }

        assert maxEntryInCPUConsumption != null;
        assert maxEntryInRamConsumption != null;
        if ( maxEntryInCPUConsumption.getKey().equals( NodeType.HREGIONSERVER ) || maxEntryInRamConsumption.getKey()
                                                                                                           .equals(
                                                                                                                   NodeType.HREGIONSERVER ) )
        {
            return true;
        }
        return false;
    }


    protected String parseService( String output, String target ) throws AlertException
    {
        Matcher m = Pattern.compile( "(?m)^.*$" ).matcher( output );
        if ( m.find() )
        {
            if ( m.group().toLowerCase().contains( target.toLowerCase() ) )
            {
                return m.group();
            }
        }
        else
        {
            throwAlertException( String.format( "Could not parse PID from %s", output ), null );
        }
        return null;
    }


    protected int parsePid( String output ) throws AlertException
    {
        Pattern p = Pattern.compile( "pid\\s*:\\s*(\\d+)", Pattern.CASE_INSENSITIVE );

        Matcher m = p.matcher( output );

        if ( m.find() )
        {
            return Integer.parseInt( m.group( 1 ) );
        }
        else
        {
            throwAlertException( String.format( "Could not parse PID from %s", output ), null );
        }
        return 0;
    }


    protected void notifyUser()
    {
        //TODO implement me when user identity management is complete and we can figure out user email
    }


    @Override
    public String getSubscriberId()
    {
        return HBASE_ALERT_LISTENER;
    }
}

