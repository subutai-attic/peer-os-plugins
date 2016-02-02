//package io.subutai.plugin.hbase.impl.alert;
//
//
//import java.util.HashMap;
//import java.util.List;
//import java.util.Map;
//import java.util.Set;
//import java.util.regex.Matcher;
//import java.util.regex.Pattern;
//
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import io.subutai.common.command.CommandResult;
//import io.subutai.common.command.CommandUtil;
//import io.subutai.common.environment.Environment;
//import io.subutai.common.environment.EnvironmentNotFoundException;
//import io.subutai.common.metric.ContainerHostMetric;
//import io.subutai.common.metric.ProcessResourceUsage;
//import io.subutai.common.peer.EnvironmentContainerHost;
//import io.subutai.core.metric.api.AlertListener;
//import io.subutai.core.metric.api.MonitoringSettings;
//import io.subutai.plugin.common.api.NodeType;
//import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
//import io.subutai.plugin.hbase.api.HBaseConfig;
//import io.subutai.plugin.hbase.impl.Commands;
//import io.subutai.plugin.hbase.impl.HBaseImpl;
//
//
///**
// * Node resource threshold excess alert listener
// */
//public class HBaseAlertListener implements AlertListener
//{
//    public static final String HBASE_ALERT_LISTENER = "HBASE_ALERT_LISTENER";
//    private static final Logger LOG = LoggerFactory.getLogger( HBaseAlertListener.class.getName() );
//    private static double MAX_RAM_QUOTA_MB = 1024;
//    private static int RAM_QUOTA_INCREMENT_PERCENTAGE = 25;
//    private static int MAX_CPU_QUOTA_PERCENT = 80;
//    private static int CPU_QUOTA_INCREMENT_PERCENT = 10;
//    private HBaseImpl hbase;
//    private CommandUtil commandUtil = new CommandUtil();
//
//
//    public HBaseAlertListener( final HBaseImpl hbase )
//    {
//        this.hbase = hbase;
//    }
//
//
//    @Override
//    public void onAlert( final ContainerHostMetric metric ) throws Exception
//    {
//        //find hbase cluster by environment id
//        List<HBaseConfig> clusters = hbase.getClusters();
//
//        HBaseConfig targetCluster = null;
//        for ( HBaseConfig cluster : clusters )
//        {
//            if ( cluster.getEnvironmentId().equals( metric.getEnvironmentId() ) )
//            {
//                targetCluster = cluster;
//                break;
//            }
//        }
//
//        if ( targetCluster == null )
//        {
//            throwAlertException( String.format( "Cluster not found by environment id %s", metric.getEnvironmentId() ),
//                    null );
//            return;
//        }
//
//        //get cluster environment
//        Environment environment;
//        EnvironmentContainerHost sourceHost = null;
//        Set<EnvironmentContainerHost> containers;
//        try
//        {
//            environment = hbase.getEnvironmentManager().loadEnvironment( metric.getEnvironmentId() );
//            //get environment containers and find alert's source host
//
//            containers = environment.getContainerHosts();
//
//            for ( EnvironmentContainerHost containerHost : containers )
//            {
//                if ( containerHost.getHostname().equalsIgnoreCase( metric.getHost() ) )
//                {
//                    sourceHost = containerHost;
//                    break;
//                }
//            }
//        }
//        catch ( EnvironmentNotFoundException e )
//        {
//            LOG.error( "Could not find environment with id {}", targetCluster.getEnvironmentId() );
//            return;
//        }
//
//        if ( sourceHost == null )
//        {
//            throwAlertException( String.format( "Alert source host %s not found in environment", metric.getHost() ),
//                    null );
//            return;
//        }
//
//        //check if source host belongs to found hbase cluster
//        if ( !targetCluster.getAllNodes().contains( sourceHost.getId() ) )
//        {
//            LOG.info( String.format( "Alert source host %s does not belong to HBase cluster", metric.getHost() ) );
//            return;
//        }
//
//
//        List<NodeType> nodeRoles = targetCluster.getNodeRoles( sourceHost );
//
//        double totalRamUsage = 0;
//        double totalCpuUsage = 0;
//        double redLine = 0.7;
//
//        // confirm that Hadoop is causing the stress, otherwise no-op
//        MonitoringSettings thresholds = hbase.getAlertSettings();
//        double ramLimit = metric.getTotalRam() * ( thresholds.getRamAlertThreshold() / 100 );
//        HashMap<NodeType, Integer> ramConsumption = new HashMap<>();
//        HashMap<NodeType, Integer> cpuConsumption = new HashMap<>();
//
//        for ( NodeType nodeType : nodeRoles )
//        {
//            int pid;
//            switch ( nodeType )
//            {
//                case HMASTER:
//                    CommandResult result = commandUtil.execute( Commands.getStatusCommand(), sourceHost );
//                    pid = parsePid( parseService( result.getStdOut(), nodeType.name().toLowerCase() ) );
//                    ProcessResourceUsage processResourceUsage =
//                            hbase.getMonitor().getProcessResourceUsage( sourceHost.getContainerId(), pid );
//                    ramConsumption
//                            .put( NodeType.HMASTER, ( int ) Math.floor( processResourceUsage.getUsedRam() + 0.5d ) );
//                    cpuConsumption
//                            .put( NodeType.HMASTER, ( int ) Math.floor( processResourceUsage.getUsedCpu() + 0.5d ) );
//                    break;
//                case HREGIONSERVER:
//                    result = commandUtil.execute( Commands.getStatusCommand(), sourceHost );
//                    pid = parsePid( parseService( result.getStdOut(), nodeType.name().toLowerCase() ) );
//                    processResourceUsage = hbase.getMonitor().getProcessResourceUsage( sourceHost.getContainerId(), pid );
//                    ramConsumption.put( NodeType.HREGIONSERVER,
//                            ( int ) Math.floor( processResourceUsage.getUsedRam() + 0.5d ) );
//                    cpuConsumption.put( NodeType.HREGIONSERVER,
//                            ( int ) Math.floor( processResourceUsage.getUsedCpu() + 0.5d ) );
//                    break;
//                case HQUORUMPEER:
//                    result = commandUtil.execute( Commands.getStatusCommand(), sourceHost );
//                    pid = parsePid( parseService( result.getStdOut(), nodeType.name().toLowerCase() ) );
//                    processResourceUsage = hbase.getMonitor().getProcessResourceUsage( sourceHost.getContainerId(), pid );
//                    ramConsumption.put( NodeType.HQUORUMPEER,
//                            ( int ) Math.floor( processResourceUsage.getUsedRam() + 0.5d ) );
//                    cpuConsumption.put( NodeType.HQUORUMPEER,
//                            ( int ) Math.floor( processResourceUsage.getUsedCpu() + 0.5d ) );
//                    break;
//            }
//        }
//
//        for ( NodeType nodeType : nodeRoles )
//        {
//            totalRamUsage = +ramConsumption.get( nodeType );
//            totalCpuUsage = +cpuConsumption.get( nodeType );
//        }
//
//
//        boolean isCPUStressedByHBase = false;
//        boolean isRAMStressedByHBase = false;
//
//        if ( totalRamUsage >= ramLimit * redLine )
//        {
//            isRAMStressedByHBase = true;
//        }
//        else if ( totalCpuUsage >= thresholds.getCpuAlertThreshold() * redLine )
//        {
//            isCPUStressedByHBase = true;
//        }
//
//        if ( !( isRAMStressedByHBase || isCPUStressedByHBase ) )
//        {
//            LOG.info( "HBase cluster ok" );
//            return;
//        }
//
//
//        //auto-scaling is enabled -> scale cluster
//        if ( targetCluster.isAutoScaling() )
//        {
//            // check if a quota limit increase does it
//            boolean quotaIncreased = false;
//
//            if ( isRAMStressedByHBase )
//            {
//                //read current RAM quota
////                int ramQuota = sourceHost.getRamQuota();
////
////                if ( ramQuota < MAX_RAM_QUOTA_MB )
////                {
////                    // if available quota on resource host is greater than 10 % of calculated increase amount,
////                    // increase quota, otherwise scale horizontally
////                    int newRamQuota = ramQuota * ( 100 + RAM_QUOTA_INCREMENT_PERCENTAGE ) / 100;
////                    if ( MAX_RAM_QUOTA_MB > newRamQuota )
////                    {
////
////                        LOG.info( "Increasing ram quota of {} from {} MB to {} MB.", sourceHost.getHostname(),
////                                sourceHost.getRamQuota(), newRamQuota );
////                        //we can increase RAM quota
////                        sourceHost.setRamQuota( newRamQuota );
////
////                        quotaIncreased = true;
////                    }
////                }
//            }
//
//            if ( isCPUStressedByHBase )
//            {
//
//                //read current CPU quota
////                int cpuQuota = sourceHost.getCpuQuota();
////
////                if ( cpuQuota < MAX_CPU_QUOTA_PERCENT )
////                {
////                    int newCpuQuota = Math.min( MAX_CPU_QUOTA_PERCENT, cpuQuota + CPU_QUOTA_INCREMENT_PERCENT );
////                    LOG.info( "Increasing cpu quota of {} from {}% to {}%.", sourceHost.getHostname(), cpuQuota,
////                            newCpuQuota );
////
////                    //we can increase CPU quota
////                    sourceHost.setCpuQuota( newCpuQuota );
////
////                    quotaIncreased = true;
////                }
//            }
//
//            //quota increase is made, return
//            if ( quotaIncreased )
//            {
//                return;
//            }
//
//            // add new node
//            HadoopClusterConfig hadoopClusterConfig =
//                    hbase.getHadoopManager().getCluster( targetCluster.getHadoopClusterName() );
//            if ( hadoopClusterConfig == null )
//            {
//                throwAlertException(
//                        String.format( "Hadoop cluster %s not found", targetCluster.getHadoopClusterName() ), null );
//            }
//
//            List<String> availableNodes = hadoopClusterConfig.getAllNodes();
//            availableNodes.removeAll( targetCluster.getAllNodes() );
//
//            // no available nodes
//            if ( availableNodes.isEmpty() )
//            {
//                notifyUser();
//            }
//            else
//            {
//                String newNodeId = availableNodes.iterator().next();
//                String newNodeHostName = null;
//                for ( EnvironmentContainerHost containerHost : containers )
//                {
//                    if ( containerHost.getId().equals( newNodeId ) )
//                    {
//                        newNodeHostName = containerHost.getHostname();
//                        break;
//                    }
//                }
//
//                if ( newNodeHostName == null )
//                {
//                    throwAlertException(
//                            String.format( "Could not obtain available hadoop node from environment by id %s",
//                                    newNodeId ), null );
//                }
//
//                /**
//                 * If one of region servers consume most resources on machines,
//                 * we can scale horizontally, otherwise do nothing.
//                 *
//                 * We can just add regions servers to HBase cluster:
//                 * http://wiki.apache.org/hadoop/Hbase/FAQ_Operations#A8
//                 */
//                if ( isSourceNodeUnderStressBySlaveNodes( ramConsumption, cpuConsumption ) )
//                {
//                    // add new nodes to hbase cluster (horizontal scaling)
//                    hbase.addNode( targetCluster.getClusterName(), newNodeHostName );
//                }
//            }
//        }
//    }
//
//
//    private void throwAlertException( String context, Exception e ) throws AlertException
//    {
//        LOG.error( context, e );
//        throw new AlertException( context, e );
//    }
//
//
//    private boolean isSourceNodeUnderStressBySlaveNodes( HashMap<NodeType, Integer> ramConsumption,
//                                                         HashMap<NodeType, Integer> cpuConsumption )
//    {
//        Map.Entry<NodeType, Integer> maxEntryInRamConsumption = null;
//        for ( Map.Entry<NodeType, Integer> entry : ramConsumption.entrySet() )
//        {
//            if ( maxEntryInRamConsumption == null
//                    || entry.getValue().compareTo( maxEntryInRamConsumption.getValue() ) > 0 )
//            {
//                maxEntryInRamConsumption = entry;
//            }
//        }
//
//        Map.Entry<NodeType, Integer> maxEntryInCPUConsumption = null;
//        for ( Map.Entry<NodeType, Integer> entry : cpuConsumption.entrySet() )
//        {
//            if ( maxEntryInCPUConsumption == null
//                    || entry.getValue().compareTo( maxEntryInCPUConsumption.getValue() ) > 0 )
//            {
//                maxEntryInCPUConsumption = entry;
//            }
//        }
//
//        assert maxEntryInCPUConsumption != null;
//        assert maxEntryInRamConsumption != null;
//        return maxEntryInCPUConsumption.getKey().equals( NodeType.HREGIONSERVER ) || maxEntryInRamConsumption.getKey()
//                                                                                                             .equals(
//                                                                                                                     NodeType.HREGIONSERVER );
//    }
//
//
//    protected String parseService( String output, String target )
//    {
//        String inputArray[] = output.split( "\n" );
//        for ( String part : inputArray )
//        {
//            if ( part.toLowerCase().contains( target ) )
//            {
//                return part;
//            }
//        }
//        return null;
//    }
//
//
//    protected int parsePid( String output ) throws AlertException
//    {
//        Pattern p = Pattern.compile( "pid\\s*:\\s*(\\d+)", Pattern.CASE_INSENSITIVE );
//
//        Matcher m = p.matcher( output );
//
//        if ( m.find() )
//        {
//            return Integer.parseInt( m.group( 1 ) );
//        }
//        else
//        {
//            throwAlertException( String.format( "Could not parse PID from %s", output ), null );
//        }
//        return 0;
//    }
//
//
//    protected void notifyUser()
//    {
//        //TODO implement me when user identity management is complete and we can figure out user email
//    }
//
//
//    @Override
//    public String getSubscriberId()
//    {
//        return HBASE_ALERT_LISTENER;
//    }
//}
//
