package org.safehaus.subutai.plugin.hadoop.impl.alert;


import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.safehaus.subutai.common.command.CommandResult;
import org.safehaus.subutai.common.command.RequestBuilder;
import org.safehaus.subutai.common.metric.ProcessResourceUsage;
import org.safehaus.subutai.core.environment.api.helper.Environment;
import org.safehaus.subutai.core.metric.api.AlertListener;
import org.safehaus.subutai.core.metric.api.ContainerHostMetric;
import org.safehaus.subutai.core.metric.api.MonitoringSettings;
import org.safehaus.subutai.core.peer.api.CommandUtil;
import org.safehaus.subutai.core.peer.api.ContainerHost;
import org.safehaus.subutai.plugin.common.api.NodeType;
import org.safehaus.subutai.plugin.hadoop.api.HadoopClusterConfig;
import org.safehaus.subutai.plugin.hadoop.impl.Commands;
import org.safehaus.subutai.plugin.hadoop.impl.HadoopImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Node resource threshold excess alert listener
 */
public class HadoopAlertListener implements AlertListener
{
    private static final Logger LOG = LoggerFactory.getLogger( HadoopAlertListener.class.getName() );

    public static final String HADOOP_ALERT_LISTENER = "HADOOP_ALERT_LISTENER";
    private HadoopImpl hadoop;
    private CommandUtil commandUtil = new CommandUtil();
    private static int MAX_RAM_QUOTA_MB = 2048;
    private static int RAM_QUOTA_INCREMENT_MB = 512;
    private static int MAX_CPU_QUOTA_PERCENT = 80;
    private static int CPU_QUOTA_INCREMENT_PERCENT = 10;


    public HadoopAlertListener( final HadoopImpl hadoop )
    {
        this.hadoop = hadoop;
    }


    private void throwAlertException( String context, Exception e ) throws AlertException
    {
        LOG.error( context, e );
        throw new AlertException( context, e );
    }


    @Override
    public void onAlert( final ContainerHostMetric metric ) throws Exception
    {
        //find hadoop cluster by environment id
        List<HadoopClusterConfig> clusters = hadoop.getClusters();

        HadoopClusterConfig targetCluster = null;
        for ( HadoopClusterConfig cluster : clusters )
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
        }

        //get cluster environment
        Environment environment = hadoop.getEnvironmentManager().getEnvironmentByUUID( metric.getEnvironmentId() );
        if ( environment == null )
        {
            throwAlertException( String.format( "Environment not found by id %s", metric.getEnvironmentId() ), null );
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
        }

        //check if source host belongs to found hadoop cluster
        if ( !targetCluster.getAllNodes().contains( sourceHost.getId() ) )
        {
            LOG.info( String.format( "Alert source host %s does not belong to Hadoop cluster", metric.getHost() ) );
            return;
        }


        List<NodeType> nodeRoles = HadoopClusterConfig.getNodeRoles( targetCluster, sourceHost );

        double totalRamUsage = 0;
        double totalCpuUsage = 0;
        double redLine = 0.9;

        // confirm that Hadoop is causing the stress, otherwise no-op
        MonitoringSettings thresholds = hadoop.getAlertSettings();
        double ramLimit = metric.getTotalRam() * ( thresholds.getRamAlertThreshold() / 100 );
        HashMap<NodeType, Integer> ramConsumption = new HashMap<>();
        HashMap<NodeType, Integer> cpuConsumption = new HashMap<>();

        for ( NodeType nodeType : nodeRoles ){
            int pid;
            switch ( nodeType ){
                case NAMENODE:
                    CommandResult result = commandUtil.execute( new RequestBuilder( Commands.getStatusNameNodeCommand() ), sourceHost );
                    pid = parsePid( parseService( result.getStdOut() , nodeType.name().toLowerCase() ) );
                    ProcessResourceUsage processResourceUsage = sourceHost.getProcessResourceUsage( pid );
                    ramConsumption.put( NodeType.NAMENODE, processResourceUsage.getUsedRam() );
                    cpuConsumption.put( NodeType.NAMENODE, processResourceUsage.getUsedCpu() );
                    break;
                case SECONDARY_NAMENODE:
                    result = commandUtil.execute( new RequestBuilder( Commands.getStartNameNodeCommand() ), sourceHost );
                    pid = parsePid( parseService( result.getStdOut() , nodeType.name().toLowerCase() ) );
                    processResourceUsage = sourceHost.getProcessResourceUsage( pid );
                    ramConsumption.put( NodeType.SECONDARY_NAMENODE, processResourceUsage.getUsedRam() );
                    cpuConsumption.put( NodeType.SECONDARY_NAMENODE, processResourceUsage.getUsedCpu() );
                    break;
                case JOBTRACKER:
                    result = commandUtil.execute( new RequestBuilder( Commands.getStatusJobTrackerCommand() ), sourceHost );
                    pid = parsePid( parseService( result.getStdOut() , nodeType.name().toLowerCase() ) );
                    processResourceUsage = sourceHost.getProcessResourceUsage( pid );
                    ramConsumption.put( NodeType.JOBTRACKER, processResourceUsage.getUsedRam() );
                    cpuConsumption.put( NodeType.JOBTRACKER, processResourceUsage.getUsedCpu() );
                    break;
                case DATANODE:
                    result = commandUtil.execute( new RequestBuilder( Commands.getStatusDataNodeCommand() ), sourceHost );
                    pid = parsePid( parseService( result.getStdOut() , nodeType.name().toLowerCase() ) );
                    processResourceUsage = sourceHost.getProcessResourceUsage( pid );
                    ramConsumption.put( NodeType.DATANODE, processResourceUsage.getUsedRam() );
                    cpuConsumption.put( NodeType.DATANODE, processResourceUsage.getUsedCpu() );
                case TASKTRACKER:
                    result = commandUtil.execute( new RequestBuilder( Commands.getStatusTaskTrackerCommand() ), sourceHost );
                    pid = parsePid( parseService( result.getStdOut() , nodeType.name().toLowerCase() ) );
                    processResourceUsage = sourceHost.getProcessResourceUsage( pid );
                    ramConsumption.put( NodeType.TASKTRACKER, processResourceUsage.getUsedRam() );
                    cpuConsumption.put( NodeType.TASKTRACKER, processResourceUsage.getUsedCpu() );
                    break;
            }
        }

        for ( NodeType nodeType : nodeRoles ){
            totalRamUsage =+ ramConsumption.get( nodeType );
            totalCpuUsage =+ cpuConsumption.get( nodeType );
        }


        boolean isCPUStressedByHadoop = false;
        boolean isRAMStressedByHadoop = false;

        if ( totalRamUsage >= ramLimit * redLine )
        {
            isRAMStressedByHadoop = true;
        }
        else if ( totalCpuUsage >= thresholds.getCpuAlertThreshold() * redLine )
        {
            isCPUStressedByHadoop = true;
        }

        if ( !( isRAMStressedByHadoop || isCPUStressedByHadoop ) )
        {
            LOG.info( "Hadoop cluster ok" );
            return;
        }

        /**
         * after this point, we found out source node is under stress, we need to either
         * scale vertically ( increase available sources) or scale horizontally ( add new nodes to cluster)
         *
         * Since in hadoop master nodes cannot be scaled horizontally ( Hadoop can just have one NameNode, one JobTracker,
         * one SecondaryNameNode ), we should scale master nodes vertically. However we can scale out slave
         * nodes (DataNode and TaskTracker) horizontally.
         *
         * Vertical scaling have more priority to Horizontal scaling.
         */

        //auto-scaling is enabled -> scale cluster
        if ( targetCluster.isAutoScaling() )
        {
            // check if a quota limit increase does it
            boolean quotaIncreased = false;

            if ( isRAMStressedByHadoop )
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
            else if ( isCPUStressedByHadoop )
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

            /**
             * If one of slave nodes uses consume most resources on machines,
             * we can scale horizontally, otherwise do nothing.
             */
            if ( isSourceNodeUnderStressBySlaveNodes( ramConsumption, cpuConsumption, targetCluster ) ){
                // add new nodes to hadoop cluster (horizontal scaling)
                hadoop.addNode( targetCluster.getClusterName() );
            }
        }
        else
        {
            notifyUser();
        }
    }


    private boolean isSourceNodeUnderStressBySlaveNodes( HashMap<NodeType, Integer> ramConsumption,
                                                   HashMap<NodeType, Integer> cpuConsumption, HadoopClusterConfig targetCluster ){
        Map.Entry<NodeType, Integer> maxEntryInRamConsumption = null;
        for ( Map.Entry<NodeType, Integer> entry : ramConsumption.entrySet() ){
            if (maxEntryInRamConsumption == null || entry.getValue().compareTo( maxEntryInRamConsumption.getValue() ) > 0)
            {
                maxEntryInRamConsumption = entry;
            }
        }

        Map.Entry<NodeType, Integer> maxEntryInCPUConsumption = null;
        for ( Map.Entry<NodeType, Integer> entry : cpuConsumption.entrySet() ){
            if (maxEntryInCPUConsumption == null || entry.getValue().compareTo( maxEntryInCPUConsumption.getValue() ) > 0)
            {
                maxEntryInCPUConsumption = entry;
            }
        }

        assert maxEntryInCPUConsumption != null;
        assert maxEntryInRamConsumption != null;
        if ( maxEntryInCPUConsumption.getKey().equals( NodeType.DATANODE ) || maxEntryInCPUConsumption.getKey().equals( NodeType.TASKTRACKER ) ||
                maxEntryInRamConsumption.getKey().equals( NodeType.DATANODE ) || maxEntryInRamConsumption.getKey().equals( NodeType.TASKTRACKER ) ){
            return true;
        }
        return false;
    }


    protected String parseService( String output, String target ) throws AlertException
    {
        Matcher m = Pattern.compile("(?m)^.*$").matcher( output );
        if ( m.find() ){
            if ( m.group().toLowerCase().contains( target.toLowerCase() ) ){
                return m.group();
            }
        }
        else{
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
        return HADOOP_ALERT_LISTENER;
    }
}

