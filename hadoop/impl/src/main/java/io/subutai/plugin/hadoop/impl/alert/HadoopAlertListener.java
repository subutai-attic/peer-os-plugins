package io.subutai.plugin.hadoop.impl.alert;


import java.math.BigDecimal;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.CommandUtil;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.environment.Environment;
import io.subutai.common.metric.ProcessResourceUsage;
import io.subutai.common.metric.QuotaAlertValue;
import io.subutai.common.peer.AlertHandlerException;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.peer.ExceededQuotaAlertHandler;
import io.subutai.common.peer.PeerException;
import io.subutai.common.quota.ContainerCpuResource;
import io.subutai.common.quota.ContainerQuota;
import io.subutai.common.quota.ContainerRamResource;
import io.subutai.common.quota.Quota;
import io.subutai.common.resource.ByteUnit;
import io.subutai.common.resource.ByteValueResource;
import io.subutai.common.resource.ContainerResourceType;
import io.subutai.common.resource.NumericValueResource;
import io.subutai.core.metric.api.MonitoringSettings;
import io.subutai.core.plugincommon.api.NodeType;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.hadoop.impl.Commands;
import io.subutai.plugin.hadoop.impl.HadoopImpl;


/**
 * Node resource threshold excess alert listener
 */
public class HadoopAlertListener extends ExceededQuotaAlertHandler
{
    private static final Logger LOG = LoggerFactory.getLogger( HadoopAlertListener.class );
    private static final String HANDLER_ID = "DEFAULT_HADOOP_QUOTA_EXCEEDED_ALERT_HANDLER";
    private HadoopImpl hadoop;
    private CommandUtil commandUtil = new CommandUtil();

    private static double MAX_RAM_QUOTA_MB;
    private static int RAM_QUOTA_INCREMENT_PERCENTAGE = 25;
    private static int MAX_CPU_QUOTA_PERCENT = 80;
    private static int CPU_QUOTA_INCREMENT_PERCENT = 10;
    private static int PHYSICAL_MACHINE_RESERVED_RAM_CAPACITY_IN_MB = 2048;

    private static final String PID_STRING = "pid";


    public HadoopAlertListener( final HadoopImpl hadoop )
    {
        this.hadoop = hadoop;
    }


    private void throwAlertException( String context, Exception e ) throws AlertHandlerException
    {
        LOG.error( context, e );
        throw new AlertHandlerException( context, e );
    }


    @Override
    public String getId()
    {
        return HANDLER_ID;
    }


    @Override
    public String getDescription()
    {
        return "Node resource threshold excess default alert handler for hadoop.";
    }


    @Override
    public void process( final Environment environment, final QuotaAlertValue alertValue ) throws AlertHandlerException
    {
        //find hadoop cluster by environment id
        List<HadoopClusterConfig> clusters = hadoop.getClusters();

        String environmentId = environment.getId();
        String sourceHostId = alertValue.getValue().getHostId().getId();

        HadoopClusterConfig targetCluster = null;
        for ( HadoopClusterConfig cluster : clusters )
        {
            if ( cluster.getEnvironmentId().equals( environmentId ) )
            {
                targetCluster = cluster;
                break;
            }
        }

        if ( targetCluster == null )
        {
            throwAlertException( String.format( "Cluster not found by environment id %s", environmentId ), null );
            return;
        }

        EnvironmentContainerHost sourceHost = null;

        //get environment containers and find alert's source host
        Set<EnvironmentContainerHost> containers = environment.getContainerHosts();

        for ( EnvironmentContainerHost containerHost : containers )
        {
            if ( containerHost.getId().equalsIgnoreCase( sourceHostId ) )
            {
                sourceHost = containerHost;
                break;
            }
        }


        if ( sourceHost == null )
        {
            throwAlertException( String.format( "Alert source host %s not found in environment",
                    alertValue.getValue().getHostId().getId() ), null );
            return;
        }

        //check if source host belongs to found hadoop cluster
        if ( !targetCluster.getAllNodes().contains( sourceHost.getId() ) )
        {
            LOG.info( String.format( "Alert source host %s does not belong to Hadoop cluster",
                    alertValue.getValue().getHostId() ) );
            return;
        }

        // Set 50 percent of the available ram capacity of the resource host
        // to maximum ram quota limit assignable to the container
        //        @todo quote expection
        //        final double MAX_RAM_QUOTA_MB = sourceHost.getAvailableRamQuota().getRamQuotaValue( RamQuotaUnit.MB
        // ) * 0.5;

        List<NodeType> nodeRoles = HadoopClusterConfig.getNodeRoles( targetCluster, sourceHost );

        double totalRamUsage = 0;
        double totalCpuUsage = 0;
        double redLine = 0.5;

        // confirm that Hadoop is causing the stress, otherwise no-op
        MonitoringSettings thresholds = hadoop.getAlertSettings();
        //TODO: check total ram usage
        final double currentRam =
                ( ( BigDecimal ) ( alertValue.getValue().getCurrentValue().getValue() ) ).doubleValue();
        double ramLimit = currentRam * ( ( double ) thresholds.getRamAlertThreshold() / 100 );
        HashMap<NodeType, Double> ramConsumption = new HashMap<>();
        HashMap<NodeType, Double> cpuConsumption = new HashMap<>();
        try
        {
            for ( NodeType nodeType : nodeRoles )
            {
                int pid;
                switch ( nodeType )
                {
                    case NAMENODE:
//                        CommandResult result = commandUtil
//                                .execute( new RequestBuilder( Commands.getStatusNameNodeCommand() ).withTimeout( 60 ),
//                                        sourceHost );
//                        String output = parseService( result.getStdOut(), nodeType.name().toLowerCase() );
//                        if ( !output.toLowerCase().contains( PID_STRING ) )
//                        {
//                            break;
//                        }
//                        pid = parsePid( output );
//                        ProcessResourceUsage processResourceUsage = sourceHost.getProcessResourceUsage( pid );
//                        ramConsumption.put( NodeType.NAMENODE, processResourceUsage.getUsedRam() );
//                        cpuConsumption.put( NodeType.NAMENODE, processResourceUsage.getUsedCpu() );
//                        break;
//                    case SECONDARY_NAMENODE:
//                        result = commandUtil
//                                .execute( new RequestBuilder( Commands.getStatusNameNodeCommand() ).withTimeout( 60 ),
//                                        sourceHost );
//                        output = parseService( result.getStdOut(), "secondarynamenode" );
//                        if ( !output.toLowerCase().contains( PID_STRING ) )
//                        {
//                            break;
//                        }
//                        pid = parsePid( output );
//                        processResourceUsage = sourceHost.getProcessResourceUsage( pid );
//                        ramConsumption.put( NodeType.SECONDARY_NAMENODE, processResourceUsage.getUsedRam() );
//                        cpuConsumption.put( NodeType.SECONDARY_NAMENODE, processResourceUsage.getUsedCpu() );
//                        break;
//                    case JOBTRACKER:
//                        result = commandUtil
//                                .execute( new RequestBuilder( Commands.getStatusJobTrackerCommand() ).withTimeout( 60 ),
//                                        sourceHost );
//                        output = parseService( result.getStdOut(), nodeType.name().toLowerCase() );
//                        if ( !output.toLowerCase().contains( PID_STRING ) )
//                        {
//                            break;
//                        }
//                        pid = parsePid( output );
//                        processResourceUsage = sourceHost.getProcessResourceUsage( pid );
//                        ramConsumption.put( NodeType.JOBTRACKER, processResourceUsage.getUsedRam() );
//                        cpuConsumption.put( NodeType.JOBTRACKER, processResourceUsage.getUsedCpu() );
//                        break;
//                    case DATANODE:
//                        result = commandUtil
//                                .execute( new RequestBuilder( Commands.getStatusDataNodeCommand() ).withTimeout( 60 ),
//                                        sourceHost );
//                        output = parseService( result.getStdOut(), nodeType.name().toLowerCase() );
//                        if ( !output.toLowerCase().contains( PID_STRING ) )
//                        {
//                            break;
//                        }
//                        pid = parsePid( output );
//                        processResourceUsage = sourceHost.getProcessResourceUsage( pid );
//                        ramConsumption.put( NodeType.DATANODE, processResourceUsage.getUsedRam() );
//                        cpuConsumption.put( NodeType.DATANODE, processResourceUsage.getUsedCpu() );
//                        break;
//                    case TASKTRACKER:
//                        result = commandUtil.execute(
//                                new RequestBuilder( Commands.getStatusTaskTrackerCommand() ).withTimeout( 60 ),
//                                sourceHost );
//                        output = parseService( result.getStdOut(), nodeType.name().toLowerCase() );
//                        if ( !output.toLowerCase().contains( PID_STRING ) )
//                        {
//                            break;
//                        }
//                        pid = parsePid( output );
//                        processResourceUsage = sourceHost.getProcessResourceUsage( pid );
//                        ramConsumption.put( NodeType.TASKTRACKER, processResourceUsage.getUsedRam() );
//                        cpuConsumption.put( NodeType.TASKTRACKER, processResourceUsage.getUsedCpu() );
                        break;
                }
            }


            for ( NodeType nodeType : nodeRoles )
            {
                if ( ramConsumption.get( nodeType ) != null )
                {
                    totalRamUsage += ramConsumption.get( nodeType );
                }
                if ( cpuConsumption.get( nodeType ) != null )
                {
                    totalCpuUsage += cpuConsumption.get( nodeType );
                }
            }


            boolean isCPUStressedByHadoop = false;
            boolean isRAMStressedByHadoop = false;

            if ( totalRamUsage >= ramLimit * redLine )
            {
                isRAMStressedByHadoop = true;
            }

            if ( totalCpuUsage >= thresholds.getCpuAlertThreshold() * redLine )
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
             * Since in hadoop master nodes cannot be scaled horizontally ( Hadoop can just have one NameNode, one
             * JobTracker, one SecondaryNameNode ), we should scale master nodes vertically. However we can scale
             * out slave nodes (DataNode and TaskTracker) horizontally.
             *
             * Vertical scaling have more priority to Horizontal scaling.
             */

            //auto-scaling is enabled -> scale cluster
            if ( targetCluster.isAutoScaling() )
            {
                // check if a quota limit increase does it
                boolean quotaIncreased = false;

                //            @todo quota exception
                if ( isRAMStressedByHadoop )
                {
                    //read current RAM quota
                    ContainerQuota containerQuota = sourceHost.getQuota();
                    double ramQuota = containerQuota.get( ContainerResourceType.RAM ).getAsRamResource().getResource().getValue( ByteUnit.MB ).doubleValue();

                    if ( ramQuota < MAX_RAM_QUOTA_MB )
                    {

                        // if available quota on resource host is greater than 10 % of calculated increase amount,
                        // increase quota, otherwise scale horizontally
                        double newRamQuota = ramQuota * ( 100 + RAM_QUOTA_INCREMENT_PERCENTAGE ) / 100;
                        if ( MAX_RAM_QUOTA_MB > newRamQuota )
                        {

                            LOG.info( "Increasing ram quota of {} from {} MB to {} MB.", sourceHost.getHostname(),
                                    ramQuota, newRamQuota );
                            //we can increase RAM quota
                            Quota quota = new Quota( new ContainerRamResource( new ByteValueResource(
                                    ByteValueResource.toBytes( new BigDecimal( newRamQuota ), ByteUnit.MB ) ) ),
                                    thresholds.getRamAlertThreshold() );

                            containerQuota.add( quota );

                            sourceHost.setQuota( containerQuota );

                            quotaIncreased = true;
                        }
                    }
                }

                if ( isCPUStressedByHadoop )
                {
                    //read current CPU quota
                    //                    ResourceValue cpuQuota = sourceHost.getQuota( ResourceType.CPU );
                    final ContainerQuota containerQuota = sourceHost.getQuota();
                    ContainerCpuResource cpuQuota = containerQuota.get( ContainerResourceType.CPU ).getAsCpuResource();

                    if ( cpuQuota.getResource().getValue().intValue() < MAX_CPU_QUOTA_PERCENT )
                    {
                        int newCpuQuota = Math.min( MAX_CPU_QUOTA_PERCENT,
                                cpuQuota.getResource().getValue().intValue() + CPU_QUOTA_INCREMENT_PERCENT );
                        LOG.info( "Increasing cpu quota of {} from {}% to {}%.", sourceHost.getHostname(),
                                cpuQuota.getResource().getValue().intValue(), newCpuQuota );
                        //we can increase CPU quota

                        Quota quota = new Quota( new ContainerCpuResource( new ByteValueResource(
                                ByteValueResource.toBytes( new BigDecimal( newCpuQuota ), ByteUnit.MB ) ) ),
                                thresholds.getRamAlertThreshold() );

                        containerQuota.add( quota );

                        sourceHost.setQuota( containerQuota );

                        quotaIncreased = true;
                    }
                }

                //quota increase is made, return
                if ( quotaIncreased )
                {
                    // TODO remove the following line when testing is finished
                    hadoop.addNode( targetCluster.getClusterName() );
                    return;
                }

                /**
                 * If one of slave nodes uses consume most resources on machines,
                 * we can scale horizontally, otherwise do nothing.
                 */
                if ( isSourceNodeUnderStressBySlaveNodes( ramConsumption, cpuConsumption, targetCluster ) )
                {
                    // add new nodes to hadoop cluster (horizontal scaling)
                    hadoop.addNode( targetCluster.getClusterName() );
                }
            }
            else
            {
                notifyUser();
            }
        }
        catch ( PeerException e )
        {
            throwAlertException( e.getMessage(), e );
        }
    }


    private boolean isSourceNodeUnderStressBySlaveNodes( HashMap<NodeType, Double> ramConsumption,
                                                         HashMap<NodeType, Double> cpuConsumption,
                                                         HadoopClusterConfig targetCluster )
    {
        Map.Entry<NodeType, Double> maxEntryInRamConsumption = null;
        for ( Map.Entry<NodeType, Double> entry : ramConsumption.entrySet() )
        {
            if ( maxEntryInRamConsumption == null
                    || entry.getValue().compareTo( maxEntryInRamConsumption.getValue() ) > 0 )
            {
                maxEntryInRamConsumption = entry;
            }
        }

        Map.Entry<NodeType, Double> maxEntryInCPUConsumption = null;
        for ( Map.Entry<NodeType, Double> entry : cpuConsumption.entrySet() )
        {
            if ( maxEntryInCPUConsumption == null
                    || entry.getValue().compareTo( maxEntryInCPUConsumption.getValue() ) > 0 )
            {
                maxEntryInCPUConsumption = entry;
            }
        }

        assert maxEntryInCPUConsumption != null;
        assert maxEntryInRamConsumption != null;
        return maxEntryInCPUConsumption.getKey().equals( NodeType.DATANODE ) ||
                maxEntryInCPUConsumption.getKey().equals( NodeType.TASKTRACKER ) ||
                maxEntryInRamConsumption.getKey().equals( NodeType.DATANODE ) ||
                maxEntryInRamConsumption.getKey().equals( NodeType.TASKTRACKER );
    }


    protected String parseService( String output, String target )
    {
        String inputArray[] = output.split( "\n" );
        for ( String part : inputArray )
        {
            if ( part.toLowerCase().contains( target ) )
            {
                return part;
            }
        }
        return null;
    }


    protected int parsePid( String output ) throws AlertHandlerException
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
}

