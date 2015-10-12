package io.subutai.plugin.accumulo.impl.alert;


import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.subutai.common.command.CommandResult;
import io.subutai.common.command.CommandUtil;
import io.subutai.common.environment.Environment;
import io.subutai.common.metric.ContainerHostMetric;
import io.subutai.common.metric.ProcessResourceUsage;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.core.metric.api.AlertListener;
import io.subutai.core.metric.api.MonitoringSettings;
import io.subutai.plugin.accumulo.api.AccumuloClusterConfig;
import io.subutai.plugin.accumulo.impl.AccumuloImpl;
import io.subutai.plugin.accumulo.impl.Commands;
import io.subutai.plugin.common.api.NodeType;


public class AccumuloAlertListener implements AlertListener
{
    private static final Logger LOG = LoggerFactory.getLogger( AccumuloAlertListener.class.getName() );
    public static final String ACCUMOLO_ALERT_LISTENER = "ACCUMOLO_ALERT_LISTENER";
    private static final Logger LOGGER = LoggerFactory.getLogger( AccumuloAlertListener.class );
    private static final String PID_STRING = "pid";
    private static double MAX_RAM_QUOTA_MB;
    private static int RAM_QUOTA_INCREMENT_PERCENTAGE = 25;
    private static int MAX_CPU_QUOTA_PERCENT = 80;
    private static int CPU_QUOTA_INCREMENT_PERCENT = 10;
    private AccumuloImpl accumulo;
    private CommandUtil commandUtil = new CommandUtil();


    public AccumuloAlertListener( final AccumuloImpl accumulo )
    {
        this.accumulo = accumulo;
    }


    @Override
    public void onAlert( final ContainerHostMetric containerHostMetric ) throws Exception
    {
        //find accumulo cluster by environment id
        List<AccumuloClusterConfig> clusters = accumulo.getClusters();

        AccumuloClusterConfig targetCluster = null;
        for ( AccumuloClusterConfig cluster : clusters )
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
                accumulo.getEnvironmentManager().loadEnvironment( containerHostMetric.getEnvironmentId() );
        if ( environment == null )
        {
            throw new Exception(
                    String.format( "Environment not found by id %s", containerHostMetric.getEnvironmentId() ), null );
        }

        //get environment containers and find alert source host
        Set<EnvironmentContainerHost> containers = environment.getContainerHosts();

        EnvironmentContainerHost sourceHost = null;
        for ( EnvironmentContainerHost containerHost : containers )
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

        //check if source host belongs to found accumulo cluster
        if ( !targetCluster.getAllNodes().contains( sourceHost.getId() ) )
        {
            LOGGER.info( String.format( "Alert source host %s does not belong to accumulo cluster",
                    containerHostMetric.getHost() ) );
            return;
        }

        // Set 80 percent of the available ram capacity of the resource host
        // to maximum ram quota limit assignable to the container
        MAX_RAM_QUOTA_MB = sourceHost.getAvailableRamQuota() * 0.8;

        double totalRamUsage = 0;
        double totalCpuUsage = 0;
        double redLine = 0.7;

        List<NodeType> nodeRoles = targetCluster.getNodeRoles( sourceHost.getId() );
        HashMap<NodeType, Double> ramConsumption = new HashMap<>();
        HashMap<NodeType, Double> cpuConsumption = new HashMap<>();

        for ( NodeType nodeType : nodeRoles )
        {
            int pid = 0;
            switch ( nodeType )
            {
                case ACCUMULO_MASTER:
                    CommandResult result = commandUtil.execute( Commands.statusCommand, sourceHost );
                    String output = parseService( result.getStdOut(), convertEnumValues( nodeType ) );
                    if ( !output.toLowerCase().contains( PID_STRING ) )
                    {
                        break;
                    }
                    pid = parsePid( output );
                    break;
                case ACCUMULO_MONITOR:
                    result = commandUtil.execute( Commands.statusCommand, sourceHost );
                    output = parseService( result.getStdOut(), convertEnumValues( nodeType ) );
                    if ( !output.toLowerCase().contains( PID_STRING ) )
                    {
                        break;
                    }
                    pid = parsePid( output );
                    break;
                case ACCUMULO_GC:
                    result = commandUtil.execute( Commands.statusCommand, sourceHost );
                    output = parseService( result.getStdOut(), convertEnumValues( nodeType ) );
                    if ( !output.toLowerCase().contains( PID_STRING ) )
                    {
                        break;
                    }
                    pid = parsePid( output );
                    break;
                case ACCUMULO_TRACER:
                    result = commandUtil.execute( Commands.statusCommand, sourceHost );
                    output = parseService( result.getStdOut(), convertEnumValues( nodeType ) );
                    if ( !output.toLowerCase().contains( PID_STRING ) )
                    {
                        break;
                    }
                    pid = parsePid( output );
                case ACCUMULO_TABLET_SERVER:
                    result = commandUtil.execute( Commands.statusCommand, sourceHost );
                    output = parseService( result.getStdOut(), convertEnumValues( nodeType ) );
                    if ( !output.toLowerCase().contains( PID_STRING ) )
                    {
                        break;
                    }
                    pid = parsePid( output );
                    break;
            }
            if ( pid != 0 )
            {
                ProcessResourceUsage processResourceUsage = sourceHost.getProcessResourceUsage( pid );
                ramConsumption.put( nodeType, processResourceUsage.getUsedRam() );
                cpuConsumption.put( nodeType, processResourceUsage.getUsedCpu() );
            }
        }

        for ( NodeType nodeType : nodeRoles )
        {
            if ( ramConsumption.get( nodeType ) != null )
            {
                totalRamUsage = +ramConsumption.get( nodeType );
            }
            if ( cpuConsumption.get( nodeType ) != null )
            {
                totalCpuUsage = +cpuConsumption.get( nodeType );
            }
        }


        //confirm that accumulo is causing the stress, otherwise no-op
        MonitoringSettings thresholds = accumulo.getAlertSettings();
        double ramLimit =
                containerHostMetric.getTotalRam() * ( ( double ) thresholds.getRamAlertThreshold() / 100 ); // 0.8
        boolean cpuStressedByAccumulo = false;
        boolean ramStressedByAccumulo = false;

        if ( totalRamUsage >= ramLimit * redLine )
        {
            ramStressedByAccumulo = true;
        }

        if ( totalCpuUsage >= thresholds.getCpuAlertThreshold() * redLine )
        {
            cpuStressedByAccumulo = true;
        }

        if ( !( ramStressedByAccumulo || cpuStressedByAccumulo ) )
        {
            LOGGER.info( "Accumulo cluster runs ok" );
            return;
        }


        //auto-scaling is enabled -> scale cluster
        if ( targetCluster.isAutoScaling() )
        {
            // check if a quota limit increase does it
            boolean quotaIncreased = false;

            if ( ramStressedByAccumulo )
            {
                //read current RAM quota
                int ramQuota = accumulo.getQuotaManager().getRamQuota( sourceHost.getId() );

                if ( ramQuota < MAX_RAM_QUOTA_MB )
                {
                    // if available quota on resource host is greater than 10 % of calculated increase amount,
                    // increase quota, otherwise scale horizontally
                    int newRamQuota = ramQuota * ( 100 + RAM_QUOTA_INCREMENT_PERCENTAGE ) / 100;
                    if ( MAX_RAM_QUOTA_MB > newRamQuota )
                    {

                        LOG.info( "Increasing ram quota of {} from {} MB to {} MB.", sourceHost.getHostname(),
                                sourceHost.getRamQuota(), newRamQuota );
                        //we can increase RAM quota
                        sourceHost.setRamQuota( newRamQuota );

                        quotaIncreased = true;
                    }
                }
            }

            if ( cpuStressedByAccumulo )
            {

                //read current CPU quota
                int cpuQuota = accumulo.getQuotaManager().getCpuQuota( sourceHost.getId() );

                if ( cpuQuota < MAX_CPU_QUOTA_PERCENT )
                {
                    int newCpuQuota = Math.min( MAX_CPU_QUOTA_PERCENT, cpuQuota + CPU_QUOTA_INCREMENT_PERCENT );
                    LOG.info( "Increasing cpu quota of {} from {}% to {}%.", sourceHost.getHostname(), cpuQuota,
                            newCpuQuota );

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
            AccumuloClusterConfig accumuloClusterConfig = accumulo.getCluster( targetCluster.getClusterName() );
            if ( accumuloClusterConfig == null )
            {
                throw new Exception( String.format( "Accumulo cluster %s not found", targetCluster.getClusterName() ),
                        null );
            }

            //Get nodes which are already configured in hadoop and zookeeper clusters
            Set<String> hadoopNodes = new TreeSet<>(
                    accumulo.getHadoopManager().getCluster( targetCluster.getHadoopClusterName() ).getAllNodes() );
            Set<String> zookeeperNodes =
                    accumulo.getZkManager().getCluster( targetCluster.getZookeeperClusterName() ).getNodes();

            Set<EnvironmentContainerHost> environmentHosts = environment.getContainerHosts();
            Set<String> availableNodes = new TreeSet<>();
            for ( final EnvironmentContainerHost environmentHost : environmentHosts )
            {
                if ( hadoopNodes.contains( environmentHost.getId() ) && zookeeperNodes
                        .contains( environmentHost.getId() ) )
                {
                    availableNodes.add( environmentHost.getId() );
                }
            }
            availableNodes.removeAll( targetCluster.getAllNodes() );

            //no available nodes -> notify user
            if ( availableNodes.isEmpty() )
            {
                notifyUser();
            }
            //add first available node
            else
            {
                String newNodeId = availableNodes.iterator().next();
                String newNodeHostName = null;
                for ( EnvironmentContainerHost containerHost : containers )
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
                            String.format( "Could not obtain available Accumulo node from environment by id %s",
                                    newNodeId ), null );
                }

                //launch node addition process
                accumulo.addNode( targetCluster.getClusterName(), NodeType.valueOf( sourceHost.getNodeGroupName() ) );
            }
        }
        else
        {
            notifyUser();
        }
    }


    protected String parseService( String output, String target )
    {
        String inputArray[] = output.split( "\n" );
        for ( String part : inputArray )
        {
            if ( part.toLowerCase().contains( target.toLowerCase() ) )
            {
                return part;
            }
        }
        return null;
    }


    @Override
    public String getSubscriberId()
    {
        return ACCUMOLO_ALERT_LISTENER;
    }


    protected int parsePid( String output ) throws AlertException
    {
        Pattern p = Pattern.compile( "pid\\s*(\\d+)", Pattern.CASE_INSENSITIVE );

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


    private void throwAlertException( String context, Exception e ) throws AlertException
    {
        LOG.error( context, e );
        throw new AlertException( context, e );
    }


    public String convertEnumValues( NodeType role )
    {
        switch ( role )
        {
            case ACCUMULO_MASTER:
                return "Master";
            case ACCUMULO_GC:
                return "GC";
            case ACCUMULO_MONITOR:
                return "Monitor";
            case ACCUMULO_TABLET_SERVER:
                return "Tablet Server";
            case ACCUMULO_TRACER:
                return "Accumulo Tracer";
            case ACCUMULO_LOGGER:
                return "Logger";
        }
        return null;
    }


    protected void notifyUser()
    {
        //TODO implement me when user identity management is complete and we can figure out user email
    }
}
