package io.subutai.plugin.cassandra.impl.alert;


import java.math.BigDecimal;
import java.util.List;
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
import io.subutai.common.metric.ExceededQuota;
import io.subutai.common.metric.ProcessResourceUsage;
import io.subutai.common.metric.QuotaAlertValue;
import io.subutai.common.peer.AlertHandlerException;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.peer.ExceededQuotaAlertHandler;
import io.subutai.common.peer.PeerException;
import io.subutai.hub.share.quota.ContainerCpuResource;
import io.subutai.hub.share.quota.ContainerDiskResource;
import io.subutai.hub.share.quota.ContainerOptResource;
import io.subutai.hub.share.quota.ContainerQuota;
import io.subutai.hub.share.quota.ContainerRamResource;
import io.subutai.hub.share.quota.Quota;
import io.subutai.hub.share.resource.ByteUnit;
import io.subutai.hub.share.resource.ByteValueResource;
import io.subutai.hub.share.resource.ContainerResourceType;
import io.subutai.hub.share.resource.MeasureUnit;
import io.subutai.hub.share.resource.NumericValueResource;
import io.subutai.hub.share.resource.ResourceType;
import io.subutai.hub.share.resource.ResourceValue;
import io.subutai.core.metric.api.MonitoringSettings;
import io.subutai.plugin.cassandra.api.CassandraClusterConfig;
import io.subutai.plugin.cassandra.impl.CassandraImpl;
import io.subutai.plugin.cassandra.impl.Commands;


/**
 * Node resource threshold excess alert listener
 */
public class CassandraExceededQuotaAlertHandler extends ExceededQuotaAlertHandler
{
    private static final Logger LOG = LoggerFactory.getLogger( CassandraExceededQuotaAlertHandler.class );
    private static final String HANDLER_ID = "DEFAULT_CASSANDRA_EXCEEDED_QUOTA_ALERT_HANDLER";
    private CassandraImpl cassandra;
    private CommandUtil commandUtil = new CommandUtil();

    private static double MAX_RAM_QUOTA_MB;
    private static int RAM_QUOTA_INCREMENT_PERCENTAGE = 25;
    private static int MAX_CPU_QUOTA_PERCENT = 100;
    private static int CPU_QUOTA_INCREMENT_PERCENT = 15;


    public CassandraExceededQuotaAlertHandler( final CassandraImpl cassandra )
    {
        this.cassandra = cassandra;
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
        return "Node resource threshold excess alert handler for cassandra cluster.";
    }


    @Override
    public void process( final Environment environment, final QuotaAlertValue alert ) throws AlertHandlerException
    {
        //find cluster by environment id
        List<CassandraClusterConfig> clusters = cassandra.getClusters();

        CassandraClusterConfig targetCluster = null;
        for ( CassandraClusterConfig cluster : clusters )
        {
            if ( cluster.getEnvironmentId().equals( environment.getEnvironmentId() ) )
            {
                targetCluster = cluster;
                break;
            }
        }

        if ( targetCluster == null )
        {
            throwAlertException(
                    String.format( "Cluster not found by environment id %s", environment.getEnvironmentId() ), null );
        }

        //get environment containers and find alert's source host
        Set<EnvironmentContainerHost> containers = environment.getContainerHosts();

        EnvironmentContainerHost sourceHost = getSourceHost();

        if ( sourceHost == null )
        {
            throwAlertException(
                    String.format( "Alert source host %s not found in environment", alert.getValue().getHostId() ),
                    null );
        }

        //check if source host belongs to found cluster
        if ( !targetCluster.getSeedNodes().contains( sourceHost.getId() ) )
        {
            LOG.info( String.format( "Alert source host %s does not belong to Cassandra cluster",
                    alert.getValue().getHostId() ) );
            return;
        }


        // Set 50 percent of the available ram capacity of the resource host
        // to maximum ram quota limit assignable to the container
        try
        {
            MAX_RAM_QUOTA_MB = ramResource.getResource().getValue( ByteUnit.MB ).doubleValue() * 0.5;

            //figure out process pid
            int processPID = 0;
            try
            {
                CommandResult result = commandUtil.execute( new RequestBuilder( Commands.STATUS_COMMAND ), sourceHost );
                processPID = parsePid( result.getStdOut() );
            }
            catch ( NumberFormatException | CommandException e )
            {
                throwAlertException( "Error obtaining process PID", e );
            }

            //get process resource usage by pid
            ProcessResourceUsage processResourceUsage = sourceHost.getProcessResourceUsage( processPID );

            //confirm that Cassandra is causing the stress, otherwise no-op
            MonitoringSettings thresholds = cassandra.getAlertSettings();

            // in MB
            double ramLimit =
                    ramResource.getResource().getValue().doubleValue() * thresholds.getRamAlertThreshold() / 100; // 0.8
            double redLine = 0.7;
            boolean isCpuStressed = false;
            boolean isRamStressed = false;

            if ( processResourceUsage.getUsedRam() >= ramLimit * redLine )
            {
                isRamStressed = true;
            }

            if ( processResourceUsage.getUsedCpu() >= thresholds.getCpuAlertThreshold() * redLine )
            {
                isCpuStressed = true;
            }

            if ( !( isRamStressed || isCpuStressed ) )
            {
                LOG.info( "Cassandra cluster is not stressed, returning." );
                return;
            }


            //auto-scaling is enabled -> scale cluster
            if ( targetCluster.isAutoScaling() )
            {
                // check if a quota limit increase does it
                boolean quotaIncreased = false;

                if ( isRamStressed )
                {
                    //read current RAM quota
                    final ContainerQuota containerQuota = sourceHost.getQuota();
                    double ramQuota = containerQuota.get( ContainerResourceType.RAM ).getAsRamResource().getResource()
                                                    .getValue( ByteUnit.MB ).doubleValue();

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

                if ( isCpuStressed )
                {

                    //read current CPU quota
                    final ContainerQuota containerQuota = sourceHost.getQuota();
                    ContainerCpuResource cpuQuota = containerQuota.get( ContainerResourceType.CPU ).getAsCpuResource();
                    if ( cpuQuota.getResource().getValue().intValue() < MAX_CPU_QUOTA_PERCENT )
                    {
                        int newCpuQuota = Math.min( MAX_CPU_QUOTA_PERCENT,
                                cpuQuota.getResource().getValue().intValue() + CPU_QUOTA_INCREMENT_PERCENT );
                        LOG.info( "Increasing cpu quota of {} from {}% to {}%.", sourceHost.getHostname(),
                                cpuQuota.getResource().getValue().intValue(), newCpuQuota );
                        //we can increase CPU quota
                        Quota newQuota = new Quota(
                                new ContainerCpuResource( new NumericValueResource( new BigDecimal( newCpuQuota ) ) ),
                                thresholds.getCpuAlertThreshold() );
                        containerQuota.add( newQuota );
                        sourceHost.setQuota( containerQuota );

                        quotaIncreased = true;
                    }
                }

                //quota increase is made, return
                if ( quotaIncreased )
                {
                    return;
                }

                //launch node addition process
                LOG.info( "Adding new node to {} cassandra cluster", targetCluster.getClusterName() );
                cassandra.addNode( targetCluster.getClusterName() );
            }
            else
            {
                notifyUser();
            }
        }
        catch ( PeerException e )
        {
            throwAlertException( "Error obtaining quota of " + sourceHost.getId(), null );
        }
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
