package io.subutai.plugin.zookeeper.impl.alert;


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
import io.subutai.common.peer.ContainerHost;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.peer.ExceededQuotaAlertHandler;
import io.subutai.common.peer.PeerException;
import io.subutai.common.quota.ContainerCpuResource;
import io.subutai.common.quota.ContainerQuota;
import io.subutai.common.quota.ContainerRamResource;
import io.subutai.common.resource.ByteUnit;
import io.subutai.common.resource.ByteValueResource;
import io.subutai.common.resource.MeasureUnit;
import io.subutai.common.resource.NumericValueResource;
import io.subutai.common.resource.ResourceType;
import io.subutai.common.resource.ResourceValue;
import io.subutai.core.metric.api.MonitorException;
import io.subutai.core.metric.api.MonitoringSettings;
import io.subutai.plugin.zookeeper.api.ZookeeperClusterConfig;
import io.subutai.plugin.zookeeper.impl.Commands;
import io.subutai.plugin.zookeeper.impl.ZookeeperImpl;


public class ZookeeperAlertListener extends ExceededQuotaAlertHandler
{
    private static final Logger LOGGER = LoggerFactory.getLogger( ZookeeperAlertListener.class );
    private static final String HANDLER_ID = "DEFAULT_ZOOKEEPER_EXCEEDED_QUOTA_ALERT_HANDLER";
    private ZookeeperImpl zookeeper;
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
    public String getId()
    {
        return HANDLER_ID;
    }


    @Override
    public String getDescription()
    {
        return "Default zookeeper exceeded quota alert handler.";
    }


    @Override
    public void process( final Environment environment, final QuotaAlertValue alert ) throws AlertHandlerException
    {
        //find zookeeper cluster by environment id
        List<ZookeeperClusterConfig> clusters = zookeeper.getClusters();

        ZookeeperClusterConfig targetCluster = null;
        for ( ZookeeperClusterConfig cluster : clusters )
        {
            if ( cluster.getEnvironmentId().equals( environment.getId() ) )
            {
                targetCluster = cluster;
                break;
            }
        }

        if ( targetCluster == null )
        {
            throw new AlertHandlerException(
                    String.format( "Cluster not found by environment id %s", environment.getId() ) );
        }


        //get environment containers and find alert source host
        Set<EnvironmentContainerHost> containers = environment.getContainerHosts();

        ContainerHost sourceHost = null;
        final String containerId = alert.getValue().getHostId().getId();
        for ( ContainerHost containerHost : containers )
        {
            if ( containerHost.getId().equals( containerId ) )
            {
                sourceHost = containerHost;
                break;
            }
        }

        if ( sourceHost == null )
        {
            throw new AlertHandlerException(
                    String.format( "Alert source host %s not found in environment", containerId ), null );
        }

        //check if source host belongs to found zookeeper cluster
        if ( !targetCluster.getNodes().contains( sourceHost.getId() ) )
        {
            LOGGER.info( String.format( "Alert source host %s does not belong to Zookeeper cluster", containerId ) );
            return;
        }

        //figure out Zookeeper process pid
        int zookeeperPid;
        try
        {
            CommandResult result = commandUtil.execute( new RequestBuilder( Commands.getStatusCommand() ), sourceHost );
            zookeeperPid = parsePid( result.getStdOut() );
        }
        catch ( NumberFormatException | CommandException e )
        {
            throw new AlertHandlerException( "Error obtaining Zookeeper process PID", e );
        }

        //get Zookeeper process resource usage by Zookeeper pid
        ProcessResourceUsage processResourceUsage = null;
        try
        {
            processResourceUsage =
                    zookeeper.getMonitor().getProcessResourceUsage( sourceHost.getContainerId(), zookeeperPid );
        }
        catch ( MonitorException e )
        {
            throw new AlertHandlerException( "Error obtaining Zookeeper process usage", e );
        }

        //confirm that Zookeeper is causing the stress, otherwise no-op
        MonitoringSettings thresholds = zookeeper.getAlertSettings();

        final ExceededQuota alertResource = alert.getValue();

        final ResourceValue<ByteValueResource> ramValue = alertResource.getCurrentValue();
        final double currentRam = ramValue.getValue().getValue( ByteUnit.MB ).doubleValue();
        double ramLimit = currentRam * ( ( double ) thresholds.getRamAlertThreshold() / 100 );

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


        try
        {
            //auto-scaling is enabled -> scale cluster
            if ( targetCluster.isAutoScaling() )
            {
                // check if a quota limit increase does it
                boolean quotaIncreased = false;
                final ContainerQuota containerQuota = sourceHost.getQuota();

                if ( isRamStressedByZookeeper )
                {
                    //read current RAM quota
                    double ramQuota = containerQuota.getRam().getResource().getValue( ByteUnit.MB ).doubleValue();


                    if ( ramQuota < MAX_RAM_QUOTA_MB )
                    {
                        // if available quota on resource host is greater than 10 % of calculated increase amount,
                        // increase quota, otherwise scale horizontally
                        double newRamQuota = ramQuota * ( 100 + RAM_QUOTA_INCREMENT_PERCENTAGE ) / 100;
                        if ( MAX_RAM_QUOTA_MB > newRamQuota )
                        {
                            LOGGER.info( "Increasing ram quota of {} from {} MB to {} MB.", sourceHost.getHostname(),
                                    ramQuota, newRamQuota );
                            //we can increase RAM quota
                            ContainerRamResource newQuota = new ContainerRamResource( new ByteValueResource(
                                    ByteValueResource.toBytes( new BigDecimal( newRamQuota ), ByteUnit.MB ) ) );
                            containerQuota.addResource( newQuota );
                            sourceHost.setQuota( containerQuota );

                            quotaIncreased = true;
                        }
                    }
                }
                if ( isCpuStressedByZookeeper )
                {

                    //read current CPU quota
                    ContainerCpuResource cpuQuota = containerQuota.getCpu();
                    if ( cpuQuota.getResource().getValue().intValue() < MAX_CPU_QUOTA_PERCENT )
                    {
                        int newCpuQuota = Math.min( MAX_CPU_QUOTA_PERCENT,
                                cpuQuota.getResource().getValue().intValue() + CPU_QUOTA_INCREMENT_PERCENT );
                        LOGGER.info( "Increasing cpu quota of {} from {}% to {}%.", sourceHost.getHostname(),
                                cpuQuota.getResource().getValue().intValue(), newCpuQuota );
                        //we can increase CPU quota
                        ContainerCpuResource newQuota =
                                new ContainerCpuResource( new NumericValueResource( new BigDecimal( newCpuQuota ) ) );
                        containerQuota.addResource( newQuota );
                        sourceHost.setQuota( containerQuota );

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
        catch ( PeerException e )
        {
            throw new AlertHandlerException( e.getMessage() );
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
            throw new AlertHandlerException( String.format( "Could not parse PID from %s", output ), null );
        }
    }


    protected void notifyUser()
    {
        //TODO implement me when user identity management is complete and we can figure out user email
    }
}
