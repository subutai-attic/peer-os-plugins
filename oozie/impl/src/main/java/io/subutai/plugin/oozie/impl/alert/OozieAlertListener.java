package io.subutai.plugin.oozie.impl.alert;


import java.math.BigDecimal;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.subutai.common.command.CommandResult;
import io.subutai.common.command.CommandUtil;
import io.subutai.common.environment.Environment;
import io.subutai.common.metric.ExceededQuota;
import io.subutai.common.metric.ProcessResourceUsage;
import io.subutai.common.metric.QuotaAlertValue;
import io.subutai.common.peer.AlertHandlerException;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.common.peer.ExceededQuotaAlertHandler;
import io.subutai.core.metric.api.MonitoringSettings;
import io.subutai.hub.share.quota.ContainerCpuResource;
import io.subutai.hub.share.quota.ContainerQuota;
import io.subutai.hub.share.quota.ContainerRamResource;
import io.subutai.hub.share.quota.Quota;
import io.subutai.hub.share.resource.ByteUnit;
import io.subutai.hub.share.resource.ByteValueResource;
import io.subutai.hub.share.resource.ContainerResourceType;
import io.subutai.hub.share.resource.ResourceValue;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.oozie.api.OozieClusterConfig;
import io.subutai.plugin.oozie.impl.Commands;
import io.subutai.plugin.oozie.impl.OozieImpl;


public class OozieAlertListener extends ExceededQuotaAlertHandler
{
    private static final Logger LOG = LoggerFactory.getLogger( OozieAlertListener.class );
    private OozieImpl oozie;
    public static final String OOZIE_ALERT_LISTENER = "OOZIE_ALERT_LISTENER";
    private static final String HANDLER_ID = "DEFAULT_OOZIE_EXCEEDED_QUOTA_ALERT_HANDLER";
    private CommandUtil commandUtil = new CommandUtil();
    private static int MAX_RAM_QUOTA_MB = 2048;
    private static int RAM_QUOTA_INCREMENT_MB = 512;
    private static int RAM_QUOTA_INCREMENT_PERCENTAGE = 25;
    private static int MAX_CPU_QUOTA_PERCENT = 80;
    private static int CPU_QUOTA_INCREMENT_PERCENT = 10;


    public OozieAlertListener( final OozieImpl oozie )
    {
        this.oozie = oozie;
    }


    private void throwAlertException( String context, Exception e ) throws AlertHandlerException
    {
        LOG.error( context, e );
        throw new AlertHandlerException( context, e );
    }


    //    @Override
    //    public String getTemplateName()
    //    {
    //        return "hadoop";
    //    }


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
            throwAlertException( String.format( "Could not parse PID from %s", output ), null );
        }
        return 0;
    }


    protected void notifyUser()
    {
        //TODO implement me when user identity management is complete and we can figure out user email
    }


    @Override
    public String getDescription()
    {
        return "Node resource exceeded threshold alert handler for oozie cluster.";
    }


    @Override
    public void process( final Environment environment, final QuotaAlertValue quotaAlertValue )
            throws AlertHandlerException
    {
        //find oozie cluster by environment id
        List<OozieClusterConfig> clusters = oozie.getClusters();

        OozieClusterConfig targetCluster = null;
        for ( OozieClusterConfig cluster : clusters )
        {
            if ( cluster.getEnvironmentId().equals( environment.getId() ) )
            {
                targetCluster = cluster;
                break;
            }
        }

        if ( targetCluster == null )
        {
            throwAlertException( String.format( "Cluster not found by environment id %s", environment.getId() ), null );
            return;
        }

        //get environment containers and find alert source host
        Set<EnvironmentContainerHost> containers = environment.getContainerHosts();

        EnvironmentContainerHost sourceHost = null;
        for ( EnvironmentContainerHost containerHost : containers )
        {
            if ( containerHost.getId().equals( quotaAlertValue.getValue().getHostId().toString() ) )
            {
                sourceHost = containerHost;
                break;
            }
        }

        if ( sourceHost == null )
        {
            throwAlertException( String.format( "Alert source host %s not found " + "in environment",
                    quotaAlertValue.getValue().getHostId() ), null );
            return;
        }

        //check if source host belongs to found oozie cluster
        if ( !targetCluster.getAllNodes().contains( sourceHost.getId() ) )
        {
            LOG.info( String.format( "Alert source host %s does not belong " + "to Oozie cluster",
                    quotaAlertValue.getValue().getHostId() ) );
            return;
        }

        //figure out oozie process pid
        int ooziePid = 0;
        try
        {
            CommandResult result = commandUtil.execute( Commands.getStatusServerCommand(), sourceHost );
            ooziePid = parsePid( result.getStdOut() );
        }
        catch ( Exception e )
        {
            throwAlertException( "Error obtaining process PID", e );
            return;
        }

        try
        {
            //get oozie process resource usage by oozie pid
            ProcessResourceUsage processResourceUsage =
                    oozie.getMonitor().getProcessResourceUsage( sourceHost.getContainerId(), ooziePid );

            //confirm that oozie is causing the stress, otherwise no-op
            MonitoringSettings thresholds = oozie.getAlertSettings();
            ExceededQuota exceededQuota = quotaAlertValue.getValue();
            ResourceValue<BigDecimal> currentValue = exceededQuota.getCurrentValue();

            double ramLimit =
                    currentValue.getValue().doubleValue() * ( thresholds.getRamAlertThreshold() / 100 ); // 0.8
            double redLine = 0.9;
            boolean isCpuStressedByOozie = false;
            boolean isRamStressedByOozie = false;

            if ( processResourceUsage.getUsedRam() >= ramLimit * redLine )
            {
                isRamStressedByOozie = true;
            }
            if ( processResourceUsage.getUsedCpu() >= thresholds.getCpuAlertThreshold() * redLine )
            {
                isCpuStressedByOozie = true;
            }

            if ( !( isRamStressedByOozie || isCpuStressedByOozie ) )
            {
                LOG.info( "oozie cluster runs ok" );
                return;
            }


            //auto-scaling is enabled -> scale cluster
            if ( targetCluster.isAutoScaling() )
            {
                // check if a quota limit increase does it
                boolean quotaIncreased = false;

                if ( isRamStressedByOozie )
                {
                    //read current RAM quota
                    ContainerQuota containerQuota = sourceHost.getQuota();
                    double ramQuota = containerQuota.get( ContainerResourceType.RAM ).getAsRamResource().getResource()
                                                    .getValue( ByteUnit.MB ).doubleValue();
                    //                    int ramQuota = oozie.getQuotaManager().getRamQuota( sourceHost.getId() );
                    if ( ramQuota < MAX_RAM_QUOTA_MB )
                    {
                        double newRamQuota = ramQuota * ( 100 + RAM_QUOTA_INCREMENT_MB ) / 100;

                        if ( MAX_RAM_QUOTA_MB > newRamQuota )
                        {
                            LOG.info( "Increasing ram quota of {} from {} MB to {} MB.", sourceHost.getHostname(),
                                    ramQuota, newRamQuota );

                            Quota quota = new Quota( new ContainerRamResource( newRamQuota, ByteUnit.MB ),
                                    thresholds.getRamAlertThreshold() );

                            containerQuota.add( quota );
                            sourceHost.setQuota( containerQuota );
                            quotaIncreased = true;
                        }
                    }
                }
                if ( isCpuStressedByOozie )
                {
                    //read current RAM quota
                    ContainerQuota containerQuota = sourceHost.getQuota();
                    ContainerCpuResource cpuQuota = containerQuota.get( ContainerResourceType.CPU ).getAsCpuResource();

                    if ( cpuQuota.getResource().getValue().intValue() < MAX_CPU_QUOTA_PERCENT )
                    {
                        int newCpuQuota = Math.min( MAX_CPU_QUOTA_PERCENT,
                                cpuQuota.getResource().getValue().intValue() + CPU_QUOTA_INCREMENT_PERCENT );
                        LOG.info( "Increasing cpu quota of {} from {}% to {}%.", sourceHost.getHostname(),
                                cpuQuota.getResource().getValue().intValue(), newCpuQuota );

                        Quota quota =
                                new Quota( new ContainerCpuResource( newCpuQuota ), thresholds.getRamAlertThreshold() );

                        containerQuota.add( quota );
                        sourceHost.setQuota( containerQuota );
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
                        oozie.getHadoopManager().getCluster( targetCluster.getHadoopClusterName() );
                if ( hadoopClusterConfig == null )
                {
                    throwAlertException(
                            String.format( "Oozie cluster %s not found", targetCluster.getHadoopClusterName() ), null );
                    return;
                }

                List<String> availableNodes = hadoopClusterConfig.getAllNodes();
                availableNodes.removeAll( targetCluster.getNodes() );

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
                        throwAlertException(
                                String.format( "Could not obtain available hadoop node from environment by id %s",
                                        newNodeId ), null );
                        return;
                    }

                    //launch node addition process
                    oozie.addNode( targetCluster.getClusterName(), newNodeHostName );
                }
            }
            else
            {
                notifyUser();
            }
        }
        catch ( Exception e )
        {
            throwAlertException( "Error processing threshold alert", e );
        }
    }


    @Override
    public String getId()
    {
        return HANDLER_ID;
    }
}
