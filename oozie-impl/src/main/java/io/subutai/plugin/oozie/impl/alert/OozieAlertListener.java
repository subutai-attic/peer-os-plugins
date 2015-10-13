package io.subutai.plugin.oozie.impl.alert;


import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.subutai.common.command.CommandException;
import io.subutai.common.command.CommandResult;
import io.subutai.common.command.CommandUtil;
import io.subutai.common.environment.Environment;
import io.subutai.common.metric.ContainerHostMetric;
import io.subutai.common.metric.ProcessResourceUsage;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.core.metric.api.AlertListener;
import io.subutai.core.metric.api.MonitoringSettings;
import io.subutai.plugin.hadoop.api.HadoopClusterConfig;
import io.subutai.plugin.oozie.api.OozieClusterConfig;
import io.subutai.plugin.oozie.impl.Commands;
import io.subutai.plugin.oozie.impl.OozieImpl;


public class OozieAlertListener implements AlertListener
{
    private static final Logger LOG = LoggerFactory.getLogger( OozieAlertListener.class );
    private OozieImpl oozie;
    public static final String OOZIE_ALERT_LISTENER = "OOZIE_ALERT_LISTENER";
    private CommandUtil commandUtil = new CommandUtil();
    private static int MAX_RAM_QUOTA_MB = 2048;
    private static int RAM_QUOTA_INCREMENT_MB = 512;
    private static int MAX_CPU_QUOTA_PERCENT = 80;
    private static int CPU_QUOTA_INCREMENT_PERCENT = 10;


    public OozieAlertListener( final OozieImpl oozie )
    {
        this.oozie = oozie;
    }


    private void throwAlertException( String context, Exception e ) throws AlertException
    {
        LOG.error( context, e );
        throw new AlertException( context, e );
    }


    @Override
    public void onAlert( ContainerHostMetric metric ) throws Exception
    {
        //find oozie cluster by environment id
        List<OozieClusterConfig> clusters = oozie.getClusters();

        OozieClusterConfig targetCluster = null;
        for ( OozieClusterConfig cluster : clusters )
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
        Environment environment = oozie.getEnvironmentManager().loadEnvironment( metric.getEnvironmentId() );
        if ( environment == null )
        {
            throwAlertException( String.format( "Environment not found by id %s", metric.getEnvironmentId() ), null );
        }

        //get environment containers and find alert source host
        Set<EnvironmentContainerHost> containers = environment.getContainerHosts();

        EnvironmentContainerHost sourceHost = null;
        for ( EnvironmentContainerHost containerHost : containers )
        {
            if ( containerHost.getId().equals( metric.getHostId() ) )
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

        //check if source host belongs to found oozie cluster
        if ( !targetCluster.getAllNodes().contains( sourceHost.getId() ) )
        {
            LOG.info( String.format( "Alert source host %s does not belong to Oozie cluster", metric.getHost() ) );
            return;
        }

        //figure out oozie process pid
        int ooziePid = 0;
        try
        {
            CommandResult result = commandUtil.execute( Commands.getStatusServerCommand(), sourceHost );
            ooziePid = parsePid( result.getStdOut() );
        }
        catch ( NumberFormatException | CommandException e )
        {
            throwAlertException( "Error obtaining process PID", e );
        }

        //get oozie process resource usage by oozie pid
        ProcessResourceUsage processResourceUsage = oozie.getMonitor().getProcessResourceUsage( sourceHost, ooziePid );

        //confirm that oozie is causing the stress, otherwise no-op
        MonitoringSettings thresholds = oozie.getAlertSettings();
        double ramLimit = metric.getTotalRam() * ( thresholds.getRamAlertThreshold() / 100 ); // 0.8
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
                int ramQuota = oozie.getQuotaManager().getRamQuota( sourceHost.getId() );


                if ( ramQuota < MAX_RAM_QUOTA_MB )
                {
                    //we can increase RAM quota
                    oozie.getQuotaManager().setRamQuota( sourceHost.getId(),
                            Math.min( MAX_RAM_QUOTA_MB, ramQuota + RAM_QUOTA_INCREMENT_MB ) );

                    quotaIncreased = true;
                }
            }
            if ( isCpuStressedByOozie )
            {

                //read current CPU quota
                int cpuQuota = oozie.getQuotaManager().getCpuQuota( sourceHost.getId() );

                if ( cpuQuota < MAX_CPU_QUOTA_PERCENT )
                {
                    //we can increase CPU quota
                    oozie.getQuotaManager().setCpuQuota( sourceHost.getId(),
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
            HadoopClusterConfig hadoopClusterConfig =
                    oozie.getHadoopManager().getCluster( targetCluster.getHadoopClusterName() );
            if ( hadoopClusterConfig == null )
            {
                throwAlertException(
                        String.format( "Oozie cluster %s not found", targetCluster.getHadoopClusterName() ), null );
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


    @Override
    public String getSubscriberId()
    {
        return OOZIE_ALERT_LISTENER;
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
            throwAlertException( String.format( "Could not parse PID from %s", output ), null );
        }
        return 0;
    }


    protected void notifyUser()
    {
        //TODO implement me when user identity management is complete and we can figure out user email
    }
}
