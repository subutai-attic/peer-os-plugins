package io.subutai.plugin.elasticsearch.impl.alert;


import java.util.Iterator;
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
import io.subutai.common.metric.Alert;
import io.subutai.common.metric.ContainerHostMetric;
import io.subutai.common.metric.ProcessResourceUsage;
import io.subutai.common.peer.EnvironmentContainerHost;
import io.subutai.core.metric.api.AlertListener;
import io.subutai.core.metric.api.MonitoringSettings;
import io.subutai.plugin.elasticsearch.api.ElasticsearchClusterConfiguration;
import io.subutai.plugin.elasticsearch.impl.ElasticsearchImpl;


public class EsAlertListener implements AlertListener
{
    private static final Logger LOG = LoggerFactory.getLogger( EsAlertListener.class.getName() );

    public static final String ES_ALERT_LISTENER = "ES_ALERT_LISTENER";
    private CommandUtil commandUtil = new CommandUtil();
    private static double MAX_RAM_QUOTA_MB;
    private static int RAM_QUOTA_INCREMENT_PERCENTAGE = 25;
    private static int MAX_CPU_QUOTA_PERCENT = 100;
    private static int CPU_QUOTA_INCREMENT_PERCENT = 15;

    private ElasticsearchImpl elasticsearch;


    public EsAlertListener( final ElasticsearchImpl elasticsearch )
    {
        this.elasticsearch = elasticsearch;
    }


    private void throwAlertException( String context, Exception e ) throws AlertException
    {
        LOG.error( context, e );
        throw new AlertException( context, e );
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
    public void onAlert( final Alert alert ) throws Exception
    {

    }


    @Override
    public String getSubscriberId()
    {
        return ES_ALERT_LISTENER;
    }
}
