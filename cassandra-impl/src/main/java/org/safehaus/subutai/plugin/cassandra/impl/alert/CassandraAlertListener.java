package org.safehaus.subutai.plugin.cassandra.impl.alert;


import org.safehaus.subutai.core.metric.api.AlertListener;
import org.safehaus.subutai.core.metric.api.ContainerHostMetric;
import org.safehaus.subutai.core.peer.api.CommandUtil;
import org.safehaus.subutai.plugin.cassandra.impl.CassandraImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Node resource threshold excess alert listener
 */
public class CassandraAlertListener implements AlertListener
{
    private static final Logger LOG = LoggerFactory.getLogger( CassandraAlertListener.class.getName() );
    public static final String CASSANDRA_ALERT_LISTENER = "CASSANDRA_ALERT_LISTENER";
    private CassandraImpl cassandra;
    private CommandUtil commandUtil = new CommandUtil();
    private static int MAX_RAM_QUOTA_MB = 2048;
    private static int RAM_QUOTA_INCREMENT_MB = 512;
    private static int MAX_CPU_QUOTA_PERCENT = 80;
    private static int CPU_QUOTA_INCREMENT_PERCENT = 10;


    public CassandraAlertListener( final CassandraImpl cassandra )
    {
        this.cassandra = cassandra;
    }


    @Override
    public void onAlert( final ContainerHostMetric containerHostMetric ) throws Exception
    {

    }


    @Override
    public String getSubscriberId()
    {
        return CASSANDRA_ALERT_LISTENER;
    }
}
