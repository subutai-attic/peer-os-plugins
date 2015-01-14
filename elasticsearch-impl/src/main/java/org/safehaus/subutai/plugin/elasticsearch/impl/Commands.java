package org.safehaus.subutai.plugin.elasticsearch.impl;


import org.safehaus.subutai.common.settings.Common;
import org.safehaus.subutai.plugin.elasticsearch.api.ElasticsearchClusterConfiguration;


public class Commands
{
    public static final String PACKAGE_NAME =
            Common.PACKAGE_PREFIX + ElasticsearchClusterConfiguration.PRODUCT_KEY.toLowerCase();

    public static String statusCommand = "service elasticsearch status";
    public static String startCommand = "service elasticsearch start";
    public static String stopCommand = "service elasticsearch stop";
    public static String configure = ". /etc/profile && es-conf.sh";
    public static String checkCommand = "dpkg -l | grep '^ii' | grep " + Common.PACKAGE_PREFIX_WITHOUT_DASH;
    public static String installCommand = String.format( "apt-get --force-yes --assume-yes install %s", PACKAGE_NAME );
}
