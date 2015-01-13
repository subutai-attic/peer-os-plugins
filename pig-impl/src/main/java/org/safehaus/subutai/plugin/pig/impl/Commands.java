package org.safehaus.subutai.plugin.pig.impl;


import org.safehaus.subutai.common.settings.Common;
import org.safehaus.subutai.plugin.pig.api.PigConfig;


public class Commands
{
    public static final String PACKAGE_NAME = Common.PACKAGE_PREFIX + PigConfig.PRODUCT_KEY.toLowerCase();

    public static final String installCommand = "apt-get --force-yes --assume-yes install " + PACKAGE_NAME;
    public static final String uninstallCommand = "apt-get --force-yes --assume-yes purge " + PACKAGE_NAME;
    public static final String checkCommand = "dpkg -l | grep '^ii' | grep " + Common.PACKAGE_PREFIX_WITHOUT_DASH;
}
