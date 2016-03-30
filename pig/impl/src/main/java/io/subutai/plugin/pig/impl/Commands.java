package io.subutai.plugin.pig.impl;


import io.subutai.common.command.OutputRedirection;
import io.subutai.common.command.RequestBuilder;
import io.subutai.common.settings.Common;
import io.subutai.plugin.pig.api.PigConfig;


public class Commands
{
    public static final String PACKAGE_NAME = Common.PACKAGE_PREFIX + PigConfig.PRODUCT_KEY.toLowerCase();
    public static final String installCommand = "apt-get --force-yes --assume-yes install " + PACKAGE_NAME;
    public static final String uninstallCommand = "apt-get --force-yes --assume-yes purge " + PACKAGE_NAME;
    public static final String checkCommand = "dpkg -l | grep '^ii' | grep " + Common.PACKAGE_PREFIX_WITHOUT_DASH;

    public static RequestBuilder getAptUpdate()
    {
        return new RequestBuilder( "apt-get --force-yes --assume-yes update" ).withTimeout( 2000 ).withStdOutRedirection(
                OutputRedirection.NO );
    }

}
