package io.subutai.plugin.lucene.impl;


import io.subutai.common.settings.Common;
import io.subutai.plugin.lucene.api.LuceneConfig;


public class Commands
{

    public static final String PACKAGE_NAME = Common.PACKAGE_PREFIX + "lucene2";
    public static final String installCommand = "apt-get --force-yes --assume-yes install " + PACKAGE_NAME;
    public static final String uninstallCommand = "apt-get --force-yes --assume-yes purge " + PACKAGE_NAME;
    public static final String checkCommand = "dpkg -l | grep '^ii' | grep " + Common.PACKAGE_PREFIX_WITHOUT_DASH;
    public static final String updateCommand = "apt-get --force-yes --assume-yes update";

}
