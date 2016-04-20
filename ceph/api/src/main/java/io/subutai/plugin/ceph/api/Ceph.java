package io.subutai.plugin.ceph.api;


import io.subutai.core.plugincommon.api.ApiBase;
import io.subutai.webui.api.WebuiModule;


public interface Ceph extends ApiBase<CephClusterConfig>
{
    public WebuiModule getWebModule();

    public void setWebModule( final WebuiModule webModule );
}
