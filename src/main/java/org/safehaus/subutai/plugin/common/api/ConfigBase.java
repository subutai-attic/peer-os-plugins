package org.safehaus.subutai.plugin.common.api;


public interface ConfigBase
{

    /**
     * Returns name of cluster
     *
     * @return - name of cluster
     */
    public String getClusterName();

    /**
     * Returns product name
     *
     * @return - product name
     */
    public String getProductName();

    /**
     * Returns product key
     *
     * @return - product key
     */
    public String getProductKey();
}
