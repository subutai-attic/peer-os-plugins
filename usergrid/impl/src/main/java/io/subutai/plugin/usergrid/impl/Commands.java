/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.usergrid.impl;


import java.util.ArrayList;
import java.util.List;

import io.subutai.plugin.usergrid.api.UsergridConfig;


public class Commands
{
    UsergridConfig usergridConfig;
    private static final String catalinaHome = "/usr/share/tomcat7";
    private static final String catalinaBase = "/var/lib/tomcat7";


    public Commands ( UsergridConfig config )
    {
        this.usergridConfig = config;
    }


    public static String getCreatePropertiesFile ()
    {
        return "touch /root/usergrid-deployment.properties";
    }


    public static String getTomcatRestart ()
    {
        return "sudo /etc/init.d/tomcat7 restart";
    }


    public static String getExportJAVAHOME ()
    {
        return "sudo export JAVA_HOME=\"/usr/lib/jvm/java-8-oracle\"";
    }


    public static String getCopyRootWAR ()
    {
        return "sudo cp /root/ROOT.war " + catalinaBase + "/webapps";
    }


    public static String getUntarPortal ()
    {
        return "sudo tar -xf /root/usergrid-portal.tar";
    }


    public static String getRenamePortal ()
    {
        return "sudo mv /usergrid-portal.2.0.18 " + catalinaBase + "/webapps/portal";
    }


    public static String getCopyPortal ()
    {
        return "sudo cp -rf /root/portal " + catalinaBase + "/webapps/";
    }


    public static String replaceRPC ()
    {
        return ( "sed -i -e 's/rpc_address: localhost/rpc_address: 0.0.0.0/g' /etc/cassandra/cassandra.yaml" );
    }


    public static String getRestartCassandra ()
    {
        return "/opt/cassandra-2.0.9/bin/cassandra start";
    }


    public static String getRemoveROOTFolder ()
    {
        return "rm -rf /var/lib/tomcat7/webapps/ROOT";
    }


    public static String getRemoveSourcesList ()
    {
        return "rm -f /etc/apt/sources.list.d/*";
    }


    public static String getAptgetUpdate ()
    {
        return "apt-get update";
    }


    public static String getInstallCurl ()
    {
        return "apt-get install curl -y";
    }


    public static String getVirtual ( String a )
    {
        return "<VirtualHost *:80>\n"
                + "	ServerName *." + a + "\n"
                + "\n"
                + "	ProxyRequests On\n"
                + "	ProxyPass / http://localhost:8080/\n"
                + "	ProxyPassReverse / http://localhost:8080/\n"
                + "\n"
                + "	<Location \"/\">\n"
                + "	  Order allow,deny\n"
                + "	  Allow from all\n"
                + "	</Location>\n"
                + "	\n"
                + "</VirtualHost>";
    }


    public static String get000Default ( String userDomain )
    {
        return "<VirtualHost *:*>\n"
                + "    ProxyPreserveHost On\n"
                + "\n"
                + "    # Servers to proxy the connection, or;\n"
                + "    # List of application servers:\n"
                + "    # Usage:\n"
                + "    # ProxyPass / http://[IP Addr.]:[port]/\n"
                + "    # ProxyPassReverse / http://[IP Addr.]:[port]/\n"
                + "    # Example: \n"
                + "    ProxyPass / http://0.0.0.0:8080/\n"
                + "    ProxyPassReverse / http://0.0.0.0:8080/\n"
                + "\n"
                + "    ServerName " + userDomain + "\n"
                + "</VirtualHost>";
    }


    public static String getCopyModes ()
    {
        return "cp /etc/apache2/mods-available/proxy* /etc/apache2/mods-enabled/";
    }


    public static String getCopyXMLEnc ()
    {
        return "cp /etc/apache2/mods-available/xml* /etc/apache2/mods-enabled/";
    }


    public static String getPutJAVAHome ()
    {
        return "echo 'JAVA_HOME=/usr/lib/jvm/java-8-oracle' >> /etc/default/tomcat7";
    }


    public static String getCopySlotMem ()
    {
        return "cp /etc/apache2/mods-available/slotmem* /etc/apache2/mods-enabled/";
    }


    public static String makeSureTomcatRestartOnBoot ()
    {
        return "echo 'bash /exportScript.sh' >> /etc/rc.local";
    }


    public static String makeSureApacheRestartOnBoot ()
    {
        return "echo #!/bin/sh -e\n'/etc/init.d/apache2 restart\nbash /exportScript.sh\nexit 0' > /etc/rc.local";
    }


    public static List<String> getCurlCommands ()
    {
        List<String> r = new ArrayList ();
        r.add ( "curl -v -u superuser:test -X PUT http://localhost:8080/system/database/setup" );
        r.add ( "curl -v -u superuser:test -X PUT http://localhost:8080/system/database/bootstrap" );
        r.add ( "curl -v -u superuser:test -X GET http://localhost:8080/system/superuser/setup" );
        return r;
    }


    public static String getStartElastic ()
    {
        //echo "threadpool.index.queue_size: -1" >> elasticsearch.yml
        return "/usr/share/elasticsearch/bin/elasticsearch -d";
    }


    public static String getAdminSuperUserString ()
    {
        return "######################################################\n"
                + "# Admin and test user setup\n"
                + "usergrid.sysadmin.login.allowed=true\n"
                + "usergrid.sysadmin.login.name=superuser\n"
                + "usergrid.sysadmin.login.password=test\n"
                + "usergrid.sysadmin.login.email=usergrid@usergrid.com\n"
                + "\n"
                + "usergrid.sysadmin.email=usergrid@usergrid.com\n"
                + "usergrid.sysadmin.approve.users=true\n"
                + "usergrid.sysadmin.approve.organizations=true\n"
                + "\n"
                + "# Base mailer account - default for all outgoing messages\n"
                + "usergrid.management.mailer=Admin <usergrid@usergrid.com>\n"
                + "\n"
                + "usergrid.setup-test-account=true\n"
                + "usergrid.test-account.app=test-app\n"
                + "usergrid.test-account.organization=test-organization\n"
                + "usergrid.test-account.admin-user.username=test\n"
                + "usergrid.test-account.admin-user.name=Test User\n"
                + "usergrid.test-account.admin-user.email=testadmin@usergrid.com\n"
                + "usergrid.test-account.admin-user.password=testadmin\n";
    }


    public static String getAutoConfirmString ()
    {
        return "######################################################\n"
                + "# Auto-confirm and sign-up notifications settings\n"
                + "\n"
                + "usergrid.management.admin_users_require_confirmation=false\n"
                + "usergrid.management.admin_users_require_activation=false\n"
                + "\n"
                + "usergrid.management.organizations_require_activation=false\n"
                + "usergrid.management.notify_sysadmin_of_new_organizations=true\n"
                + "usergrid.management.notify_sysadmin_of_new_admin_users=true\n";
    }


    public static String getBaseURL ()
    {
        return "######################################################\n"
                + "# URLs\n"
                + "\n"
                + "# Redirect path when request come in for TLD\n"
                + "usergrid.redirect_root=${BASEURL}/status\n"
                + "\n"
                + "usergrid.view.management.organizations.organization.activate=${BASEURL}/accounts/welcome\n"
                + "usergrid.view.management.organizations.organization.confirm=${BASEURL}/accounts/welcome\n"
                + "\n"
                + "usergrid.view.management.users.user.activate=${BASEURL}/accounts/welcome\n"
                + "usergrid.view.management.users.user.confirm=${BASEURL}/accounts/welcome\n"
                + "\n"
                + "usergrid.admin.confirmation.url=${BASEURL}/management/users/%s/confirm\n"
                + "usergrid.user.confirmation.url=${BASEURL}/%s/%s/users/%s/confirm\n"
                + "usergrid.organization.activation.url=${BASEURL}/management/organizations/%s/activate\n"
                + "usergrid.admin.activation.url=${BASEURL}/management/users/%s/activate\n"
                + "usergrid.user.activation.url=${BASEURL}/%s/%s/users/%s/activate\n"
                + "\n"
                + "usergrid.admin.resetpw.url=${BASEURL}/management/users/%s/resetpw\n"
                + "usergrid.user.resetpw.url=${BASEURL}/%s/%s/users/%s/resetpw\n";
    }


    public static String getCollectionString ()
    {
        return "collections.keyspace=Usergrid_Applications\n"
                + "collections.keyspace.strategy.options=replication_factor:1\n"
                + "collections.keyspace.strategy.class=org.apache.cassandra.locator.SimpleStrategy\n"
                + "\n"
                + "collection.stage.transient.timeout=60\n"
                + "collection.max.entity.size=5000000\n"
                + "\n"
                + "hystrix.threadpool.graph_user.coreSize=40\n"
                + "hystrix.threadpool.graph_async.coreSize=40\n"
                + "cassandra.keyspace.strategy=org.apache.cassandra.locator.SimpleStrategy\n"
                + "cassandra.keyspace.strategy.options.replication_factor=1\n"
                + "elasticsearch.port=9300\n";
    }


}

