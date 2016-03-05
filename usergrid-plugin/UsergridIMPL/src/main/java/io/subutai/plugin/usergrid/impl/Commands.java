/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package io.subutai.plugin.usergrid.impl;


import io.subutai.plugin.usergrid.api.UsergridConfig;


/**
 *
 * @author caveman
 * @author Beyazıt Kelçeoğlu
 */
public class Commands
{
    UsergridConfig usergridConfig;
    private static String catalinaHome = "/usr/share/tomcat7";
    private static String catalinaBase = "/var/lib/tomcat7";


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
        return "/etc/init.d/tomcat7 restart";
    }


    public static String getCopyRootWAR ()
    {
        return "cp /root/ROOT.war " + catalinaBase + "/webapps";
    }


    public static String getCopyPortal ()
    {
        return "cp /root/usergrid-portal.tar" + catalinaBase + "/webapps";
    }


    public static String getUntarPortal ()
    {
        return "tar xf " + catalinaBase + "/webapps/usergrid-portal.tar";
    }


    public static String getRenamePortal ()
    {
        return "mv " + catalinaBase + "/webapps/usergrid-portal.2.0.18 " + catalinaBase + "/webapps/portal";
    }


    public static String getAdminSuperUserString ()
    {
        return "######################################################\n"
                + "# Admin and test user setup\n"
                + "usergrid.sysadmin.login.allowed=true\n"
                + "usergrid.sysadmin.login.name=superuser\n"
                + "usergrid.sysadmin.login.password=usergrid\n"
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
                + "usergrid.test-account.admin-user.password=testadmin";
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
                + "usergrid.management.notify_sysadmin_of_new_admin_users=true";
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
                + "usergrid.user.activation.url=${BASEURL}%s/%s/users/%s/activate\n"
                + "\n"
                + "usergrid.admin.resetpw.url=${BASEURL}/management/users/%s/resetpw\n"
                + "usergrid.user.resetpw.url=${BASEURL}/%s/%s/users/%s/resetpw";
    }


}

