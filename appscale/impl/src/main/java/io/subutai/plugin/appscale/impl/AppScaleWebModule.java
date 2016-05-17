package io.subutai.plugin.appscale.impl;


import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;

import io.subutai.webui.api.WebuiModule;
import io.subutai.webui.entity.AngularjsDependency;
import io.subutai.webui.entity.WebuiModuleResourse;


/**
 * @author caveman
 * @author Beyazıt Kelçeoğlu
 */
public class AppScaleWebModule implements WebuiModule
{

    public static final String NAME = "AppScale";
    public static final String IMG = "plugins/appscale/appscale.png";
    private static final Map<String, Integer> TEMPLATES_REQUIREMENT;

    static
    {
        TEMPLATES_REQUIREMENT = new HashMap<>();
        TEMPLATES_REQUIREMENT.put( "appscale", 1 );
    }


    private WebuiModuleResourse appscaleResource;


    public void init()
    {
        this.appscaleResource = new WebuiModuleResourse( NAME.toLowerCase(), IMG );
        AngularjsDependency angularjsDependency =
                new AngularjsDependency( "subutai.plugins.appscale", "plugins/appscale/appscale.js",
                        "plugins/appscale/controller.js", "plugins/appscale/service.js",
                        "subutai-app/environment/service.js" );

        appscaleResource.addDependency( angularjsDependency );
    }


    @Override
    public String getAngularState()
    {
        return appscaleResource.getAngularjsList();
    }


    @Override
    public String getName()
    {
        return NAME;
    }


    @Override
    public String getModuleInfo()
    {
        return String.format( "{\"img\" : \"%s\", \"name\" : \"%s\", \"requirement\" : %s}", IMG, NAME,
                new Gson().toJson( TEMPLATES_REQUIREMENT ).toString() );
    }


    @Override
    public String getAngularDependecyList()
    {
        return String.format( ".state('%s', %s)", NAME.toLowerCase(), appscaleResource.getAngularjsList() );
    }
}

