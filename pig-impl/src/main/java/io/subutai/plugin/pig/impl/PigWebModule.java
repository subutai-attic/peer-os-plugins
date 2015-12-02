package io.subutai.plugin.pig.impl;


import io.subutai.webui.api.WebuiModule;


public class PigWebModule implements WebuiModule
{
    public static String NAME = "Pig";
    public static String IMG = "plugins/pig/pig.png";

    @Override
    public String getName() {
        return NAME;
    }


    @Override
    public String getModuleInfo() {
        return String.format("{\"img\" : \"%s\", \"name\" : \"%s\"}", IMG, NAME);
    }


    @Override
    public String getAngularDependecyList() {
        return String.format("{" +
                "name: 'subutai.blueprints', files: ["
                + "'subutai-app/blueprints/blueprints.js',"
                + "'subutai-app/blueprints/controller.js',"
                + "'subutai-app/environment/service.js'"
                + "]}");
    }
}
