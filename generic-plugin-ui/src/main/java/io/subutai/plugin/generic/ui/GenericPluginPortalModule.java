package io.subutai.plugin.generic.ui;


import java.io.File;

import com.vaadin.ui.Component;

import io.subutai.common.util.FileUtil;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.registry.api.TemplateRegistry;
import io.subutai.plugin.generic.api.GenericPlugin;
import io.subutai.server.ui.api.PortalModule;


public class GenericPluginPortalModule implements PortalModule
{
    public static final String MODULE_IMAGE = "test.png";
    private static final String NAME = "Generic Plugin";
    private GenericPlugin genericPlugin;
	private TemplateRegistry registry;
	private EnvironmentManager manager;

    public GenericPluginPortalModule( GenericPlugin genericPlugin, TemplateRegistry registry, EnvironmentManager manager )
    {
    	this.registry = registry;
    	this.manager = manager;
        this.genericPlugin = genericPlugin;
    }


    @Override
    public String getId()
    {
        return NAME;
    }


    @Override
    public String getName()
    {
        return NAME;
    }


    @Override
    public File getImage()
    {
        return FileUtil.getFile( GenericPluginPortalModule.MODULE_IMAGE, this );
    }


    @Override
    public Component createComponent()
    {
        return new GenericPluginComponent (registry, manager, genericPlugin);
    }


    @Override
    public Boolean isCorePlugin()
    {
        return false;
    }
}
