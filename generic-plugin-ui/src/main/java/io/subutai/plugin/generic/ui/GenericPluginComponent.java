package io.subutai.plugin.generic.ui;


import com.vaadin.ui.CustomComponent;
import com.vaadin.ui.VerticalLayout;
import io.subutai.core.environment.api.EnvironmentManager;
import io.subutai.core.registry.api.TemplateRegistry;
import io.subutai.plugin.generic.ui.wizard.Wizard;


public class GenericPluginComponent extends CustomComponent
{
    private final Wizard wizard;

    public GenericPluginComponent(TemplateRegistry registry, EnvironmentManager manager)
    {
        this.wizard = new Wizard (registry, manager);

        setSizeFull();
        VerticalLayout content = new VerticalLayout();
        content.setSpacing( true );
        content.setSizeFull();
        content.addComponent (this.wizard.getContent());
        setCompositionRoot( content );
    }
}
