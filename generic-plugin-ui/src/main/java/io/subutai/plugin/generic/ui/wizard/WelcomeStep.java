package io.subutai.plugin.generic.ui.wizard;


import java.util.List;

import com.vaadin.server.FileResource;
import com.vaadin.server.Page;
import com.vaadin.shared.ui.label.ContentMode;
import com.vaadin.ui.Alignment;
import com.vaadin.ui.Button;
import com.vaadin.ui.GridLayout;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Label;
import com.vaadin.ui.Notification;
import com.vaadin.ui.Panel;

import io.subutai.common.util.FileUtil;
import io.subutai.plugin.generic.api.GenericPlugin;
import io.subutai.plugin.generic.api.model.Operation;
import io.subutai.plugin.generic.api.model.Profile;
import io.subutai.plugin.generic.ui.GenericPluginPortalModule;


public class WelcomeStep extends Panel
{
    public WelcomeStep( final Wizard wizard, final GenericPlugin genericPlugin )
    {
        setSizeFull();

        GridLayout grid = new GridLayout( 10, 6 );
        grid.setSpacing( true );
        grid.setMargin( true );
        grid.setSizeFull();

        Label welcomeMsg =
                new Label( "<center><h2>Welcome to Generic Plugin, execute your custom commands on containers </h2>" );
        welcomeMsg.setContentMode( ContentMode.HTML );
        grid.addComponent( welcomeMsg, 3, 1, 6, 2 );

        Label logoImg = new Label();
        logoImg.setIcon( new FileResource( FileUtil.getFile( GenericPluginPortalModule.MODULE_IMAGE, this ) ) );
        logoImg.setContentMode( ContentMode.HTML );
        logoImg.setHeight( 150, Unit.PIXELS );
        logoImg.setWidth( 150, Unit.PIXELS );
        grid.addComponent( logoImg, 1, 3, 2, 5 );

        HorizontalLayout buttonsGrid = new HorizontalLayout();
        final Button createProfile = new Button( "Create profile" );
        createProfile.addStyleName( "default" );
        final Button configureOperations = new Button( "Configure operations" );
        configureOperations.addStyleName( "default" );
        final Button manageContainers = new Button( "Manage containers" );
        manageContainers.addStyleName( "default" );
        buttonsGrid.addComponent( createProfile );
        buttonsGrid.addComponent( configureOperations );
        buttonsGrid.addComponent( manageContainers );
        grid.addComponent( buttonsGrid, 6, 4, 6, 4 );
        grid.setComponentAlignment( buttonsGrid, Alignment.BOTTOM_RIGHT );

        createProfile.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( final Button.ClickEvent clickEvent )
            {
                wizard.changeWindow( 1 );
                wizard.putForm();
            }
        } );

        configureOperations.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( final Button.ClickEvent clickEvent )
            {
                List<Profile> profiles = genericPlugin.getProfiles();
                if ( profiles.isEmpty() )
                {
                    show( "Please create profiles first" );
                }
                else
                {
                    wizard.changeWindow( 2 );
                    wizard.putForm();
                }
            }
        } );

        manageContainers.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( final Button.ClickEvent clickEvent )
            {
                List<Profile> profiles = genericPlugin.getProfiles();
                if ( profiles.isEmpty() )
                {
                    show( "Please create profiles first" );
                }
                else if ( wizard.getManager().getEnvironments().isEmpty() )
                {
                    show( "Please create environments first" );
                }
                else
                {
                    boolean exists = false;
                    for ( Profile p : profiles )
                    {
                        List<Operation> operations = genericPlugin.getProfileOperations( p.getId() );
                        if ( !operations.isEmpty() )
                        {
                            exists = true;
                            break;
                        }
                    }
                    if ( !exists )
                    {
                        show( "Please create at least one operation on one of profiles" );
                    }
                    wizard.changeWindow( 3 );
                    wizard.putForm();
                }
            }
        } );

        setContent( grid );
    }


    private void show( String notification )
    {
        Notification notif = new Notification( notification );
        notif.setDelayMsec( 2000 );
        notif.show( Page.getCurrent() );
    }
}