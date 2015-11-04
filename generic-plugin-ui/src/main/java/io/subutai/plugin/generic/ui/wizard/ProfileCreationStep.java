package io.subutai.plugin.generic.ui.wizard;


import com.google.gwt.thirdparty.guava.common.base.Strings;
import com.vaadin.annotations.Theme;
import com.vaadin.ui.Button;
import com.vaadin.ui.HorizontalLayout;
import com.vaadin.ui.Notification;
import com.vaadin.ui.Panel;
import com.vaadin.ui.TextField;
import com.vaadin.ui.VerticalLayout;

import io.subutai.plugin.generic.api.GenericPlugin;


@Theme( "valo" )
public class ProfileCreationStep extends Panel
{
//    private GenericPlugin genericPlugin;
    private Panel panel;
    private VerticalLayout panelContent;
    private TextField newProfile;
    private HorizontalLayout buttonsGrid;
    private Button create;
    private Button back;

    public ProfileCreationStep( final Wizard wizard, final GenericPlugin genericPlugin )
    {
        this.setSizeFull();

        panel = new Panel( "<h2>Create profile<h2>" );
        panelContent = new VerticalLayout();
        panel.setContent( panelContent );
        panelContent.setSpacing( true );

        newProfile = new TextField( "Profile" );
        newProfile.setInputPrompt( "New profile name" );
        newProfile.setRequired( true );
        panelContent.addComponent( newProfile );

        buttonsGrid = new HorizontalLayout();
        buttonsGrid.setSpacing( true );

        create = new Button( "Create" );
        create.addStyleName( "default" );

        back = new Button( "Back" );
        back.addStyleName( "default" );
        buttonsGrid.addComponent( back );
        buttonsGrid.addComponent( create );
        panelContent.addComponent( buttonsGrid );

        create.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( final Button.ClickEvent clickEvent )
            {
                if ( Strings.isNullOrEmpty( newProfile.getValue() ) )
                {
                    show( "Please enter profile name" );
                }
                else
                {
                    genericPlugin.saveProfile( newProfile.getValue() );
                    wizard.changeWindow( 0 );
                    wizard.putForm();
                }
            }
        } );

        back.addClickListener( new Button.ClickListener()
        {
            @Override
            public void buttonClick( final Button.ClickEvent clickEvent )
            {
                wizard.changeWindow( 0 );
                wizard.putForm();
            }
        } );

        this.setContent( panel );
    }


    private void show( String notification )
    {
        Notification.show( notification );
    }
}
