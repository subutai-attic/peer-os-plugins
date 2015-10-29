package io.subutai.plugin.generic.ui.wizard;


import com.vaadin.annotations.Theme;
import com.vaadin.server.Page;
import com.vaadin.ui.*;

@Theme( "valo" )
public class ProfileCreationStep extends Panel
{
    public ProfileCreationStep (final Wizard wizard)
    {
        this.setSizeFull();

        final Panel panel = new Panel ("Create profile");
        final VerticalLayout panelContent = new VerticalLayout();
        panel.setContent (panelContent);
        panelContent.setSpacing(true);
        final TextField newProfile = new TextField ("Profile");
        newProfile.setInputPrompt("New profile name");
        newProfile.setRequired (true);
        panelContent.addComponent (newProfile);
        final HorizontalLayout buttonsGrid = new HorizontalLayout();
        buttonsGrid.setSpacing(true);
        final Button create = new Button("Create");
        create.addStyleName( "default" );
        final Button back = new Button ("Back");
        back.addStyleName( "default" );
		buttonsGrid.addComponent (back);
        buttonsGrid.addComponent (create);
        panelContent.addComponent( buttonsGrid );
        create.addClickListener (new Button.ClickListener()
        {
            @Override
            public void buttonClick (final Button.ClickEvent clickEvent)
            {
            	Notification notif = new Notification("");
                if (newProfile.getValue() == null || newProfile.getValue().isEmpty())
                {
					notif.setCaption("Please enter profile name");
					notif.setDelayMsec (2000);
					notif.show (Page.getCurrent());
                }
                else
                {
                	boolean exists = false;
                	for (int i = 0; i < wizard.getConfig().getProfiles().size(); ++i)
					{
						if (newProfile.getValue().equals (wizard.getConfig().getProfiles().get (i).getName()))
						{
							notif.setCaption("Profile already exists");
							notif.setDelayMsec (2000);
							notif.show (Page.getCurrent());
							exists = true;
							break;
						}
					}
					if (!exists)
					{
						wizard.getConfig().addProfile(newProfile.getValue());
						wizard.changeWindow(0);
						wizard.putForm();
						notif.setCaption("Profile " + newProfile.getValue() + " created");
						notif.setDelayMsec(2000);
						notif.show(Page.getCurrent());
					}
                }
            }
        });
        back.addClickListener(new Button.ClickListener()
        {
            @Override
            public void buttonClick(final Button.ClickEvent clickEvent)
            {
                wizard.changeWindow (0);
                wizard.putForm();
            }
        });
        this.setContent(panel);
    }
}
