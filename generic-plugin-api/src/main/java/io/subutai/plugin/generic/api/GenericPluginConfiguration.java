package io.subutai.plugin.generic.api;


import io.subutai.common.peer.ContainerHost;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;

public class GenericPluginConfiguration
{
	private ArrayList <Profile> profiles = new ArrayList<>(); // TODO: delete this array
	private ContainerHost host = null;
	private String command = null;
	private String cwd = null;
	private Float timeOut = null;
	private Boolean daemon = null;
	public void addProfile (String newProfile) // TODO: add to database instead
	{
		Profile prof = new Profile(newProfile);
		this.profiles.add (prof);
		Collections.sort(this.profiles, new Comparator<Profile>()
		{
			@Override
			public int compare(Profile p1, Profile p2)
			{

				return p1.getName().compareTo(p2.getName());
			}
		});
	}

	public Profile findProfile (String profileName) // TODO: find in database instead
	{
		for (Profile p : this.profiles)
		{
			if (p.getName().equals (profileName))
			{
				return p;
			}
		}
		return null;
	}

	public void replaceProfile (Profile newProfile) // TODO: replace in database instead
	{
		boolean found = false;
		for (Iterator<Profile> it = this.profiles.iterator(); it.hasNext();)
		{
			Profile p = it.next();
			if (p.getName().equals (newProfile.getName()))
			{
				it.remove();
				found = true;
				break;
			}
		}
		if (found)
		{
			this.profiles.add(newProfile);
			Collections.sort(this.profiles, new Comparator<Profile>()
			{
				@Override
				public int compare(Profile p1, Profile p2)
				{
					return p1.getName().compareTo(p2.getName());
				}
			});
		}
	}

	public void setHost (ContainerHost host)
	{
		this.host = host;
	}


	public ContainerHost getHost()
	{
		return this.host;
	}


	public void setInstruction (Profile profile, String operation) // TODO: search in database instead
	{
		for (int i = 0; i < profile.getOperations().size(); ++i)
		{
			if (profile.getOperations().get (i) == operation)
			{
				this.command = profile.getOperations().get (i);
				this.cwd = profile.getCwds().get (i);
				this.timeOut = profile.getTimeouts().get (i);
				this.daemon = profile.getDaemons().get (i);
				break;
			}
		}
	}

	public String getCommand()
	{
		return this.command;
	}


	public String getCwd()
	{
		return this.cwd;
	}


	public Float getTimeOut()
	{
		return this.timeOut;
	}


	public Boolean getDaemon()
	{
		return this.daemon;
	}


	public ArrayList <Profile> getProfiles()
	{
		return this.profiles;
	}
}
