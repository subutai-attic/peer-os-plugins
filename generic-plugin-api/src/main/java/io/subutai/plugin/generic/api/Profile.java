package io.subutai.plugin.generic.api;

import java.util.ArrayList;


public class Profile extends Object
{
	private String name;
	private ArrayList <String> operations;
	private ArrayList <String> commands;
//	private ArrayList <String> templates;
	private ArrayList <String> cwds;
	private ArrayList <Float> timeOuts;
	private ArrayList <Boolean> daemons;
	public Profile (String name)
	{
		this.name = name;
		this.operations = new ArrayList<>();
		this.commands = new ArrayList<>();
//		this.templates = new ArrayList<>();
		this.cwds = new ArrayList<>();
		this.timeOuts = new ArrayList<>();
		this.daemons = new ArrayList<>();
	}
	public void addOperation (String operation, String command/*, String template*/, String cwd, Float timeOut, Boolean daemon)
	{
		this.operations.add (operation);
		this.commands.add (command);
		//this.templates.add (template);
		this.cwds.add (cwd);
		this.timeOuts.add (timeOut);
		this.daemons.add (daemon);
	}

	public void deleteOperation (String operation)
	{
		for (int i = 0; i < this.operations.size(); ++i)
		{
			if (this.operations.get (i) == operation)
			{
				this.operations.remove(i);
				this.commands.remove (i);
				//this.templates.remove (i);
				this.cwds.remove (i);
				this.timeOuts.remove (i);
				this.daemons.remove (i);
			}
		}
	}

	public String getName()
	{
		return this.name;
	}

	public ArrayList <String> getOperations()
	{
		return this.operations;
	}

	public ArrayList <String> getCommands()
	{
		return this.commands;
	}

	public ArrayList <String> getCwds()
	{
		return this.cwds;
	}

	public ArrayList <Float> getTimeouts()
	{
		return this.timeOuts;
	}

	public ArrayList <Boolean> getDaemons()
	{
		return this.daemons;
	}

/*	public ArrayList <String> getTemplates()
	{
		return this.templates;
	}*/


	public int getIndex (String operation)
	{
		for (int i = 0; i < this.operations.size(); ++i)
		{
			if (this.operations.get (i) == operation)
			{
				return i;
			}
		}
		return -1;
	}

	@Override
	public boolean equals (Object p)
	{
		if (p == null)
		{
			return false;
		}
		if (p == this)
		{
			return true;
		}
		if (!(p instanceof Profile))
		{
			return false;
		}
		Profile other = (Profile)p;
		return this.name.equals (other.name);
	}
}
