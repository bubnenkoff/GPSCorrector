module config;
import std.string;
import std.stdio;
import std.path;
import std.file;

import dini;
//import globals;

struct DBConfig
{
	string name;
	string user;
	string password;
	string host;
	ushort port;
	//

	this(Ini ini)
	{
		this.name = ini.getKey("dbname");
		this.user = ini.getKey("dbuser");
		this.password = ini.getKey("dbpassword");
		this.host = ini.getKey("dbhost");
		this.port = ini.getKey("dbport").to!ushort;
	}

}


class Config
{
    DBConfig postgre;
	DBConfig local;
    DBConfig remote;
    DBConfig accounts;

	this()
	{
		string configPath = buildPath((thisExePath[0..((thisExePath.lastIndexOf("\\"))+1)]), "config.ini");
		if (!exists(configPath))
		{
			throw new Exception("config.ini do not exists");
		}
		auto ini = Ini.Parse(configPath);

		this.postgre = DBConfig( ini["postgres"] );
		this.local = DBConfig( ini["mysql-local"]);
		this.remote = DBConfig( ini["mysql-remote-history"]);
		this.accounts = DBConfig( ini["mysql-remote-accs"]);

	}


}