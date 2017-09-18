import std.stdio;
import std.string;
// import std.file;
import std.datetime;
import core.thread;
import ddb.postgres;
import mysql;
import vibe.d;
import config;
import std.typecons;
import std.algorithm;
import std.variant;
import std.conv;
import std.format;
import core.memory;
import std.range;
import std.array;
import std.datetime;
import std.experimental.logger;
import std.uni : toLower;
import progress.bar;

import globals;

Connection mysqlLocal;
Connection mysqlRemote; // History DB
Connection mysqlRemoteAccs; // Users Accounts
PGConnection pgconnection;

class Database
{
	Config config;

	this(Config config)
	{
		this.config = config;

		try
		{
			auto postgre = config.postgre;
			pgconnection = new PGConnection(["host" : postgre.host, "user": postgre.user, "password" : postgre.password, "database" : postgre.name, "port" : postgre.port.to!string]);
		}

		catch(Exception e)
		{
			writeln("[ERROR] Can't connect to PostgreSQL Server. Check connection credentials");
			throw new MySQLException(e.msg);
		}

		mysqlLocal = MySQLConnect( config.local, "");
		mysqlRemote = MySQLConnect( config.remote, "");
		mysqlRemoteAccs = MySQLConnect( config.accounts, "");
		
	}
	
	Connection MySQLConnect(ref DBConfig config, string errMsg)
	{
		auto pool = new MySQLPool(config.host, config.user, config.password, config.name, config.port); 
		try
		{
			auto conn = pool.lockConnection();
			return conn;
		}
		catch(MySQLException e)
		{
			writeln( errMsg );
			throw new MySQLException(e.msg);
		}
	}


	void dbInfo()
	{
		string checkDBVersion = `show variables where Variable_name = 'version';`;
		auto dbversion = mysqlRemote.queryRow(checkDBVersion);
		if((dbversion[1].coerce!string).canFind("MariaDB"))
			writefln("Current DataBase version: %s", dbversion[1]);
		else
			throw new Exception(`[ERROR]. Can't add columns to "historygps" table. Only MariaDB support ADD COLUMN IF NOT EXISTS syntax`);
		writeln("Using DB: ",  config.local.name);
		Thread.sleep(3.seconds);
	}
/+
	/* MariaDB Only!!! */
	void createColumnsIfNotExists(GPSAndSensorTuple gpsandsensortuple) // before processing of every column we should columns should be created 
	{
		//	example: `ALTER TABLE historygps_12387 ADD COLUMN IF NOT EXISTS road_dist double DEFAULT 0`;
		string sql1 = `ALTER TABLE ` ~ gpsandsensortuple.gps ~ ` ADD COLUMN IF NOT EXISTS road_dist double DEFAULT 0;`;
		string sql2 = `ALTER TABLE ` ~ gpsandsensortuple.gps ~ ` ADD COLUMN IF NOT EXISTS nearest_lat double DEFAULT 0;`;
		string sql3 = `ALTER TABLE ` ~ gpsandsensortuple.gps ~ ` ADD COLUMN IF NOT EXISTS nearest_lon double DEFAULT 0;`;

		try
		{
			mysqlconnection.exec(sql1);
			mysqlconnection.exec(sql2);
			mysqlconnection.exec(sql3);
		}

		catch(Exception e)
		{
			writeln(e.msg);
			writeln("[ERROR] on adding columns to MySQL/MariaDB");
		}

	}
+/
	void createLocalDBStruct(GPSAndSensorTuple gpsandsensortuple)
	{

		string sql = `CREATE TABLE IF NOT EXISTS ` ~ gpsandsensortuple.gps ~ `(
		id bigint(11) primary key AUTO_INCREMENT,
		nearest_lat DOUBLE DEFAULT NULL,
		nearest_lon DOUBLE DEFAULT NULL,
		velocity INTEGER DEFAULT NULL,
		roadtype TINYTEXT NULL,
		roadmaxspeed DOUBLE DEFAULT NULL,
		RecordDate DATETIME DEFAULT NULL,
		inCity INTEGER  DEFAULT NULL
		) ENGINE=InnoDB;`;

		// writeln(sql);
		try
		{
			mysqlLocal.exec(sql);
			writeln("Table in *LOCAL* DB created: ", gpsandsensortuple.gps);
		}

		catch(Exception e)
		{
			writeln("[ERROR] on creation table in MySQL/MariaDB");
			writeln(e.msg);
		}

	}




}