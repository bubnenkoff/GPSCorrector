import vibe.core.core;
import vibe.web.rest;
import vibe.http.server;
import vibe.http.router;
import std.datetime;
import std.stdio;
import core.thread;
import std.getopt;
import std.file : remove, exists;
import std.experimental.logger;
import stat;
import misc;
import globals;
import config;
import database;
import dataextractor;
import datainserter;
import datacalculator;

static this()
{
	removeOldLog(); // misc
	fLogger = new FileLogger(logName); 
}

void main(string[] args)
{

	if(args.length < 2)
	{
		writeln("No parameters specified see --help");
		return;
	}

	int [] AccountIds; // we can specify a list of AccountIds that we want to process // Якобс имеет ID 5794
	bool calcRoadStat; 
	string lastNDayes; // in days for substitition in SELECT single track function
	string processingDateInterval; 
	int carIDs; // separate cars // Только по одиночке!!!

	try
	{
		auto helpInformation = getopt(args, 
		//std.getopt.config.required, 
		"acs|a", "Specify AccountIds for processing. Example: -a=1,2,3 or --acs=1,2,3. -a=0 Process All", &AccountIds,
		"ld|d", "Interval of last N dayes. Example: -d=7. -d=0 Process all periods", &lastNDayes,
		"roadstat|r", "Calc roadstat (Middle, Median, 5m, 10m, 15m ...)", &calcRoadStat,
		"int|i", "Interval of dates for processing. Example: -i=2017.01.01-2018.01.01", &processingDateInterval, 
		"carid|c", "Car ID -c=12387. Multiple values DO NOT support", &carIDs
		
		); //Only the option directly following std.getopt.config.required is required. 
		
		if (helpInformation.helpWanted)
		{
			defaultGetoptPrinter("RoadPoint Application. Example of run: roadpoint.exe -c=12387 -i=2017.05.29-2017.06.01",
			helpInformation.options);
			return;
		}	
	}
	catch(GetOptException e)
	{
		writeln(e.msg);
		writeln("See -h for command line parameters");
		return;
	}

	auto TotalStartTime = MonoTime.currTime;	
	Config config = new Config();
	Database database = new Database(config);

	DataWriterFactory dataWriterFactory = new DataWriterFactory(database, config);
	IDataWriter iDataWriter = dataWriterFactory.getWriter();
	DBExtractor dbExtractor = new DBExtractor(config, database, iDataWriter);


	if (lastNDayes == "0") // if specify no, process ALL tracks. Small hack for making long period
	{
		writeln("Processing All Date may take a lot of time!!! Processing will start in 5 seconds!");
		Thread.sleep(5.seconds);
		lastNDayes = "1000";
	}

	if(calcRoadStat && AccountIds.length == 0) // Statistic
	{
		MyStat mystat = new MyStat(config, dbExtractor);
		mystat.roadStat();
	}

	if(AccountIds.length > 0 && processingDateInterval.length == 0)
	{
		writeln(processingDateInterval.length);
		writeln(lastNDayes.length);
		writeln(AccountIds.length);
		writeln("You specified AccountID, but not specified dayes interval");
		return;
	}

	if(AccountIds.length && carIDs>0)
	{
		writeln("Can't specify Account AND CarID. Only one. Not both");
		return;
	}

	if(lastNDayes.length > 0 && processingDateInterval.length > 0)
	{
		writeln("Can't use lastNDayes and processingDateInterval together");
		return;
	}

	if(AccountIds.length > 0 && lastNDayes.length > 0 || processingDateInterval.length > 0) // Processing
	{
		dbExtractor.processTracks(AccountIds, lastNDayes, processingDateInterval, carIDs); // possible values: sql_all sql_15_minute_data // select type, region of interest
	}

	auto TotalEndTime = MonoTime.currTime;
	auto totalDuration = TotalEndTime - TotalStartTime;
	writefln("[INFO] Total tracks: %s. Total Processing Time: %s minutes", TotalProcessdTracks, totalDuration.total!"minutes");
	fLogger.logf("Total tracks: %s. Total Processing Time: %s minutes", TotalProcessdTracks, totalDuration.total!"minutes");

}
