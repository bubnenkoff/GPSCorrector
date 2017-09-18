module dataextractor;

import std.stdio;	
import std.array;
import std.algorithm;
import std.variant;
import std.conv;
import std.format;
import std.range;
import std.datetime;

import database;
import globals;
import config;
import ddb.postgres;
import datacalculator;
import datainserter;
import mysql;
import progress.bar;

class DBExtractor
{
    Config config;
	Database database;
    MyCalculator myCalculator;
    IDataWriter iDataWriter;

	this(Config config, Database database, IDataWriter iDataWriter)
	{
		this.config = config;
		this.database = database;
        this.iDataWriter = iDataWriter;
        myCalculator = new MyCalculator(database);
	}

	/*
		select_type -- every data, or some interval (now day)
		regionOfInterest -- we calculate statistic for every track to get understand if it's realted with some region and put statistic in table: regions_statistic
		by this stat we should have ability extract with getSingleTrackInfo all data or data for region of interest
	*/
	void processTracks(int [] AccountIds, string lastNDays, string processingDateInterval, int carIDs)
	{	
		try 
		{
			foreach(GPSAndSensor; getTablesGPSSensorList(AccountIds, carIDs))
			{
				//pass array [] of carGPSPoint from every track to calc function.
				iDataWriter.writeData(myCalculator.calcNearestRoadDistanceWithMetadataForEveryTrackPoint(getSingleTrackInfo(GPSAndSensor, lastNDays, processingDateInterval, carIDs), GPSAndSensor), GPSAndSensor); // We should pass GPSAndSensor for processing every single track and for function of calculation of NearestRoad
				markTrackAsProcessed(GPSAndSensor);
			}
		}
		catch(Exception e)
		{
			writeln(e.msg);
			return;
		}
	}   

    /* Возвращат массив с парами  GPSAndSensor которые необходимо обработать (все минус обработанные) для заданного CarID или AccountIds */
	GPSAndSensorTuple [] getTablesGPSSensorList(int [] AccountIds, int carIDs = 0) // getting GPS and Sensors as Pairs. Если указан carIDs то он главнее. 0 -- значит не указан, больше 0 значения
	{
		//dbInfo();
		string sql_select;

		if(carIDs > 0)
		{
			if(getProcessedTracks.canFind(carIDs)) 
				throw new Exception(`[INFO] No Tracks for Processing. Current Car ID is processed. See "processed_tables" table.`);
			else
				sql_select = `SELECT table_name FROM information_schema.tables where table_schema='` ~ config.remote.name ~ `' AND table_name IN ("HistorySensor_` ~ to!string(carIDs) ~ `", ` ~ ` "HistoryGPS_` ~ to!string(carIDs) ~ `")`;
		}

		else
		{
			// ZERO mean ALL. ALL NOT Processed. NO MANUAL MODE!
			if (AccountIds.length == 1 && AccountIds[0] == 0) // if no region specified ("only russia supported") and NO specified Accounts for Processing
			{
				auto neededId = setDifference(getAllTracksId.sort(), getProcessedTracks().sort()); // Вот тут выведет разность между ВСЕМИ и обработанными. А нужно вывести разницу между ВЫБРАННЫМИ и Обработанными. !!!
				if(neededId.count == 0)
					throw new Exception(`[INFO] No Tracks for Processing. All Tracks are Processed. See "processed_tables" table.`);
				auto gps_list = neededId.map!(a=> `historygps_` ~ to!string(a));
				writeln("Tracks for processing: ", gps_list.count);
				auto sensor_list = neededId.map!(a=> `historysensor_` ~ to!string(a));
				auto gps_and_sensor = (to!string(gps_list) ~ ", " ~ to!string(sensor_list)).replace(`[`,``).replace(`]`,``);
				sql_select = `SELECT table_name FROM information_schema.tables where table_schema='` ~ config.remote.name ~ `' AND table_name IN (` ~ gps_and_sensor ~ `)`;
			}
				
			else if(AccountIds.length >= 1 && AccountIds[0] != 0) // If AccountIds is specified and it's not ZERO
				sql_select = `SELECT table_name FROM information_schema.tables where table_name IN (` ~ manualSpecificationOfAccountId(AccountIds) ~ `);`;
			else
				throw new Exception ("Unknown parameters for processing");
		}
		ResultRange result = mysqlRemote.query(sql_select);
		auto MySQLTablesRange = result.array;
		result.close();
		auto historysensor = MySQLTablesRange.map!(a => a[0].coerce!string).filter!(a=>a.canFind("HistorySensor")); // регистр букв не менять, иначе выборка из MySQL на Linux работать не будет!
		auto historygps = MySQLTablesRange.map!(a => a[0].coerce!string).filter!(a=>a.canFind("HistoryGPS"));
		if(MySQLTablesRange.length == 0)
			throw new Exception("All Tracks are processed (function getTablesGPSSensorList return 0). See processed_tables");
		
		GPSAndSensorTuple gpsandsensortuple; 
		GPSAndSensorTuple [] gpsandsensortuples;
		Bar b = new Bar();
		b.message = {return "Processing GPS-Sensor List";};
		b.start();
		b.max = MySQLTablesRange.count/2; // because total count is gps+sensor
		TotalProcessdTracks = MySQLTablesRange.count/2; // because total count is gps+sensor // For global loggining
		b.suffix = {return b.percent.to!string ~ "/100";};

		foreach(sensor;historysensor)
		{
			gpsandsensortuple.gps = historygps.filter!(a=>a.canFind(sensor.split("_")[1])).front;
			gpsandsensortuple.sensor = sensor;
			//writefln("gpsandsensortuple.gps: %s | gpsandsensortuple.sensor: %s", gpsandsensortuple.gps, gpsandsensortuple.sensor);
			gpsandsensortuples ~= gpsandsensortuple; // globals.d
			//createColumnsIfNotExists(gpsandsensortuple); //MySQL Do not support this syntax
			b.next();
		}
		b.finish();
		writefln("[INFO] Total Number of pairs sensor-gps for processing: %s \n", gpsandsensortuples.count);
		return gpsandsensortuples; 
	}

	string manualSpecificationOfAccountId(int [] AccountIds) // возвращаем НЕ удаленные
	{
		// extract TracksID for this customer: SELECT * FROM `Car` WHERE Account IN (1,2,3);
		int [] tracksIDForCurrentCarOwner;
		//writeln("SELECT id FROM Car WHERE Account IN (" ~ to!string(AccountIds).replace(`[`,``).replace(`]`,``) ~ `) AND DeleteDate IS NOT NULL`);
		string sql = `SELECT id FROM Car WHERE Account IN (` ~ to!string(AccountIds).replace(`[`,``).replace(`]`,``) ~ `) AND DeleteDate IS NULL;`; //!!! сейчас выбираем все НЕ удаленные Было измненено на: IS NULL

		ResultRange range = query(mysqlRemoteAccs, sql); // Бывает так, что треки от машин не существуют в базе истории треков. Пример трек 12386 есть в БД аккаунтов (AccountId = 2648), но отсутствует в треках 
		auto answer = range.array;
		foreach(r;answer)
		{
			tracksIDForCurrentCarOwner ~= r[0].coerce!int; //id
			
		}
		//writeln(tracksIDForCurrentCarOwner);
		auto neededId = setDifference(tracksIDForCurrentCarOwner.sort(), getProcessedTracks().sort()); 
		if(neededId.count == 0)
			throw new Exception(`[INFO] No Tracks for Processing. All Tracks are Processed. See "processed_tables" table.`);

		return (neededId.map!(a=> "'HistorySensor_" ~ to!string(a) ~ `',`).array ~ neededId.map!(a=> "'HistoryGPS_" ~ to!string(a) ~ `',`).array).join.replaceLast(`,`,``);
	}

	void markTrackAsProcessed(GPSAndSensorTuple GPSAndSensor) // if we already processed tack we should mark it in mysql table processed_tables
	{
		try //ADD добавить вставку времени за которое был произведен пересчет
		{
			Prepared prepared = prepare(mysqlLocal, `INSERT IGNORE INTO processed_tables (processing_date, track_id) VALUES (?,?);`);
			string timeString = Clock.currTime.toISOExtString().replace("T"," ").split(`.`)[0];
			prepared.setArgs(timeString, GPSAndSensor.gps.split("_")[1]);
			prepared.exec();
		}
		catch(Exception e)
		{
			writefln("Impossible mark TrackID: %s as processed", GPSAndSensor.gps.split("_")[1]);
			writeln(e.msg);
		}
	}

	int [] getProcessedTracks()
	{
		string sql = `SELECT track_id FROM processed_tables;`;
		int [] processedTracks;
		ResultRange result = mysqlLocal.query(sql);
		foreach(track; result)
		{
			processedTracks ~= track[0].coerce!int;
		}
		// writeln("Already Processed Tracks ID: ", processedTracks, "\n");
		// readln;
		return processedTracks;
	}

	int [] getAllTracksId() // using only for processing ALL tracks
	{
		string sql = `SELECT table_name FROM information_schema.tables where table_schema='` ~ config.remote.name ~ `' AND table_name LIKE 'historygps%';`;
		// writeln(sql);
		int [] processedTracks;
		ResultRange result = mysqlRemote.query(sql);
		foreach(track; result)
		{
			processedTracks ~= to!int(track[0].coerce!string.split("_")[1]);
		}
		
		//writeln("All Tracks IDs: ", processedTracks);
		return processedTracks;
	}     


	/* 
		For every single track we do request to PostgreSQL database, extracting data in array of struct and then send data to PostgreSQL.
		We will do processing by 10 array of structs.
	*/
	carGPSPoint [] getSingleTrackInfo(GPSAndSensorTuple GPSAndSensor, string lastNDays, string processingDateInterval, int carIDs) // extract info for single track. Main - lat and lon
	{
		//writeln("Extracting information (id, lat, lon, etc) about single track");
		string sql;
		if(lastNDays.length > 0)
            sql = `SELECT
            HistoryGPS_12387.Id,
            HistoryGPS_12387.RecordDate,
            FLOOR(velocity),
            Lat,
            Lon
            FROM HistoryGPS_12387 LEFT JOIN HistorySensor_12387 ON HistoryGPS_12387.Id = HistorySensor_12387.HistoryId WHERE HistorySensor_12387.Sensor = 1 AND HistoryGPS_12387.RecordDate >= (CURDATE() - INTERVAL ` ~ to!string(lastNDays) ~ ` DAY)  AND HistorySensor_12387.Value = 1 AND Velocity>0  AND LAT IS NOT NULL AND LON IS NOT NULL`;
		
		if(processingDateInterval.length > 0) // делаем выбор в пользу processingDateInterval
		{
            string startdate = processingDateInterval.split("-")[0].replace(`.`,`-`);
            // writeln(startdate);
            string enddate = processingDateInterval.split("-")[1].replace(`.`,`-`);
            sql = `SELECT
            HistoryGPS_12387.Id,
            HistoryGPS_12387.RecordDate,
            FLOOR(velocity),
            Lat,
            Lon
            FROM HistoryGPS_12387 LEFT JOIN HistorySensor_12387 ON HistoryGPS_12387.Id = HistorySensor_12387.HistoryId WHERE HistorySensor_12387.Sensor = 1 AND HistoryGPS_12387.RecordDate BETWEEN '` ~ startdate ~ `' AND '` ~ enddate ~ `' AND HistorySensor_12387.Value = 1 AND Velocity>0  AND LAT IS NOT NULL AND LON IS NOT NULL`;
		}

		ResultRange result = mysqlRemote.query(sql.replace(`HistoryGPS_12387`, GPSAndSensor.gps).replace(`HistorySensor_12387`, GPSAndSensor.sensor));
		auto myPointsLonLat = result.array;
		if(myPointsLonLat.length == 0) // if nothing in DB no reason to continue
			return null; 
		result.close();
		string itemName = format("GPS: %s | Sensor: %s", GPSAndSensor.gps, GPSAndSensor.sensor);
		carGPSPoint [] cargpspoints = new carGPSPoint[](myPointsLonLat.length); // collect data for single track. After filling iterate and do requests to PG
		// writeln("myPointsLonLat: ", myPointsLonLat.count);
		Bar b = new Bar();
		b.message = { return itemName; };
		b.start();
		b.max = myPointsLonLat.count;
		b.suffix = {return b.percent.to!string ~ "/100";};
		foreach(i, point;myPointsLonLat)
		{					
			carGPSPoint cargpspoint;
			cargpspoint.id = point[0].coerce!ulong;
			cargpspoint.recordDate = DateTime.fromSimpleString(point[1].coerce!string).toISOExtString(); // some magic to get string in 2016-10-31T15:37:24 format
			
			if(!point.isNull( 2 ) && !point.isNull( 3 ) && !point.isNull( 4 ))
			{
				cargpspoint.velocity = point[2].coerce!int;
				cargpspoint.lat = point[3].coerce!double;
				cargpspoint.lon = point[4].coerce!double;
				cargpspoints[i] = cargpspoint;
				b.next();
			}

			else
				continue;
		}

		b.finish();

		return cargpspoints;
	}


}