module stat;

import std.stdio;	
import std.array;
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

import database;
import globals;
import dataextractor;;
import config;
import ddb.postgres;
import mysql;
import progress.bar;
import vibe.data.json;
import vibe.web.web;

class MyStat
{
	Config config;
	DBExtractor dbExtractor; // getProcessedTracks is placed in trackprocessing

	this(Config config, DBExtractor dbExtractor)
	{
		this.config = config;
		this.dbExtractor = dbExtractor;
	}

	void calcTrackRegionStat(carGPSPoint [] cargpspoints, GPSAndSensorTuple GPSAndSensor) // how many times car spend in region
	{
		struct CarRegion
		{
			ulong trackid;
			double velocity;
			string recordDate;
			string regionName;
			string okato_code;
		}
		CarRegion carRegion;
		CarRegion [] carRegions;

		string sql = `SELECT Name, okato_code FROM adm4_region_f WHERE ST_Contains(ST_SetSRID(geom, 4326), ST_SetSRID(ST_POINT(37.72308, 55.47957), 4326));`; // select region name for point
		writeln("cargpspoints count: ", cargpspoints.count);

		Bar b = new Bar();
		b.message = { return "Calc Region-Point Statistic (usually daily stat)"; };
		b.start();
		b.max = cargpspoints.count;
		foreach(i, cargpspoint; cargpspoints)
		{
			auto cmd = new PGCommand(pgconnection, sql.replace(`37.72308`, to!string(cargpspoint.lon)).replace(`55.47957`, to!string(cargpspoint.lat))); // for for better accuracy format %f should be used
			try
			{
				auto answer = cmd.executeRow; // region name
				carRegion.trackid = to!ulong(GPSAndSensor.gps.split("_")[1]);
				carRegion.velocity = cargpspoint.velocity;
				carRegion.recordDate = cargpspoint.recordDate;
				carRegion.regionName = to!string(answer[0]); // or answer.front[0] ? 
				carRegion.okato_code = to!string(answer[1]);
				carRegions ~= carRegion;
				b.next();
			}

			catch(Exception e) // DB return empty result
			{
				writefln("The Point outside Russia. PG return NULL for Lat: %s Lon: %s", cargpspoint.lat, cargpspoint.lon);
				writeln(e.msg);
				continue;
			}
			b.finish();

	  }

		//string insertsql = `INSERT IGNORE INTO regions_statistic(recordDate_trackid, recordDate, regionName,trackid, velocity) VALUES('myrecd', 'mydate', 'myregname', mytrackid, myvel)`;
		writeln("\nInsert region-track statistic to MySQL");
		foreach(i, item; carRegions)	// processing after collection
		{
			string insertsql = `INSERT IGNORE INTO regions_statistic(recordDate_trackid, recordDate, regionName, trackid, velocity, okato_code) VALUES('` ~ to!string(item.recordDate) ~ "_" ~ to!string(item.trackid) ~ `', '` ~ to!string(item.recordDate) ~ `', '` ~ to!string(item.regionName) ~ `', ` ~ to!string(item.trackid) ~ `, ` ~ to!string(item.velocity) ~ `, ` ~  to!string(item.okato_code) ~  `)`;
			mysqlLocal.exec(insertsql);
			// writeln(insertsql);
			if(i%10==0)
				write(".");

		}

	}

	void roadStat()
	{
        double middle_val; // среднее
		double median_val; // медиана
		double std_val; // среднее квадратичное STDDEV_SAMP
		double min_dist90; // 90% точек
		int totalInMotion; // всего в движении
		int total_count_lt5;
		int total_count_lt10;
		int total_count_lt15;
		int total_count_lt20;
		int total_count_lt25;
		int total_count_lt30;
		int total_count_lt40;
		int total_count_lt50;
		string maxRecordDate; // максимальная и минимальные даты
		string minRecordDate; // максимальная и минимальные даты
		string recordDateDiff; // in days

		string total; // work as a hack
		int gps_all_count; // all GPS points even where is Sensor off

		writefln("ID:   | Middle    | Median    | std       | 5m        | 10m       | 15m       | 20m       | 25m       | 30m       | 40m       | 50m      |min_dist90 | MinRecordDate          | MaxRecordDate          | DateDiff |Total_in_motion |  Total  ");
		foreach(track; dbExtractor.getProcessedTracks)
		{
			try
				{
				//Average:----------------------------------------------
					string xxx = "HistoryGPS_" ~ to!string(track); // because Linux
					// ResultRange range = query(mysqlLocal, "SELECT avg(road_dist) FROM ` ~ xxx ~ `;");
					// foreach(r;range)
					// {
					// 	middle_val = r[0].coerce!int; //id
					// }
					Row answer = queryRow(mysqlLocal, `SELECT avg(road_dist) FROM ` ~ xxx ~ `;`);
					middle_val = answer[0].coerce!double;

				//Median:--------------------------------------------
					Row answer1 = queryRow(mysqlLocal, `SELECT CEIL(COUNT(*)/2) FROM ` ~ xxx ~ `;`);
					Row answer2 = queryRow(mysqlLocal, `SELECT max(road_dist) FROM (SELECT road_dist FROM ` ~ xxx ~ ` ORDER BY road_dist limit ` ~ to!string(answer1[0].coerce!int) ~ `) x`);
					median_val = answer2[0].coerce!double;
				
				//Квадратичное:--------------------------------------------
					Row answer3 = queryRow(mysqlLocal, `SELECT STDDEV_SAMP(road_dist) FROM ` ~ xxx ~ `;`);
					std_val = answer3[0].coerce!double;

				//Total In Motion:--------------------------------------------
					Row answer4 = queryRow(mysqlLocal, `SELECT COUNT(*) FROM ` ~ xxx ~ `;`);
					totalInMotion = answer4[0].coerce!int;
					
				//Count n meters ----------------------------------
					string sql = `SELECT (SELECT COUNT(*) FROM historygps_23321 WHERE road_dist<5) as lt5, 
									(SELECT COUNT(*) FROM historygps_23321 WHERE road_dist<10) as lt10,
									(SELECT COUNT(*) FROM historygps_23321 WHERE road_dist<15) as lt15,
									(SELECT COUNT(*) FROM historygps_23321 WHERE road_dist<20) as lt20,
									(SELECT COUNT(*) FROM historygps_23321 WHERE road_dist<25) as lt25,
									(SELECT COUNT(*) FROM historygps_23321 WHERE road_dist<30) as lt30,
									(SELECT COUNT(*) FROM historygps_23321 WHERE road_dist<40) as lt40,
									(SELECT COUNT(*) FROM historygps_23321 WHERE road_dist<50) as lt50`.replace(`historygps_23321`, xxx);

					Row answer5 = queryRow(mysqlLocal, sql);
					total_count_lt5 = answer5[0].coerce!int;
					total_count_lt10 = answer5[1].coerce!int;
					total_count_lt15 = answer5[2].coerce!int;
					total_count_lt20 = answer5[3].coerce!int;
					total_count_lt25 = answer5[4].coerce!int;
					total_count_lt30 = answer5[5].coerce!int;
					total_count_lt40 = answer5[6].coerce!int;
					total_count_lt50 = answer5[7].coerce!int;

				//90%:--------------------------------------------
					Row answer6 = queryRow(mysqlLocal, `SELECT CEIL(COUNT(*) * 0.9) FROM ` ~ xxx ~ `;`);
					Row answer7 = queryRow(mysqlLocal, `SELECT max(road_dist) FROM (SELECT road_dist FROM ` ~ xxx ~ ` ORDER BY road_dist ASC limit ` ~ to!string(answer6[0].coerce!int) ~ `) x`); //ASC !
					min_dist90 = answer7[0].coerce!double;


				// Max/min dates:
					Row answer9 = queryRow(mysqlLocal, `SELECT max(RecordDate) as maxRecordDate, min(RecordDate) as minRecordDate, datediff(max(RecordDate), min(RecordDate)) as datediff FROM historygps_23314`.replace(`historygps_23314`, xxx));
					maxRecordDate = answer9[0].coerce!string;
					minRecordDate = answer9[1].coerce!string;
					recordDateDiff = answer9[2].coerce!string;

			
				// Count from *REMOTE* server where sensor is off
					if(track != 1) // 1 we have only in lical DB in global we should skip it
					{
						string sql1 =  `SELECT COUNT(*) FROM ` ~ xxx ~ ` WHERE RecordDate >='` ~ DateTime.fromSimpleString(minRecordDate).toISOExtString.replace(`T`, ` `) ~  `' AND RecordDate <'` ~ DateTime.fromSimpleString(maxRecordDate).toISOExtString.replace(`T`, ` `) ~ `'`;
						Row answer10 = queryRow(mysqlRemote, sql1);  // we will use recordDateDiff to calc period
						gps_all_count = answer10[0].coerce!int;
						// writeln(sql1);
					}
					
					if(track == 1) // some hack to get total to back
					{
						total = format("%5s | %s   | %7s   | %7s   | %7s | %7s | %7s | %7s | %7s | %7s | %7s | %7s | %s   | %s   | %s   | %s     | %12s   | %s   ", "ALL", middle_val, median_val, std_val, total_count_lt5, total_count_lt10, total_count_lt15, total_count_lt20, total_count_lt25, total_count_lt30, total_count_lt40, total_count_lt50, min_dist90, minRecordDate, maxRecordDate, recordDateDiff, totalInMotion, gps_all_count);
						continue;
					}
					else
					{
						writefln("%s | %s   | %7s   | %7s   | %7s   | %7s   | %7s   | %7s   | %7s   | %7s   | %7s   | %7s   | %s   | %7s   | %7s   | %4s     | %12s   | %s   ", track, middle_val, median_val, std_val, total_count_lt5, total_count_lt10, total_count_lt15, total_count_lt20, total_count_lt25, total_count_lt30, total_count_lt40, total_count_lt50, min_dist90, minRecordDate, maxRecordDate, recordDateDiff, totalInMotion, gps_all_count);
					}
				}
				catch(Exception e)
				{
					continue; // if we got any exception skip the track
				}
				
		}

			writeln(total); // Make total last line

	}

    Json DoTrackSpeedAnalyze(int trackid, string endDateTime)
    {
        // if()
        // writeln("trackid: ", trackid);
        return Json.emptyObject;
    }

}