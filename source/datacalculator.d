module datacalculator;

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
import ddb.postgres;
import mysql;
import progress.bar;

class MyCalculator
{
	Database database;

	this(Database database)
	{
		this.database = database;
	}

	NearestRoadDistance [] calcNearestRoadDistanceWithMetadataForEveryTrackPoint(carGPSPoint [] cargpspoints, GPSAndSensorTuple GPSAndSensor)
	{
		auto startTime = MonoTime.currTime;
        NearestRoadDistance nearestroaddistance;
        NearestRoadDistance [] nearestroaddistances;

		if(cargpspoints.length == 0)  //if no data
		{
			writefln("[NO DATA] for Track: %s has no data from DB. Skipping", GPSAndSensor.gps);
			return null;
		}

		database.createLocalDBStruct(GPSAndSensor); // for every track create table

            try
            {		
                writef("\nTrack %s. Total Points: %s. Processing", GPSAndSensor.gps, cargpspoints.count);
                Bar b = new Bar();
                b.message = { return "Calc Nearest Road Distance For Every Track Point"; };
                b.start();
                b.max = cargpspoints.count;

                Prepared prepared = prepare(mysqlLocal, `INSERT IGNORE INTO ` ~ GPSAndSensor.gps ~ ` (id, roadtype, inCity, roadmaxspeed, nearest_lat, nearest_lon, RecordDate, velocity) VALUES (?,?,?,?,?,?,?,?)`);

                string sql = `SELECT osm_id, type, maxspeed, ST_asText(ST_ClosestPoint(geometry, ST_SetSRID(ST_POINT($1, $2), 4326))) as newpoint FROM roads
                            WHERE (SELECT bool_or( ST_Contains(geometry, ST_SetSRID(ST_POINT($1, $2), 4326))) FROM admin)
                                AND "type" IN ('motorway', 'trunk', 'primary', 'secondary', 'tertiary', 'unclassified', 'residential',
                                            'service', 'motorway_link', 'trunk_link', 'primary_link', 'secondary_link', 'motorway_junction',
                                            'living street', 'track', 'bus guideway', 'raceway', 'road','construction', 'escape')
                            ORDER BY geometry <-> ST_SetSRID(ST_POINT($1, $2), 4326) LIMIT 1;`;

                auto cmd = new PGCommand(pgconnection);
                cmd.query = sql;
                auto lon = cmd.parameters.add(1, PGType.FLOAT8); //FLOAT8 - double
                auto lat = cmd.parameters.add(2, PGType.FLOAT8);
                cmd.prepare();
                foreach(i, cargpspoint; cargpspoints)
                {
                    try
                    {
                        lon.value = cargpspoint.lon; // FLOAT8 = double
                        lat.value = cargpspoint.lat;

                        auto result = cmd.executeQuery();
                        auto answer = result.array;
                        result.close();
                        
                        if(answer.empty)
                            continue;

                        // было так: nearestroaddistance.id = answer.front[0].coerce!ulong;	
                        nearestroaddistance.id = cargpspoint.id;
                        nearestroaddistance.recordDate = cargpspoint.recordDate;
                        nearestroaddistance.velocity = cargpspoint.velocity;

                        //HACK!
                        try
                        {	
                           nearestroaddistance.maxspeed = answer.front[2].coerce!int;
                        }
                        catch(Exception e)
                        {
                           nearestroaddistance.maxspeed = 0;
                        }
                        nearestroaddistance.roadtype = answer.front[1].coerce!string;	
                        nearestroaddistance.lon = to!double(((answer.front[3]).coerce!string).replace(`POINT(`,``).replace(`)`,``).split(" ")[0]);
                        nearestroaddistance.lat = to!double(((answer.front[3]).coerce!string).replace(`POINT(`,``).replace(`)`,``).split(" ")[1]);
                        // ----- проверяем попадает ли точка в город или нет. Пока оставил как есть.
                        auto cmd2 = new PGCommand(pgconnection);
                        string inCitySQLCheck = `SELECT ST_Contains(ST_SetSRID(ST_POINT($1, $2), 4326), st_union) from a_common_city;`;
                        cmd2.parameters.add(1, PGType.FLOAT8).value = cargpspoint.lon; // FLOAT8 = double
                        cmd2.parameters.add(2, PGType.FLOAT8).value = cargpspoint.lat;
                        cmd2.query = inCitySQLCheck;
                        auto result2 = cmd2.executeQuery();
                        auto answer2 = result2.array;
                        if(answer2.front[0].coerce!bool == false)
                            nearestroaddistance.inCity = 0;
                        else
                            nearestroaddistance.inCity = 1;
                        // ----- проверяем попадает ли точка в город или нет
                        nearestroaddistances ~= nearestroaddistance;
                        result2.close();
                        b.next();				
                    }

                    catch(Exception e) // executeRow throw exeption if DB answer is empty
                    {
                        writeln(e.msg);
                        continue;
                    }

                }
                b.finish();

                auto endTime = MonoTime.currTime;
                auto duration = endTime - startTime;
                writefln("[INFO] Nearest Road Distance Processing Time: %s", duration.total!"minutes");
                fLogger.logf("Track ID: %s. Points in Track: %s. Processing time: %s minutes", GPSAndSensor.gps, cargpspoints.count, duration.total!"minutes");
                
                return nearestroaddistances;

            }

            catch(Exception e)
            {
                fLogger.criticalf("Fail to calculate nearest road distance. Track ID: %s. Points in Track: ", GPSAndSensor.gps, cargpspoints.count);
                fLogger.critical(e.msg);
                return null;
            }
        

	}


}

