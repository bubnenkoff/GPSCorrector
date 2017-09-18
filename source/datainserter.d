module datainserter;

import std.stdio;
import std.array;
import std.file;
import std.algorithm;
import vibe.data.json;
import mysql;
import globals;
import database;
import config;

string mystorage = "fs";

// GPSAndSensorTuple GPSAndSensor через конструктор
// чтобы класс получал mysqlLocal

class DataWriterFactory 
{
    IDataWriter getWriter(string fullName) // FullName = path with fileName
    {
        return new DBDataWriter(); 
    }


}

interface IDataWriter 
{
    void writeData(NearestRoadDistance [] nearestroaddistances, GPSAndSensorTuple GPSAndSensor);
}

class DBDataWriter : IDataWriter
{
    Database database; // прям в конструтторе объявить

    this(Database database, Config config)
    {
        this.database = database; // создаю тут
    }

    void writeData(NearestRoadDistance [] nearestroaddistances, GPSAndSensorTuple GPSAndSensor)
    {
        Prepared prepared = prepare(database, `INSERT IGNORE INTO ` ~ GPSAndSensor.gps ~ ` (id, roadtype, inCity, roadmaxspeed, nearest_lat, nearest_lon, RecordDate, velocity) VALUES (?,?,?,?,?,?,?,?)`);

        foreach (nearestroaddistance; nearestroaddistances)
        {
            try
            {
                prepared.setArgs(nearestroaddistance.id, nearestroaddistance.roadtype, nearestroaddistance.inCity, nearestroaddistance.maxspeed, nearestroaddistance.lat, nearestroaddistance.lon, nearestroaddistance.recordDate, nearestroaddistance.velocity);
                prepared.exec();
            }
            catch(Exception e)
            {
                writeln("\n[ERROR] during INSERT in *LOCAL* MySQL");
                writeln(e.msg);
            }
        }
    }
}

class FileDataWriter : IDataWriter
{
    override void writeData(NearestRoadDistance [] nearestroaddistances, GPSAndSensorTuple GPSAndSensor)
    {
        File file = File(GPSAndSensor.gps ~ `.txt`, "w");
        file.write(nearestroaddistances.serializeToJson());
    }
}

//GPSAndSensor.gps нужно передавать в конструкторе чтобы можно было указать куда именно мы пишем