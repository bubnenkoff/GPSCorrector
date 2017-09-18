module globals;
import std.typecons;
import std.experimental.logger;


string logName = "ProcessingLog.txt";

struct carGPSPoint
{
    ulong id;
    string recordDate;
    double velocity;
    double lat;
    double lon;
}

struct NearestRoadDistance
{
    ulong id;
    string roadtype;				
    int maxspeed;
    double lon;
    double lat;
    int inCity;
    string recordDate;
    double velocity;
}			

FileLogger fLogger;

alias GPSAndSensorTuple = Tuple!(string, "gps", string, "sensor");

ulong TotalProcessdTracks; // global count for loggining



