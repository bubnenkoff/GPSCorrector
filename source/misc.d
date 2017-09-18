module misc;
import std.file;
import std.stdio;
import globals;

void removeOldLog()
{
	if(logName.exists)
    {
        try 
        {
            //remove(logName); // instead removing we can overwrite file. Removing is blocked by global Log
        }
        catch(Exception e)
        {
            writeln("Can't remove old ", logName);
            writeln(e.msg);
        }
    }
}