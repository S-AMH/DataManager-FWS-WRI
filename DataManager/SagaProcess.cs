using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataManager
{
    class SagaProcess
    {

        static string extractFileNamesGFS0p13(string _date, string _run, string variable,
            string timeInterval, string sperator = ";")
        {
            string year;
            string month;
            string day;
            string result = "";

            year = _date.Substring(0, 4);
            month = _date.Substring(4, 2);
            day = _date.Substring(6, 2);

            FileInfo[] files = new DirectoryInfo(Path.Combine(resource.GFS0p13RawDataDB, year, month, day, _run, timeInterval, variable)).GetFiles().Select(fn => new FileInfo(fn.FullName)).OrderBy(f => f.Name).ToArray();
            Array.Sort(files, ATG.atgMethods.CompareNatural);

            foreach (var file in files)
                result = result + file.Name + sperator;

            return result;
        }
        static string extractFileNamesICON(string _date, string _run, string variable,
            string timeInterval, string sperator = ";")
        {
            string year;
            string month;
            string day;
            string result = "";

            year = _date.Substring(0, 4);
            month = _date.Substring(4, 2);
            day = _date.Substring(6, 2);

            FileInfo[] files = new DirectoryInfo(Path.Combine(resource.ICON_RawDB, year, month, day, _run, timeInterval, variable)).GetFiles().Select(fn => new FileInfo(fn.FullName)).OrderBy(f => f.Name).ToArray();
            Array.Sort(files, ATG.atgMethods.CompareNatural);

            foreach (var file in files)
                result = result + file.Name + sperator;

            return result;
        }
        static string extractFileNamesWRF0p11(string _date, string _run, string modelName, string variable,
            string timeInterval, string sperator = ";")
        {
            string year;
            string month;
            string day;
            string result = "";

            year = _date.Substring(0, 4);
            month = _date.Substring(4, 2);
            day = _date.Substring(6, 2);

            FileInfo[] files = new DirectoryInfo(Path.Combine(resource.wrfRawDB, modelName, year, month, day, _run, timeInterval, variable)).GetFiles().Select(fn => new FileInfo(fn.FullName)).OrderBy(f => f.Name).ToArray();
            Array.Sort(files, ATG.atgMethods.CompareNatural);

            foreach (var file in files)
                result = result + file.Name + sperator;

            return result;
        }
        public static bool runSagaCommandZonalForPointsGFS0p13
            (string gridNames, string _date, string _run, string timeInterval, string variable, string shapeFile)
        {
            Console.WriteLine("DEBUG: Zonal for points GFS0p13 for " + shapeFile + " on variable " + variable + " " + timeInterval);
            string target = "";
            string csvName = "";
            string year = _date.Substring(0, 4);
            string month = _date.Substring(4, 2);
            string day = _date.Substring(6, 2);
            string workingDir = "";
            string sagaCmd = "";
            bool exited = false;
            Process cmd = new Process();
            cmd.StartInfo.UseShellExecute = false;
            cmd.StartInfo.CreateNoWindow = true;
            cmd.StartInfo.WindowStyle = ProcessWindowStyle.Hidden;

            target = Path.Combine(resource.GFS0p13ProcessedDB, year, month, day, _run, timeInterval, variable);
            Directory.CreateDirectory(target);
            csvName = Path.Combine(target, Path.GetFileNameWithoutExtension(shapeFile) + "_" + year + month + day + _run + ".csv");
            target = Path.Combine(target, Path.GetFileNameWithoutExtension(shapeFile) + "_" + year + month + day + _run + ".shp");
            workingDir = Path.Combine(resource.GFS0p13RawDataDB, year, month, day, _run, timeInterval, variable);

            sagaCmd = resource.SagaCmdDir + " -f=s shapes_grid 0 -SHAPES=";
            sagaCmd = sagaCmd + shapeFile;
            sagaCmd = sagaCmd + " -GRIDS=" + gridNames.Substring(0, gridNames.Length - 1);
            sagaCmd = sagaCmd + " -RESULT=" + target + " -RESAMPLING=1";

            Directory.SetCurrentDirectory(workingDir);

            cmd.StartInfo.FileName = @"cmd.exe";
            cmd.StartInfo.WindowStyle = ProcessWindowStyle.Hidden;
            cmd.StartInfo.CreateNoWindow = true;
            cmd.StartInfo.Arguments = @"/C " + sagaCmd;

            do
            {
                cmd.Start();
                exited = cmd.WaitForExit(600000);
                if (exited != true)
                    cmd.Kill();
            } while (exited != true);

            exited = false;


            sagaCmd = sagaCmd = resource.SagaCmdDir + " -f=s io_table 0 -TABLE=" + target + " -SEPARATOR=2 -FILENAME=" + csvName;
            cmd.StartInfo.Arguments = @"/C " + sagaCmd;
            do
            {
                cmd.Start();
                exited = cmd.WaitForExit(600000);
                if (exited != true)
                    cmd.Kill();
            } while (exited != true);

            return true;
        }
        public static bool runSagaCommandZonalForPointsICON
            (string gridNames, string _date, string _run, string timeInterval, string variable, string shapeFile)
        {
            Console.WriteLine("DEBUG: Zonal for points ICON for " + shapeFile + " on variable " + variable + " " + timeInterval);
            string target = "";
            string csvName = "";
            string year = _date.Substring(0, 4);
            string month = _date.Substring(4, 2);
            string day = _date.Substring(6, 2);
            string workingDir = "";
            string sagaCmd = "";
            bool exited = false;
            Process cmd = new Process();
            cmd.StartInfo.UseShellExecute = false;
            cmd.StartInfo.CreateNoWindow = true;
            cmd.StartInfo.WindowStyle = ProcessWindowStyle.Hidden;
            //cmd.StartInfo.UseShellExecute = true;
            //cmd.StartInfo.CreateNoWindow = false;
            //cmd.StartInfo.WindowStyle = ProcessWindowStyle.Normal;

            target = Path.Combine(resource.ICON_ProccesedDB, year, month, day, _run, timeInterval, variable);

            Directory.CreateDirectory(target);
            csvName = Path.Combine(target, Path.GetFileNameWithoutExtension(shapeFile) + "_" + year + month + day + _run + ".csv");
            target = Path.Combine(target, Path.GetFileNameWithoutExtension(shapeFile) + "_" + year + month + day + _run + ".shp");
            workingDir = Path.Combine(resource.ICON_RawDB, year, month, day, _run, timeInterval, variable);
            Console.WriteLine(csvName);

            sagaCmd = resource.SagaCmdDir + " -f=s shapes_grid 0 -SHAPES=";
            sagaCmd = sagaCmd + shapeFile;
            sagaCmd = sagaCmd + " -GRIDS=" + gridNames.Substring(0, gridNames.Length - 1);
            sagaCmd = sagaCmd + " -RESULT=" + target + " -RESAMPLING=1";

            Directory.SetCurrentDirectory(workingDir);

            cmd.StartInfo.FileName = @"cmd.exe";
            cmd.StartInfo.WindowStyle = ProcessWindowStyle.Hidden;
            cmd.StartInfo.CreateNoWindow = true;
            cmd.StartInfo.Arguments = @"/C " + sagaCmd;

            do
            {
                cmd.Start();
                exited = cmd.WaitForExit(600000);
                if (exited != true)
                    cmd.Kill();
            } while (exited != true);

            exited = false;


            sagaCmd = sagaCmd = resource.SagaCmdDir + " -f=s io_table 0 -TABLE=" + target + " -SEPARATOR=2 -FILENAME=" + csvName;
            cmd.StartInfo.Arguments = @"/C " + sagaCmd;
            do
            {
                cmd.Start();
                exited = cmd.WaitForExit(600000);
                if (exited != true)
                    cmd.Kill();
            } while (exited != true);

            return true;
        }
        public static bool runSagaCommandZonalForPointsWRF0p11
            (string gridNames, string _date, string _run,string modelName, string timeInterval, string variable, string shapeFile)
        {
            Console.WriteLine("DEBUG: Zonal for points WRF0p11 for " + shapeFile + " on variable " + variable + " " + timeInterval);
            string target = "";
            string csvName = "";
            string year = _date.Substring(0, 4);
            string month = _date.Substring(4, 2);
            string day = _date.Substring(6, 2);
            string workingDir = "";
            string sagaCmd = "";
            bool exited = false;
            Process cmd = new Process();
            cmd.StartInfo.UseShellExecute = false;
            cmd.StartInfo.CreateNoWindow = true;
            cmd.StartInfo.WindowStyle = ProcessWindowStyle.Hidden;

            target = Path.Combine(resource.wrfProccessedDB, modelName, year, month, day, _run, timeInterval, variable);
            Directory.CreateDirectory(target);
            csvName = Path.Combine(target, Path.GetFileNameWithoutExtension(shapeFile) + "_" + year + month + day + _run + ".csv");
            target = Path.Combine(target, Path.GetFileNameWithoutExtension(shapeFile) + "_" + year + month + day + _run + ".shp");
            workingDir = Path.Combine(resource.wrfRawDB, modelName, year, month, day, _run, timeInterval, variable);

            sagaCmd = resource.SagaCmdDir + " -f=s shapes_grid 0 -SHAPES=";
            sagaCmd = sagaCmd + shapeFile;
            sagaCmd = sagaCmd + " -GRIDS=" + gridNames.Substring(0, gridNames.Length - 1);
            sagaCmd = sagaCmd + " -RESULT=" + target + " -RESAMPLING=1";

            Directory.SetCurrentDirectory(workingDir);

            cmd.StartInfo.FileName = @"cmd.exe";
            cmd.StartInfo.Arguments = @"/C " + sagaCmd;
            do
            {
                cmd.Start();
                exited = cmd.WaitForExit(600000);
                if (exited != true)
                    cmd.Kill();
            } while (exited != true);

            exited = false;

            sagaCmd = sagaCmd = resource.SagaCmdDir + " -f=s io_table 0 -TABLE=" + target + " -SEPARATOR=2 -FILENAME=" + csvName;
            cmd.StartInfo.Arguments = @"/C " + sagaCmd;
            do
            {
                cmd.Start();
                exited = cmd.WaitForExit(600000);
                if (exited != true)
                    cmd.Kill();
            } while (exited != true);
        if (!File.Exists(csvName))
            throw new FileNotFoundException(csvName + " has not been created.");
        

            return true;
        }
        public static bool runSagaCommandZonalForPointsRainSum_12_24GFS0p13
            (string gridNames, string _date, string _run,
            string timeInterval, string variable, string shapefile, string time)
        {
            string target = "";
            string csvName = "";
            string year = _date.Substring(0, 4);
            string month = _date.Substring(4, 2);
            string day = _date.Substring(6, 2);
            //int cnt = 5;
            Console.WriteLine("DEBUG: Zonal for points RAINSUM GFS for " + shapefile + " on variable " + variable + " " + timeInterval);
                string workingDir = "";
                string sagaCmd = "";
                bool exited = false;
                Process cmd = new Process();
                cmd.StartInfo.UseShellExecute = false;
                cmd.StartInfo.CreateNoWindow = true;
                cmd.StartInfo.WindowStyle = ProcessWindowStyle.Hidden;

                target = Path.Combine(resource.GFS0p13ProcessedDB, year, month, day, _run, timeInterval, variable);
                Directory.CreateDirectory(target);
                csvName = Path.Combine(target, Path.GetFileNameWithoutExtension(shapefile) + "_" + time + "_" + year + month + day + _run + ".csv");
                target = Path.Combine(target, Path.GetFileNameWithoutExtension(shapefile) + "_" + time + "_" + year + month + day + _run + ".shp");
                workingDir = Path.Combine(resource.GFS0p13RawDataDB, year, month, day, _run, timeInterval, variable);

                Directory.SetCurrentDirectory(workingDir);

                sagaCmd = resource.SagaCmdDir + " -f=s shapes_grid 0 -SHAPES=";
                sagaCmd = sagaCmd + shapefile;
                sagaCmd = sagaCmd + " -GRIDS=" + gridNames.Substring(0, gridNames.Length - 1);
                sagaCmd = sagaCmd + " -RESULT=" + target + " -RESAMPLING=1";

                cmd.StartInfo.FileName = @"cmd.exe";
                cmd.StartInfo.Arguments = @"/C " + sagaCmd;
                do
                {
                Console.WriteLine(cmd.StartInfo.Arguments);
                    cmd.Start();
                    exited = cmd.WaitForExit(600000);
                    if (exited != true)
                        cmd.Kill();
                } while (exited != true);

                exited = false;

                sagaCmd = resource.SagaCmdDir;
                sagaCmd = sagaCmd + " -f=s table_calculus 17 -MEAN=1 -MAX=1 -TABLE=" + target;
                if (time == "12hr")
                    sagaCmd = sagaCmd + " -FIELDS=15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34 -RESULT=" + target;
                else if (time == "24hr")
                    sagaCmd = sagaCmd + " -FIELDS=15,16,17,18,19,20,21,22,23,24 -RESULT=" + target;
                else if (time == "ALL")
                    sagaCmd = sagaCmd + " -FIELDS=15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94 -RESULT=" + target;
                else
                    return false;
                cmd.StartInfo.FileName = @"cmd.exe";
                cmd.StartInfo.Arguments = @"/C " + sagaCmd;
                do
                {
                    cmd.Start();
                    exited = cmd.WaitForExit(600000);
                    if (exited != true)
                        cmd.Kill();
                } while (exited != true);
                exited = false;

                sagaCmd = sagaCmd = resource.SagaCmdDir + " -f=s io_table 0 -TABLE=" + target + " -SEPARATOR=2 -FILENAME=" + csvName;
                cmd.StartInfo.Arguments = @"/C " + sagaCmd;
                do
                {
                    cmd.Start();
                    exited = cmd.WaitForExit(600000);
                    if (exited != true)
                        cmd.Kill();
                } while (exited != true);
            if (!File.Exists(csvName))
                throw new FileNotFoundException(csvName + " has not been created.");
            
            return true;
        }

        public static bool runSagaCommandZonalForPointsRainSum_12_24WRF0p11
            (string gridNames, string _date, string _run, string modelName,
            string timeInterval, string variable, string shapefile, string time)
        {
            string target = "";
            string csvName = "";
            string year = _date.Substring(0, 4);
            string month = _date.Substring(4, 2);
            string day = _date.Substring(6, 2);
            Console.WriteLine("DEBUG: Zonal for points RAINSUM WRF for " + shapefile + " on variable " + variable + " " + timeInterval);
            string workingDir = "";
                string sagaCmd = "";

                bool exited = false;
                Process cmd = new Process();
                cmd.StartInfo.UseShellExecute = false;
                cmd.StartInfo.CreateNoWindow = true;
                cmd.StartInfo.WindowStyle = ProcessWindowStyle.Hidden;

                target = Path.Combine(resource.wrfProccessedDB, modelName, year, month, day, _run, timeInterval, variable);
                Directory.CreateDirectory(target);
                csvName = Path.Combine(target, Path.GetFileNameWithoutExtension(shapefile) + "_" + time + "_" + year + month + day + _run + ".csv");
                target = Path.Combine(target, Path.GetFileNameWithoutExtension(shapefile) + "_" + time + "_" + year + month + day + _run + ".shp");
                workingDir = Path.Combine(resource.wrfRawDB, modelName, year, month, day, _run, timeInterval, variable);

                Directory.SetCurrentDirectory(workingDir);

                sagaCmd = resource.SagaCmdDir + " -f=s shapes_grid 0 -SHAPES=";
                sagaCmd = sagaCmd + shapefile;
                sagaCmd = sagaCmd + " -GRIDS=" + gridNames.Substring(0, gridNames.Length - 1);
                sagaCmd = sagaCmd + " -RESULT=" + target + " -RESAMPLING=1";

                cmd.StartInfo.FileName = @"cmd.exe";
                cmd.StartInfo.Arguments = @"/C " + sagaCmd;
                do
                {
                    cmd.Start();
                    exited = cmd.WaitForExit(600000);
                    if (exited != true)
                        cmd.Kill();
                } while (exited != true);

                exited = false;

                sagaCmd = resource.SagaCmdDir;
                sagaCmd = sagaCmd + " -f=s table_calculus 17 -MEAN=1 -MAX=1 -TABLE=" + target;
                if (time == "12hr")
                    sagaCmd = sagaCmd + " -FIELDS=15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34 -RESULT=" + target;
                else if (time == "24hr")
                    sagaCmd = sagaCmd + " -FIELDS=15,16,17,18,19,20,21,22,23,24 -RESULT=" + target;
                else if (time == "ALL")
                    sagaCmd = sagaCmd + " -FIELDS=15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94 -RESULT=" + target;
                else
                    return false;
                cmd.StartInfo.FileName = @"cmd.exe";
                cmd.StartInfo.Arguments = @"/C " + sagaCmd;
                do
                {
                    cmd.Start();
                    exited = cmd.WaitForExit(600000);
                    if (exited != true)
                        cmd.Kill();
                } while (exited != true);


                exited = false;
                sagaCmd = sagaCmd = resource.SagaCmdDir + " -f=s io_table 0 -TABLE=" + target + " -SEPARATOR=2 -FILENAME=" + csvName;
                cmd.StartInfo.Arguments = @"/C " + sagaCmd;
                do
                {
                    cmd.Start();
                    exited = cmd.WaitForExit(600000);
                    if (exited != true)
                        cmd.Kill();
                } while (exited != true);
            if (!File.Exists(csvName))
                throw new FileNotFoundException(csvName + " has not been created.");
            

            return true;
        }

        public static bool zonalForPoints(string _date, string _run, string var="RAIN")
        { 

            //string temp1hr = extractFileNamesGFS0p13(_date, _run, "TEMP", "1hr");
            //string rain1hr = extractFileNamesGFS0p13(_date, _run, "RAIN", "1hr");
            //string temp3hr = extractFileNamesGFS0p13(_date, _run, "TEMP", "3hr");
            string rain3hr = extractFileNamesGFS0p13(_date, _run, "RAIN", "3hr");
            //string rainSum12hr = extractFileNamesGFS0p13(_date, _run, "RAIN-12hr", "3hr");
            //string rainSum24hr = extractFileNamesGFS0p13(_date, _run, "RAIN-24hr", "3hr");

            if (var == "APCP")
            {
                //string apcp1hr = extractFileNamesGFS0p13(_date, _run, "APCP", "1hr");
                //string apcpSum12hr = extractFileNamesGFS0p13(_date, _run, "APCP-12hr", "3hr");
                //string apcpSum24hr = extractFileNamesGFS0p13(_date, _run, "APCP-24hr", "3hr");
                string apcp3hr = extractFileNamesGFS0p13(_date, _run, "APCP", "3hr");
                //runSagaCommandZonalForPointsGFS0p13(apcp1hr, _date, _run, "1hr", "APCP", resource._1_SHAPE_FILE_);
                var t11 = Task.Factory.StartNew( () =>  runSagaCommandZonalForPointsGFS0p13(apcp3hr, _date, _run, "3hr", "APCP", resource._1_SHAPE_FILE_));
                //runSagaCommandZonalForPointsGFS0p13(apcp1hr, _date, _run, "1hr", "APCP", resource._3_SHAPE_FILE_);
                var t22= Task.Factory.StartNew( () => runSagaCommandZonalForPointsGFS0p13(apcp3hr, _date, _run, "3hr", "APCP", resource._3_SHAPE_FILE_));
                //runSagaCommandZonalForPointsGFS0p13(apcp1hr, _date, _run, "1hr", "APCP", resource._4_SHAPE_FILE_);
                //runSagaCommandZonalForPointsGFS0p13(apcp3hr, _date, _run, "3hr", "APCP", resource._4_SHAPE_FILE_);
                //runSagaCommandZonalForPointsRainSum_12_24GFS0p13(apcpSum12hr, _date, _run, "3hr", "APCP-12hr", resource._1_SHAPE_FILE_, "12hr");
                //runSagaCommandZonalForPointsRainSum_12_24GFS0p13(apcpSum24hr, _date, _run, "3hr", "APCP-24hr", resource._1_SHAPE_FILE_, "24hr");
                //runSagaCommandZonalForPointsRainSum_12_24GFS0p13(apcpSum24hr, _date, _run, "3hr", "APCP", resource._1_SHAPE_FILE_, "ALL");
                //runSagaCommandZonalForPointsRainSum_12_24GFS0p13(apcpSum24hr, _date, _run, "1hr", "APCP", resource._1_SHAPE_FILE_, "ALL");
                //runSagaCommandZonalForPointsRainSum_12_24GFS0p13(apcpSum12hr, _date, _run, "3hr", "APCP-12hr", resource._3_SHAPE_FILE_, "12hr");
                //runSagaCommandZonalForPointsRainSum_12_24GFS0p13(apcpSum24hr, _date, _run, "3hr", "APCP-24hr", resource._3_SHAPE_FILE_, "24hr");
                //runSagaCommandZonalForPointsRainSum_12_24GFS0p13(apcpSum24hr, _date, _run, "3hr", "APCP", resource._3_SHAPE_FILE_, "ALL");
                //runSagaCommandZonalForPointsRainSum_12_24GFS0p13(apcpSum24hr, _date, _run, "1hr", "APCP", resource._3_SHAPE_FILE_, "ALL");
                //runSagaCommandZonalForPointsRainSum_12_24GFS0p13(apcpSum12hr, _date, _run, "3hr", "APCP-12hr", resource._4_SHAPE_FILE_, "12hr");
                //runSagaCommandZonalForPointsRainSum_12_24GFS0p13(apcpSum24hr, _date, _run, "3hr", "APCP-24hr", resource._4_SHAPE_FILE_, "24hr");
                //runSagaCommandZonalForPointsRainSum_12_24GFS0p13(apcpSum24hr, _date, _run, "3hr", "APCP", resource._4_SHAPE_FILE_, "ALL");
                //runSagaCommandZonalForPointsRainSum_12_24GFS0p13(apcpSum24hr, _date, _run, "1hr", "APCP", resource._4_SHAPE_FILE_, "ALL");
                Task.WaitAll(new Task[] { t11, t22 });
                return true;
            }

            //runSagaCommandZonalForPointsGFS0p13(rain1hr, _date, _run, "1hr", "RAIN", resource._1_SHAPE_FILE_);
            //runSagaCommandZonalForPointsGFS0p13(temp1hr, _date, _run, "1hr", "TEMP", resource._1_SHAPE_FILE_);
            var t1 = Task.Factory.StartNew( () => runSagaCommandZonalForPointsGFS0p13(rain3hr, _date, _run, "3hr", "RAIN", resource._1_SHAPE_FILE_));
            //runSagaCommandZonalForPointsGFS0p13(temp3hr, _date, _run, "3hr", "TEMP", resource._1_SHAPE_FILE_);
            //runSagaCommandZonalForPointsRainSum_12_24GFS0p13(rainSum12hr, _date, _run, "3hr", "RAIN-12hr", resource._1_SHAPE_FILE_, "12hr");
            //runSagaCommandZonalForPointsRainSum_12_24GFS0p13(rainSum24hr, _date, _run, "3hr", "RAIN-24hr", resource._1_SHAPE_FILE_, "24hr");
            //runSagaCommandZonalForPointsRainSum_12_24GFS0p13(rainSum24hr, _date, _run, "3hr", "RAIN", resource._1_SHAPE_FILE_, "ALL");
            //runSagaCommandZonalForPointsRainSum_12_24GFS0p13(rainSum24hr, _date, _run, "1hr", "RAIN", resource._1_SHAPE_FILE_, "ALL");
            //runSagaCommandZonalForPointsGFS0p13(rain1hr, _date, _run, "1hr", "RAIN", resource._3_SHAPE_FILE_);
            //runSagaCommandZonalForPointsGFS0p13(temp1hr, _date, _run, "1hr", "TEMP", resource._3_SHAPE_FILE_);
            var t2 = Task.Factory.StartNew( () => runSagaCommandZonalForPointsGFS0p13(rain3hr, _date, _run, "3hr", "RAIN", resource._3_SHAPE_FILE_));
            //runSagaCommandZonalForPointsGFS0p13(temp3hr, _date, _run, "3hr", "TEMP", resource._3_SHAPE_FILE_);
            //runSagaCommandZonalForPointsRainSum_12_24GFS0p13(rainSum12hr, _date, _run, "3hr", "RAIN-12hr", resource._3_SHAPE_FILE_, "12hr");
            //runSagaCommandZonalForPointsRainSum_12_24GFS0p13(rainSum24hr, _date, _run, "3hr", "RAIN-24hr", resource._3_SHAPE_FILE_, "24hr");
            //runSagaCommandZonalForPointsRainSum_12_24GFS0p13(rainSum24hr, _date, _run, "3hr", "RAIN", resource._3_SHAPE_FILE_, "ALL");
            //runSagaCommandZonalForPointsRainSum_12_24GFS0p13(rainSum24hr, _date, _run, "1hr", "RAIN", resource._3_SHAPE_FILE_, "ALL");
            //runSagaCommandZonalForPointsGFS0p13(rain1hr, _date, _run, "1hr", "RAIN", resource._4_SHAPE_FILE_);
            //runSagaCommandZonalForPointsGFS0p13(temp1hr, _date, _run, "1hr", "TEMP", resource._4_SHAPE_FILE_);
            //runSagaCommandZonalForPointsGFS0p13(rain3hr, _date, _run, "3hr", "RAIN", resource._4_SHAPE_FILE_);
            ////runSagaCommandZonalForPointsGFS0p13(temp3hr, _date, _run, "3hr", "TEMP", resource._4_SHAPE_FILE_);
            //runSagaCommandZonalForPointsRainSum_12_24GFS0p13(rainSum12hr, _date, _run, "3hr", "RAIN-12hr", resource._4_SHAPE_FILE_, "12hr");
            //runSagaCommandZonalForPointsRainSum_12_24GFS0p13(rainSum24hr, _date, _run, "3hr", "RAIN-24hr", resource._4_SHAPE_FILE_, "24hr");
            //runSagaCommandZonalForPointsRainSum_12_24GFS0p13(rainSum24hr, _date, _run, "3hr", "RAIN", resource._4_SHAPE_FILE_, "ALL");
            //runSagaCommandZonalForPointsRainSum_12_24GFS0p13(rainSum24hr, _date, _run, "1hr", "RAIN", resource._4_SHAPE_FILE_, "ALL");
            Task.WaitAll(new Task[] { t1, t2 });
            return true;
        }

        public static bool zonalForPointsICON(string _date, string _run, string var = "RAIN")
        {

            //string temp1hr = extractFileNamesGFS0p13(_date, _run, "TEMP", "1hr");
            //string rain1hr = extractFileNamesGFS0p13(_date, _run, "RAIN", "1hr");
            //string temp3hr = extractFileNamesGFS0p13(_date, _run, "TEMP", "3hr");
            string rain3hr = extractFileNamesICON(_date, _run, "RAIN", "3hr");
            Console.WriteLine(rain3hr);
            //string rainSum12hr = extractFileNamesGFS0p13(_date, _run, "RAIN-12hr", "3hr");
            //string rainSum24hr = extractFileNamesGFS0p13(_date, _run, "RAIN-24hr", "3hr");

            if (var == "APCP")
            {
                //string apcp1hr = extractFileNamesGFS0p13(_date, _run, "APCP", "1hr");
                //string apcpSum12hr = extractFileNamesGFS0p13(_date, _run, "APCP-12hr", "3hr");
                //string apcpSum24hr = extractFileNamesGFS0p13(_date, _run, "APCP-24hr", "3hr");
                string apcp3hr = extractFileNamesICON(_date, _run, "APCP", "3hr");
                Console.WriteLine(apcp3hr);
                //runSagaCommandZonalForPointsGFS0p13(apcp1hr, _date, _run, "1hr", "APCP", resource._1_SHAPE_FILE_);
                runSagaCommandZonalForPointsICON(apcp3hr, _date, _run, "3hr", "APCP", resource._1_SHAPE_FILE_);
                //runSagaCommandZonalForPointsGFS0p13(apcp1hr, _date, _run, "1hr", "APCP", resource._3_SHAPE_FILE_);
                runSagaCommandZonalForPointsICON(apcp3hr, _date, _run, "3hr", "APCP", resource._3_SHAPE_FILE_);
                //runSagaCommandZonalForPointsGFS0p13(apcp1hr, _date, _run, "1hr", "APCP", resource._4_SHAPE_FILE_);
                //runSagaCommandZonalForPointsGFS0p13(apcp3hr, _date, _run, "3hr", "APCP", resource._4_SHAPE_FILE_);
                //runSagaCommandZonalForPointsRainSum_12_24GFS0p13(apcpSum12hr, _date, _run, "3hr", "APCP-12hr", resource._1_SHAPE_FILE_, "12hr");
                //runSagaCommandZonalForPointsRainSum_12_24GFS0p13(apcpSum24hr, _date, _run, "3hr", "APCP-24hr", resource._1_SHAPE_FILE_, "24hr");
                //runSagaCommandZonalForPointsRainSum_12_24GFS0p13(apcpSum24hr, _date, _run, "3hr", "APCP", resource._1_SHAPE_FILE_, "ALL");
                //runSagaCommandZonalForPointsRainSum_12_24GFS0p13(apcpSum24hr, _date, _run, "1hr", "APCP", resource._1_SHAPE_FILE_, "ALL");
                //runSagaCommandZonalForPointsRainSum_12_24GFS0p13(apcpSum12hr, _date, _run, "3hr", "APCP-12hr", resource._3_SHAPE_FILE_, "12hr");
                //runSagaCommandZonalForPointsRainSum_12_24GFS0p13(apcpSum24hr, _date, _run, "3hr", "APCP-24hr", resource._3_SHAPE_FILE_, "24hr");
                //runSagaCommandZonalForPointsRainSum_12_24GFS0p13(apcpSum24hr, _date, _run, "3hr", "APCP", resource._3_SHAPE_FILE_, "ALL");
                //runSagaCommandZonalForPointsRainSum_12_24GFS0p13(apcpSum24hr, _date, _run, "1hr", "APCP", resource._3_SHAPE_FILE_, "ALL");
                //runSagaCommandZonalForPointsRainSum_12_24GFS0p13(apcpSum12hr, _date, _run, "3hr", "APCP-12hr", resource._4_SHAPE_FILE_, "12hr");
                //runSagaCommandZonalForPointsRainSum_12_24GFS0p13(apcpSum24hr, _date, _run, "3hr", "APCP-24hr", resource._4_SHAPE_FILE_, "24hr");
                //runSagaCommandZonalForPointsRainSum_12_24GFS0p13(apcpSum24hr, _date, _run, "3hr", "APCP", resource._4_SHAPE_FILE_, "ALL");
                //runSagaCommandZonalForPointsRainSum_12_24GFS0p13(apcpSum24hr, _date, _run, "1hr", "APCP", resource._4_SHAPE_FILE_, "ALL");
                return true;
            }

            //runSagaCommandZonalForPointsGFS0p13(rain1hr, _date, _run, "1hr", "RAIN", resource._1_SHAPE_FILE_);
            //runSagaCommandZonalForPointsGFS0p13(temp1hr, _date, _run, "1hr", "TEMP", resource._1_SHAPE_FILE_);
            runSagaCommandZonalForPointsICON(rain3hr, _date, _run, "3hr", "RAIN", resource._1_SHAPE_FILE_);
            //runSagaCommandZonalForPointsGFS0p13(temp3hr, _date, _run, "3hr", "TEMP", resource._1_SHAPE_FILE_);
            //runSagaCommandZonalForPointsRainSum_12_24GFS0p13(rainSum12hr, _date, _run, "3hr", "RAIN-12hr", resource._1_SHAPE_FILE_, "12hr");
            //runSagaCommandZonalForPointsRainSum_12_24GFS0p13(rainSum24hr, _date, _run, "3hr", "RAIN-24hr", resource._1_SHAPE_FILE_, "24hr");
            //runSagaCommandZonalForPointsRainSum_12_24GFS0p13(rainSum24hr, _date, _run, "3hr", "RAIN", resource._1_SHAPE_FILE_, "ALL");
            //runSagaCommandZonalForPointsRainSum_12_24GFS0p13(rainSum24hr, _date, _run, "1hr", "RAIN", resource._1_SHAPE_FILE_, "ALL");
            //runSagaCommandZonalForPointsGFS0p13(rain1hr, _date, _run, "1hr", "RAIN", resource._3_SHAPE_FILE_);
            //runSagaCommandZonalForPointsGFS0p13(temp1hr, _date, _run, "1hr", "TEMP", resource._3_SHAPE_FILE_);
            runSagaCommandZonalForPointsICON(rain3hr, _date, _run, "3hr", "RAIN", resource._3_SHAPE_FILE_);
            //runSagaCommandZonalForPointsGFS0p13(temp3hr, _date, _run, "3hr", "TEMP", resource._3_SHAPE_FILE_);
            //runSagaCommandZonalForPointsRainSum_12_24GFS0p13(rainSum12hr, _date, _run, "3hr", "RAIN-12hr", resource._3_SHAPE_FILE_, "12hr");
            //runSagaCommandZonalForPointsRainSum_12_24GFS0p13(rainSum24hr, _date, _run, "3hr", "RAIN-24hr", resource._3_SHAPE_FILE_, "24hr");
            //runSagaCommandZonalForPointsRainSum_12_24GFS0p13(rainSum24hr, _date, _run, "3hr", "RAIN", resource._3_SHAPE_FILE_, "ALL");
            //runSagaCommandZonalForPointsRainSum_12_24GFS0p13(rainSum24hr, _date, _run, "1hr", "RAIN", resource._3_SHAPE_FILE_, "ALL");
            //runSagaCommandZonalForPointsGFS0p13(rain1hr, _date, _run, "1hr", "RAIN", resource._4_SHAPE_FILE_);
            //runSagaCommandZonalForPointsGFS0p13(temp1hr, _date, _run, "1hr", "TEMP", resource._4_SHAPE_FILE_);
            //runSagaCommandZonalForPointsGFS0p13(rain3hr, _date, _run, "3hr", "RAIN", resource._4_SHAPE_FILE_);
            ////runSagaCommandZonalForPointsGFS0p13(temp3hr, _date, _run, "3hr", "TEMP", resource._4_SHAPE_FILE_);
            //runSagaCommandZonalForPointsRainSum_12_24GFS0p13(rainSum12hr, _date, _run, "3hr", "RAIN-12hr", resource._4_SHAPE_FILE_, "12hr");
            //runSagaCommandZonalForPointsRainSum_12_24GFS0p13(rainSum24hr, _date, _run, "3hr", "RAIN-24hr", resource._4_SHAPE_FILE_, "24hr");
            //runSagaCommandZonalForPointsRainSum_12_24GFS0p13(rainSum24hr, _date, _run, "3hr", "RAIN", resource._4_SHAPE_FILE_, "ALL");
            //runSagaCommandZonalForPointsRainSum_12_24GFS0p13(rainSum24hr, _date, _run, "1hr", "RAIN", resource._4_SHAPE_FILE_, "ALL");
            return true;
        }

        public static bool zonalForPointsWRF(string _date, string _run, string _variable = "RAIN")
        {

            string[] modelName = { "ensemble", "ferrier", "lin", "wsm6" };

            for(int i = 0; i < 4; i ++)
            {
                if (_variable == "APCP")
                {
                    string apcp3hr = extractFileNamesWRF0p11(_date, _run, modelName[i], "APCP", "3hr");
                    //string apcpSum12hr = extractFileNamesWRF0p11(_date, _run, modelName[i], "APCP-12hr", "3hr");
                    //string apcpSum24hr = extractFileNamesWRF0p11(_date, _run, modelName[i], "APCP-24hr", "3hr");

                    var t1 = Task.Factory.StartNew(()=>runSagaCommandZonalForPointsWRF0p11(apcp3hr, _date, _run, modelName[i], "3hr", "APCP", resource._1_SHAPE_FILE_));
                    var t2 = Task.Factory.StartNew(()=>runSagaCommandZonalForPointsWRF0p11(apcp3hr, _date, _run, modelName[i], "3hr", "APCP", resource._3_SHAPE_FILE_));
                    //runSagaCommandZonalForPointsWRF0p11(apcp3hr, _date, _run, modelName[i], "3hr", "APCP", resource._4_SHAPE_FILE_);
                    //runSagaCommandZonalForPointsRainSum_12_24WRF0p11(apcpSum12hr, _date, _run, modelName[i], "3hr", "APCP-12hr", resource._1_SHAPE_FILE_, "12hr");
                    //runSagaCommandZonalForPointsRainSum_12_24WRF0p11(apcpSum24hr, _date, _run, modelName[i], "3hr", "APCP-24hr", resource._1_SHAPE_FILE_, "24hr");
                    //runSagaCommandZonalForPointsRainSum_12_24WRF0p11(apcpSum24hr, _date, _run, modelName[i], "3hr", "APCP", resource._1_SHAPE_FILE_, "ALL");
                    //runSagaCommandZonalForPointsRainSum_12_24WRF0p11(apcpSum12hr, _date, _run, modelName[i], "3hr", "APCP-12hr", resource._3_SHAPE_FILE_, "12hr");
                    //runSagaCommandZonalForPointsRainSum_12_24WRF0p11(apcpSum24hr, _date, _run, modelName[i], "3hr", "APCP-24hr", resource._3_SHAPE_FILE_, "24hr");
                    //runSagaCommandZonalForPointsRainSum_12_24WRF0p11(apcpSum24hr, _date, _run, modelName[i], "3hr", "APCP", resource._3_SHAPE_FILE_, "ALL");
                    //runSagaCommandZonalForPointsRainSum_12_24WRF0p11(apcpSum12hr, _date, _run, modelName[i], "3hr", "APCP-12hr", resource._4_SHAPE_FILE_, "12hr");
                    //runSagaCommandZonalForPointsRainSum_12_24WRF0p11(apcpSum24hr, _date, _run, modelName[i], "3hr", "APCP-24hr", resource._4_SHAPE_FILE_, "24hr");
                    //runSagaCommandZonalForPointsRainSum_12_24WRF0p11(apcpSum24hr, _date, _run, modelName[i], "3hr", "APCP", resource._4_SHAPE_FILE_, "ALL");
                    Task.WaitAll(new Task[] { t1, t2 });
                }
                else
                {

                    //string temp3hr = extractFileNamesWRF0p11(_date, _run, modelName[i], "TEMP", "3hr");
                    string rain3hr = extractFileNamesWRF0p11(_date, _run, modelName[i], "RAIN", "3hr");
                    //string rainSum12hr = extractFileNamesWRF0p11(_date, _run, modelName[i], "RAIN-12hr", "3hr");
                    //string rainSum24hr = extractFileNamesWRF0p11(_date, _run, modelName[i], "RAIN-24hr", "3hr");

                    var t1 = Task.Factory.StartNew(()=>runSagaCommandZonalForPointsWRF0p11(rain3hr, _date, _run, modelName[i], "3hr", "RAIN", resource._1_SHAPE_FILE_));
                    //runSagaCommandZonalForPointsWRF0p11(temp3hr, _date, _run, modelName[i], "3hr", "TEMP", resource._1_SHAPE_FILE_);
                    //runSagaCommandZonalForPointsRainSum_12_24WRF0p11(rainSum12hr, _date, _run, modelName[i], "3hr", "RAIN-12hr", resource._1_SHAPE_FILE_, "12hr");
                    //runSagaCommandZonalForPointsRainSum_12_24WRF0p11(rainSum24hr, _date, _run, modelName[i], "3hr", "RAIN-24hr", resource._1_SHAPE_FILE_, "24hr");
                    //runSagaCommandZonalForPointsRainSum_12_24WRF0p11(rainSum24hr, _date, _run, modelName[i], "3hr", "RAIN", resource._1_SHAPE_FILE_, "ALL");
                    var t2 = Task.Factory.StartNew(()=>runSagaCommandZonalForPointsWRF0p11(rain3hr, _date, _run, modelName[i], "3hr", "RAIN", resource._3_SHAPE_FILE_));
                    //runSagaCommandZonalForPointsWRF0p11(temp3hr, _date, _run, modelName[i], "3hr", "TEMP", resource._3_SHAPE_FILE_);
                    //runSagaCommandZonalForPointsRainSum_12_24WRF0p11(rainSum12hr, _date, _run, modelName[i], "3hr", "RAIN-12hr", resource._3_SHAPE_FILE_, "12hr");
                    //runSagaCommandZonalForPointsRainSum_12_24WRF0p11(rainSum24hr, _date, _run, modelName[i], "3hr", "RAIN-24hr", resource._3_SHAPE_FILE_, "24hr");
                    //runSagaCommandZonalForPointsRainSum_12_24WRF0p11(rainSum24hr, _date, _run, modelName[i], "3hr", "RAIN", resource._3_SHAPE_FILE_, "ALL");
                    //runSagaCommandZonalForPointsWRF0p11(rain3hr, _date, _run, modelName[i], "3hr", "RAIN", resource._4_SHAPE_FILE_);
                    //runSagaCommandZonalForPointsWRF0p11(temp3hr, _date, _run, modelName[i], "3hr", "TEMP", resource._4_SHAPE_FILE_);
                    //runSagaCommandZonalForPointsRainSum_12_24WRF0p11(rainSum12hr, _date, _run, modelName[i], "3hr", "RAIN-12hr", resource._4_SHAPE_FILE_, "12hr");
                    //runSagaCommandZonalForPointsRainSum_12_24WRF0p11(rainSum24hr, _date, _run, modelName[i], "3hr", "RAIN-24hr", resource._4_SHAPE_FILE_, "24hr");
                    //runSagaCommandZonalForPointsRainSum_12_24WRF0p11(rainSum24hr, _date, _run, modelName[i], "3hr", "RAIN", resource._4_SHAPE_FILE_, "ALL");
                    Task.WaitAll(new Task[]{t1,t2});
                }
            }

            return true;
        }
        public static bool runSagaCommandZonalForPolygonsGFS0p13(string gridNames, string _date, string _run, string timeInterval, string variable, string shapefile)
        {
            string year = _date.Substring(0, 4);
            string month = _date.Substring(4, 2);
            string day = _date.Substring(6, 2);
            string csvName = "";
            Console.WriteLine("DEBUG: Zonal for Polygons  GFS for " + shapefile + " on variable " + variable + " " + timeInterval);
                string target = "";
                string workingDir = "";

                string sagaCmd = "";
                bool exited = false;

                target = Path.Combine(resource.GFS0p13ProcessedDB, year, month, day, _run, timeInterval, variable);
                Directory.CreateDirectory(target);
                csvName = Path.Combine(target, Path.GetFileNameWithoutExtension(shapefile) + "_" + year + month + day + _run + ".csv");
                target = Path.Combine(target, Path.GetFileNameWithoutExtension(shapefile) + "_" + year + month + day + _run + ".shp");

                workingDir = Path.Combine(resource.GFS0p13RawDataDB, year, month, day, _run, timeInterval, variable);

                Directory.SetCurrentDirectory(workingDir);

                sagaCmd = resource.SagaCmdDir + " -f=s shapes_grid 2";
                sagaCmd = sagaCmd + " -GRIDS=" + gridNames.Substring(0, gridNames.Length - 1);
                sagaCmd = sagaCmd + " -POLYGONS=" + shapefile;
                sagaCmd = sagaCmd + " -NAMING=1 -METHOD=2 -PARALLELIZED=0 -RESULT=" + target + " -COUNT=0 -MIN=0 -MAX=0 -RANGE=0 -SUM=0 -MEAN=1 -VAR=0 -STDDEV=0 -QUANTILE=0";
                Directory.SetCurrentDirectory(workingDir);

                Process cmd = new Process();
                cmd.StartInfo.WindowStyle = ProcessWindowStyle.Hidden;
                cmd.StartInfo.CreateNoWindow = true;
                cmd.StartInfo.FileName = @"cmd.exe";
                cmd.StartInfo.Arguments = @"/C " + sagaCmd;
                do
                {
                    cmd.Start();
                    exited = cmd.WaitForExit(600000);
                    if (exited != true)
                        cmd.Kill();
                } while (exited != true);

                exited = false;

                sagaCmd = resource.SagaCmdDir + " -f=s io_table 0 -TABLE=" + target + " -SEPARATOR=2 -FILENAME=" + csvName;
                cmd.StartInfo.Arguments = @"/C " + sagaCmd;
                do
                {
                    cmd.Start();
                    exited = cmd.WaitForExit(600000);
                    if (exited != true)
                        cmd.Kill();
                } while (exited != true);
            if (!File.Exists(csvName))
                throw new FileNotFoundException(csvName + " has not been created.");
        

            return true;
        }

        public static bool runSagaCommandZonalForPolygonsICON(string gridNames, string _date, string _run, string timeInterval, string variable, string shapefile)
        {
            string year = _date.Substring(0, 4);
            string month = _date.Substring(4, 2);
            string day = _date.Substring(6, 2);
            string csvName = "";
            Console.WriteLine("DEBUG: Zonal for Polygons  ICON for " + shapefile + " on variable " + variable + " " + timeInterval);
            string target = "";
            string workingDir = "";

            string sagaCmd = "";
            bool exited = false;

            target = Path.Combine(resource.ICON_ProccesedDB, year, month, day, _run, timeInterval, variable);
            Directory.CreateDirectory(target);
            csvName = Path.Combine(target, Path.GetFileNameWithoutExtension(shapefile) + "_" + year + month + day + _run + ".csv");
            target = Path.Combine(target, Path.GetFileNameWithoutExtension(shapefile) + "_" + year + month + day + _run + ".shp");

            workingDir = Path.Combine(resource.ICON_RawDB, year, month, day, _run, timeInterval, variable);

            Directory.SetCurrentDirectory(workingDir);

            sagaCmd = resource.SagaCmdDir + " -f=s shapes_grid 2";
            sagaCmd = sagaCmd + " -GRIDS=" + gridNames.Substring(0, gridNames.Length - 1);
            sagaCmd = sagaCmd + " -POLYGONS=" + shapefile;
            sagaCmd = sagaCmd + " -NAMING=1 -METHOD=2 -PARALLELIZED=0 -RESULT=" + target + " -COUNT=0 -MIN=0 -MAX=0 -RANGE=0 -SUM=0 -MEAN=1 -VAR=0 -STDDEV=0 -QUANTILE=0";
            Directory.SetCurrentDirectory(workingDir);

            Process cmd = new Process();
            cmd.StartInfo.WindowStyle = ProcessWindowStyle.Hidden;
            cmd.StartInfo.CreateNoWindow = true;
            cmd.StartInfo.FileName = @"cmd.exe";
            cmd.StartInfo.Arguments = @"/C " + sagaCmd;
            do
            {
                cmd.Start();
                exited = cmd.WaitForExit(600000);
                if (exited != true)
                    cmd.Kill();
            } while (exited != true);

            exited = false;

            sagaCmd = resource.SagaCmdDir + " -f=s io_table 0 -TABLE=" + target + " -SEPARATOR=2 -FILENAME=" + csvName;
            cmd.StartInfo.Arguments = @"/C " + sagaCmd;
            do
            {
                cmd.Start();
                exited = cmd.WaitForExit(600000);
                if (exited != true)
                    cmd.Kill();
            } while (exited != true);
            if (!File.Exists(csvName))
                throw new FileNotFoundException(csvName + " has not been created.");


            return true;
        }

        public static bool runSagaCommandZonalForPolygonsWRF0p11(string gridNames, string _date, string _run, string modelname, string timeInterval, string variable, string shapefile)
        {
            string year = _date.Substring(0, 4);
            string month = _date.Substring(4, 2);
            string day = _date.Substring(6, 2);
            string target = "";
            string workingDir = "";
            string csvName = "";
            Console.WriteLine("DEBUG: Zonal for polygons wrf for " + shapefile + " on variable " + variable + " " + timeInterval);
            string sagaCmd = "";
            bool exited = false;
            target = Path.Combine(resource.wrfProccessedDB, modelname, year, month, day, _run, timeInterval, variable);
            Directory.CreateDirectory(target);
            csvName = Path.Combine(target, Path.GetFileNameWithoutExtension(shapefile) + "_" + year + month + day + _run + ".csv");
            target = Path.Combine(target, Path.GetFileNameWithoutExtension(shapefile) + "_" + year + month + day + _run + ".shp");
                workingDir = Path.Combine(resource.wrfRawDB, modelname, year, month, day, _run, timeInterval, variable);

                Directory.SetCurrentDirectory(workingDir);

                sagaCmd = resource.SagaCmdDir + " -f=s shapes_grid 2";
                sagaCmd = sagaCmd + " -GRIDS=" + gridNames.Substring(0, gridNames.Length - 1);
                sagaCmd = sagaCmd + " -POLYGONS=" + shapefile;
                sagaCmd = sagaCmd + " -NAMING=1 -METHOD=2 -PARALLELIZED=0 -RESULT=" + target + " -COUNT=0 -MIN=0 -MAX=0 -RANGE=0 -SUM=0 -MEAN=1 -VAR=0 -STDDEV=0 -QUANTILE=0";
                Directory.SetCurrentDirectory(workingDir);

                Process cmd = new Process();
                cmd.StartInfo.WindowStyle = ProcessWindowStyle.Hidden;
                cmd.StartInfo.CreateNoWindow = true;
                cmd.StartInfo.FileName = @"cmd.exe";
                cmd.StartInfo.Arguments = @"/C " + sagaCmd;
            do
            {
                cmd.Start();
                exited = cmd.WaitForExit(600000);
                if (exited != true)
                    cmd.Kill();
            } while (exited != true);
            exited = false;

            sagaCmd = resource.SagaCmdDir + " -f=s io_table 0 -TABLE=" + target + " -SEPARATOR=2 -FILENAME=" + csvName;
                cmd.StartInfo.Arguments = @"/C " + sagaCmd;
            do
            {
                cmd.Start();
                exited = cmd.WaitForExit(600000);
                if (exited != true)
                    cmd.Kill();
            } while (exited != true);
            if (!File.Exists(csvName))
                throw new FileNotFoundException(csvName + " has not been created.");
            

            return true;
        }
        public static bool runSagaCommandZonalForPolygonsRainSum_12_24GFS0p13
    (string gridNames, string _date, string _run,
    string timeInterval, string variable, string shapefile, string time)
        {
            string target = "";
            string csvName = "";
            string year = _date.Substring(0, 4);
            string month = _date.Substring(4, 2);
            string day = _date.Substring(6, 2);
            Console.WriteLine("DEBUG: Zonal for polygons RAINSUM GFS for " + shapefile + " on variable " + variable + " " + timeInterval);
            string workingDir = "";
                string sagaCmd = "";
                bool exited = false;
                Process cmd = new Process();
                cmd.StartInfo.WindowStyle = ProcessWindowStyle.Hidden;
                cmd.StartInfo.CreateNoWindow = true;
                cmd.StartInfo.FileName = @"cmd.exe";

                target = Path.Combine(resource.GFS0p13ProcessedDB, year, month, day, _run, timeInterval, variable);
                Directory.CreateDirectory(target);
                csvName = Path.Combine(target, Path.GetFileNameWithoutExtension(shapefile) + "_" + time + "_" + year + month + day + _run + ".csv");
                target = Path.Combine(target, Path.GetFileNameWithoutExtension(shapefile) + "_" + time + "_" + year + month + day + _run + ".shp");
                    workingDir = Path.Combine(resource.GFS0p13RawDataDB, year, month, day, _run, timeInterval, variable);

                    Directory.SetCurrentDirectory(workingDir);

                    sagaCmd = resource.SagaCmdDir + " -f=s shapes_grid 2";
                    sagaCmd = sagaCmd + " -GRIDS=" + gridNames.Substring(0, gridNames.Length - 1);
                    sagaCmd = sagaCmd + " -POLYGONS=" + shapefile;
                    if (time == "ALL")
                        sagaCmd = sagaCmd + " -NAMING=1 -METHOD=2 -PARALLELIZED=0 -RESULT=" + target + " -COUNT=0 -MIN=0 -MAX=0 -RANGE=0 -SUM=0 -MEAN=1 -VAR=0 -STDDEV=0 -QUANTILE=0";
                    else
                        sagaCmd = sagaCmd + " -NAMING=1 -METHOD=2 -PARALLELIZED=0 -RESULT=" + target + " -COUNT=0 -MIN=0 -MAX=1 -RANGE=0 -SUM=0 -MEAN=0 -VAR=0 -STDDEV=0 -QUANTILE=0";

                    cmd.StartInfo.FileName = @"cmd.exe";
                    cmd.StartInfo.Arguments = @"/C " + sagaCmd;
                do
                {
                    cmd.Start();
                    exited = cmd.WaitForExit(600000);
                    if (exited != true)
                        cmd.Kill();
                } while (exited != true);
                exited = false;

                sagaCmd = resource.SagaCmdDir;
                    if (time == "ALL")
                        sagaCmd = sagaCmd + " -f=s table_calculus 17 -SUM=1 -MAX=1 -TABLE=" + target;
                    else
                        sagaCmd = sagaCmd + " -f=s table_calculus 17 -MEAN=1 -MAX=1 -TABLE=" + target;
                    if (time == "12hr")
                        sagaCmd = sagaCmd + " -FIELDS=15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34 -RESULT=" + target;
                    else if (time == "24hr")
                        sagaCmd = sagaCmd + " -FIELDS=15,16,17,18,19,20,21,22,23,24 -RESULT=" + target;
                    else if (time == "ALL" || time == "ALL_MAX")
                        sagaCmd = sagaCmd + " -FIELDS=15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94 -RESULT=" + target;
                    else
                        return false;
                    cmd.StartInfo.FileName = @"cmd.exe";
                    cmd.StartInfo.Arguments = @"/C " + sagaCmd;
                do
                {
                    cmd.Start();
                    exited = cmd.WaitForExit(600000);
                    if (exited != true)
                        cmd.Kill();
                } while (exited != true);
                exited = false;

                sagaCmd = sagaCmd = resource.SagaCmdDir + " -f=s io_table 0 -TABLE=" + target + " -SEPARATOR=2 -FILENAME=" + csvName;
                    cmd.StartInfo.Arguments = @"/C " + sagaCmd;
                do
                {
                    cmd.Start();
                    exited = cmd.WaitForExit(600000);
                    if (exited != true)
                        cmd.Kill();
                } while (exited != true);
            if (!File.Exists(csvName))
                throw new FileNotFoundException(csvName + " has not been created.");
            

            
            return true;
        }

        public static bool runSagaCommandZonalForPolygonsRainSum_12_24ICON
    (string gridNames, string _date, string _run,
    string timeInterval, string variable, string shapefile, string time)
        {
            string target = "";
            string csvName = "";
            string year = _date.Substring(0, 4);
            string month = _date.Substring(4, 2);
            string day = _date.Substring(6, 2);
            Console.WriteLine("DEBUG: Zonal for polygons RAINSUM ICON for " + shapefile + " on variable " + variable + " " + timeInterval);
            string workingDir = "";
            string sagaCmd = "";
            bool exited = false;
            Process cmd = new Process();
            cmd.StartInfo.WindowStyle = ProcessWindowStyle.Hidden;
            cmd.StartInfo.CreateNoWindow = true;
            cmd.StartInfo.FileName = @"cmd.exe";

            target = Path.Combine(resource.ICON_ProccesedDB, year, month, day, _run, timeInterval, variable);
            Directory.CreateDirectory(target);
            csvName = Path.Combine(target, Path.GetFileNameWithoutExtension(shapefile) + "_" + time + "_" + year + month + day + _run + ".csv");
            target = Path.Combine(target, Path.GetFileNameWithoutExtension(shapefile) + "_" + time + "_" + year + month + day + _run + ".shp");
            workingDir = Path.Combine(resource.ICON_RawDB, year, month, day, _run, timeInterval, variable);

            Directory.SetCurrentDirectory(workingDir);

            sagaCmd = resource.SagaCmdDir + " -f=s shapes_grid 2";
            sagaCmd = sagaCmd + " -GRIDS=" + gridNames.Substring(0, gridNames.Length - 1);
            sagaCmd = sagaCmd + " -POLYGONS=" + shapefile;
            if (time == "ALL")
                sagaCmd = sagaCmd + " -NAMING=1 -METHOD=2 -PARALLELIZED=0 -RESULT=" + target + " -COUNT=0 -MIN=0 -MAX=0 -RANGE=0 -SUM=0 -MEAN=1 -VAR=0 -STDDEV=0 -QUANTILE=0";
            else
                sagaCmd = sagaCmd + " -NAMING=1 -METHOD=2 -PARALLELIZED=0 -RESULT=" + target + " -COUNT=0 -MIN=0 -MAX=1 -RANGE=0 -SUM=0 -MEAN=0 -VAR=0 -STDDEV=0 -QUANTILE=0";

            cmd.StartInfo.FileName = @"cmd.exe";
            cmd.StartInfo.Arguments = @"/C " + sagaCmd;
            do
            {
                cmd.Start();
                exited = cmd.WaitForExit(600000);
                if (exited != true)
                    cmd.Kill();
            } while (exited != true);
            exited = false;

            sagaCmd = resource.SagaCmdDir;
            if (time == "ALL")
                sagaCmd = sagaCmd + " -f=s table_calculus 17 -SUM=1 -MAX=1 -TABLE=" + target;
            else
                sagaCmd = sagaCmd + " -f=s table_calculus 17 -MEAN=1 -MAX=1 -TABLE=" + target;
            if (time == "12hr")
                sagaCmd = sagaCmd + " -FIELDS=15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34 -RESULT=" + target;
            else if (time == "24hr")
                sagaCmd = sagaCmd + " -FIELDS=15,16,17,18,19,20,21,22,23,24 -RESULT=" + target;
            else if (time == "ALL" || time == "ALL_MAX")
                sagaCmd = sagaCmd + " -FIELDS=15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94 -RESULT=" + target;
            else
                return false;
            cmd.StartInfo.FileName = @"cmd.exe";
            cmd.StartInfo.Arguments = @"/C " + sagaCmd;
            do
            {
                cmd.Start();
                exited = cmd.WaitForExit(600000);
                if (exited != true)
                    cmd.Kill();
            } while (exited != true);
            exited = false;

            sagaCmd = sagaCmd = resource.SagaCmdDir + " -f=s io_table 0 -TABLE=" + target + " -SEPARATOR=2 -FILENAME=" + csvName;
            cmd.StartInfo.Arguments = @"/C " + sagaCmd;
            do
            {
                cmd.Start();
                exited = cmd.WaitForExit(600000);
                if (exited != true)
                    cmd.Kill();
            } while (exited != true);
            if (!File.Exists(csvName))
                throw new FileNotFoundException(csvName + " has not been created.");



            return true;
        }

        public static bool runSagaCommandZonalForPolygonsRainSum_12_24WRF0p11
    (string gridNames, string _date, string _run, string modelNames,
    string timeInterval, string variable, string shapefile, string time)
        {
            string target = "";
            string csvName = "";
            string year = _date.Substring(0, 4);
            string month = _date.Substring(4, 2);
            string day = _date.Substring(6, 2);
            Console.WriteLine("DEBUG: Zonal for polygons RAINSUM WRF for " + shapefile + " on variable " + variable + " " + timeInterval);
            string workingDir = "";
                string sagaCmd = "";
                bool exited = false;
                Process cmd = new Process();
                cmd.StartInfo.FileName = @"cmd.exe";
                cmd.StartInfo.UseShellExecute = false;
                cmd.StartInfo.CreateNoWindow = true;
                cmd.StartInfo.WindowStyle = ProcessWindowStyle.Hidden;

                target = Path.Combine(resource.wrfProccessedDB, modelNames, year, month, day, _run, timeInterval, variable);
                Directory.CreateDirectory(target);
                csvName = Path.Combine(target, Path.GetFileNameWithoutExtension(shapefile) + "_" + time + "_" + year + month + day + _run + ".csv");
                target = Path.Combine(target, Path.GetFileNameWithoutExtension(shapefile) + "_" + time + "_" + year + month + day + _run + ".shp");
                    workingDir = Path.Combine(resource.wrfRawDB, modelNames, year, month, day, _run, timeInterval, variable);

                    Directory.SetCurrentDirectory(workingDir);

                    sagaCmd = resource.SagaCmdDir + " -f=s shapes_grid 2";
                    sagaCmd = sagaCmd + " -GRIDS=" + gridNames.Substring(0, gridNames.Length - 1);
                    sagaCmd = sagaCmd + " -POLYGONS=" + shapefile;
                    if (time == "ALL")
                        sagaCmd = sagaCmd + " -NAMING=1 -METHOD=2 -PARALLELIZED=0 -RESULT=" + target + " -COUNT=0 -MIN=0 -MAX=0 -RANGE=0 -SUM=0 -MEAN=1 -VAR=0 -STDDEV=0 -QUANTILE=0";
                    else
                        sagaCmd = sagaCmd + " -NAMING=1 -METHOD=2 -PARALLELIZED=0 -RESULT=" + target + " -COUNT=0 -MIN=0 -MAX=1 -RANGE=0 -SUM=0 -MEAN=0 -VAR=0 -STDDEV=0 -QUANTILE=0";

                    cmd.StartInfo.FileName = @"cmd.exe";
                    cmd.StartInfo.Arguments = @"/C " + sagaCmd;
                do
                {
                    cmd.Start();
                    exited = cmd.WaitForExit(600000);
                    if (exited != true)
                        cmd.Kill();
                } while (exited != true);

                exited = false;

                sagaCmd = resource.SagaCmdDir;
                    if (time == "ALL")
                        sagaCmd = sagaCmd + " -f=s table_calculus 17 -SUM=1 -MAX=1 -TABLE=" + target;
                    else
                        sagaCmd = sagaCmd + " -f=s table_calculus 17 -MEAN=1 -MAX=1 -TABLE=" + target;
                    if (time == "12hr")
                        sagaCmd = sagaCmd + " -FIELDS=15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34 -RESULT=" + target;
                    else if (time == "24hr")
                        sagaCmd = sagaCmd + " -FIELDS=15,16,17,18,19,20,21,22,23,24 -RESULT=" + target;
                    else if (time == "ALL" || time == "ALL_MAX")
                        sagaCmd = sagaCmd + " -FIELDS=15,16,17,18,19,20,21,22,23,24,25,26,27,28,29,30,31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,84,85,86,87,88,89,90,91,92,93,94 -RESULT=" + target;
                    else
                        return false;
                    cmd.StartInfo.FileName = @"cmd.exe";
                    cmd.StartInfo.Arguments = @"/C " + sagaCmd;
                do
                {
                    cmd.Start();
                    exited = cmd.WaitForExit(600000);
                    if (exited != true)
                        cmd.Kill();
                } while (exited != true);
                exited = false;

                sagaCmd = sagaCmd = resource.SagaCmdDir + " -f=s io_table 0 -TABLE=" + target + " -SEPARATOR=2 -FILENAME=" + csvName;
                    cmd.StartInfo.Arguments = @"/C " + sagaCmd;
                do
                {
                    cmd.Start();
                    exited = cmd.WaitForExit(600000);
                    if (exited != true)
                        cmd.Kill();
                } while (exited != true);
            if (!File.Exists(csvName))
                throw new FileNotFoundException(csvName + " has not been created.");
            

            return true;
        }

        public static bool zonalForPolygons(string _date, string _run, string _var = "RAIN")
        {
            bool status = true;

            //string temp1hr = extractFileNamesGFS0p13(_date, _run, "TEMP", "1hr");
            //string rain1hr = extractFileNamesGFS0p13(_date, _run, "RAIN", "1hr");
            //string temp3hr = extractFileNamesGFS0p13(_date, _run, "TEMP", "3hr");
            string rain3hr = extractFileNamesGFS0p13(_date, _run, "RAIN", "3hr");
            string rainSum12hr = extractFileNamesGFS0p13(_date, _run, "RAIN-12hr", "3hr");
            string rainSum24hr = extractFileNamesGFS0p13(_date, _run, "RAIN-24hr", "3hr");

            if (_var == "APCP")
            {
                
                //string apcp1hr = extractFileNamesGFS0p13(_date, _run, "APCP", "1hr");
                string apcp3hr = extractFileNamesGFS0p13(_date, _run, "APCP", "3hr");
                //string apcpSum12hr = extractFileNamesGFS0p13(_date, _run, "APCP-12hr", "3hr");
                //string apcpSum24hr = extractFileNamesGFS0p13(_date, _run, "APCP-24hr", "3hr");
                //runSagaCommandZonalForPolygonsGFS0p13(apcp1hr, _date, _run, "1hr", "APCP", resource._2_SHPAE_FILE_);
                //runSagaCommandZonalForPolygonsGFS0p13(apcp3hr, _date, _run, "3hr", "APCP", resource._2_SHPAE_FILE_);
                //runSagaCommandZonalForPolygonsGFS0p13(apcp1hr, _date, _run, "1hr", "APCP", resource._5_SHAPE_FILE);
                //runSagaCommandZonalForPolygonsGFS0p13(apcp3hr, _date, _run, "3hr", "APCP", resource._5_SHAPE_FILE);
                //runSagaCommandZonalForPolygonsRainSum_12_24GFS0p13(apcpSum12hr, _date, _run, "3hr", "APCP-12hr", resource._2_SHPAE_FILE_, "12hr");
                //runSagaCommandZonalForPolygonsRainSum_12_24GFS0p13(apcpSum24hr, _date, _run, "3hr", "APCP-24hr", resource._2_SHPAE_FILE_, "24hr");
                var t11 = Task.Factory.StartNew(()=>runSagaCommandZonalForPolygonsRainSum_12_24GFS0p13(apcp3hr, _date, _run, "3hr", "APCP", resource._2_SHPAE_FILE_, "ALL"));
                Task.Factory.StartNew(()=>runSagaCommandZonalForPolygonsRainSum_12_24GFS0p13(apcp3hr, _date, _run, "3hr", "APCP", resource._2_SHPAE_FILE_, "ALL_MAX"));
                //runSagaCommandZonalForPolygonsRainSum_12_24GFS0p13(apcp1hr, _date, _run, "1hr", "APCP", resource._2_SHPAE_FILE_, "ALL");
                //runSagaCommandZonalForPolygonsRainSum_12_24GFS0p13(apcpSum12hr, _date, _run, "3hr", "APCP-12hr", resource._5_SHAPE_FILE, "12hr");
                //runSagaCommandZonalForPolygonsRainSum_12_24GFS0p13(apcpSum24hr, _date, _run, "3hr", "APCP-24hr", resource._5_SHAPE_FILE, "24hr");
                var t22 = Task.Factory.StartNew(()=>runSagaCommandZonalForPolygonsRainSum_12_24GFS0p13(apcp3hr, _date, _run, "3hr", "APCP", resource._5_SHAPE_FILE, "ALL"));
                var t33 = Task.Factory.StartNew(()=>runSagaCommandZonalForPolygonsRainSum_12_24GFS0p13(apcp3hr, _date, _run, "3hr", "APCP", resource._5_SHAPE_FILE, "ALL_MAX"));
                //runSagaCommandZonalForPolygonsRainSum_12_24GFS0p13(apcp1hr, _date, _run, "1hr", "APCP", resource._5_SHAPE_FILE, "ALL");
                Task.WaitAll(new Task[] {t11,t22,t33});
                return status;
            }
            //runSagaCommandZonalForPolygonsGFS0p13(temp1hr, _date, _run, "1hr", "TEMP", resource._2_SHPAE_FILE_);
            //runSagaCommandZonalForPolygonsGFS0p13(temp3hr, _date, _run, "3hr", "TEMP", resource._2_SHPAE_FILE_);
            //runSagaCommandZonalForPolygonsGFS0p13(temp1hr, _date, _run, "1hr", "TEMP", resource._5_SHAPE_FILE);
            //runSagaCommandZonalForPolygonsGFS0p13(temp3hr, _date, _run, "3hr", "TEMP", resource._5_SHAPE_FILE);
            //runSagaCommandZonalForPolygonsGFS0p13(temp3hr, _date, _run, "3hr", "TEMP", resource._5_SHAPE_FILE);
            //runSagaCommandZonalForPolygonsRainSum_12_24GFS0p13(rainSum12hr, _date, _run, "3hr", "RAIN-12hr", resource._2_SHPAE_FILE_, "12hr");
            //runSagaCommandZonalForPolygonsRainSum_12_24GFS0p13(rainSum24hr, _date, _run, "3hr", "RAIN-24hr", resource._2_SHPAE_FILE_, "24hr");
            var t1 = Task.Factory.StartNew(()=>runSagaCommandZonalForPolygonsRainSum_12_24GFS0p13(rain3hr, _date, _run, "3hr", "RAIN", resource._2_SHPAE_FILE_, "ALL"));
            Task.Factory.StartNew(()=>runSagaCommandZonalForPolygonsRainSum_12_24GFS0p13(rain3hr, _date, _run, "3hr", "RAIN", resource._2_SHPAE_FILE_, "ALL_MAX"));
            //runSagaCommandZonalForPolygonsRainSum_12_24GFS0p13(rain1hr, _date, _run, "1hr", "RAIN", resource._2_SHPAE_FILE_, "ALL");
            //runSagaCommandZonalForPolygonsRainSum_12_24GFS0p13(rainSum12hr, _date, _run, "3hr", "RAIN-12hr", resource._5_SHAPE_FILE, "12hr");
            //runSagaCommandZonalForPolygonsRainSum_12_24GFS0p13(rainSum24hr, _date, _run, "3hr", "RAIN-24hr", resource._5_SHAPE_FILE, "24hr");
            var t2= Task.Factory.StartNew(()=>runSagaCommandZonalForPolygonsRainSum_12_24GFS0p13(rain3hr, _date, _run, "3hr", "RAIN", resource._5_SHAPE_FILE, "ALL"));
            var t3 = Task.Factory.StartNew(()=>runSagaCommandZonalForPolygonsRainSum_12_24GFS0p13(rain3hr, _date, _run, "3hr", "RAIN", resource._5_SHAPE_FILE, "ALL_MAX"));
            //runSagaCommandZonalForPolygonsRainSum_12_24GFS0p13(rain1hr, _date, _run, "1hr", "RAIN", resource._5_SHAPE_FILE, "ALL");
            Task.WaitAll(new Task[] { t1, t2, t3 });
            return status;
        }

        public static bool zonalForPolygonsICON(string _date, string _run, string _var = "RAIN")
        {
            bool status = true;

            //string temp1hr = extractFileNamesGFS0p13(_date, _run, "TEMP", "1hr");
            //string rain1hr = extractFileNamesGFS0p13(_date, _run, "RAIN", "1hr");
            //string temp3hr = extractFileNamesGFS0p13(_date, _run, "TEMP", "3hr");
            string rain3hr = extractFileNamesICON(_date, _run, "RAIN", "3hr");
            //string rainSum12hr = extractFileNamesICON(_date, _run, "RAIN-12hr", "3hr");
            //string rainSum24hr = extractFileNamesICON(_date, _run, "RAIN-24hr", "3hr");

            if (_var == "APCP")
            {

                //string apcp1hr = extractFileNamesGFS0p13(_date, _run, "APCP", "1hr");
                string apcp3hr = extractFileNamesICON(_date, _run, "APCP", "3hr");
                //string apcpSum12hr = extractFileNamesGFS0p13(_date, _run, "APCP-12hr", "3hr");
                //string apcpSum24hr = extractFileNamesGFS0p13(_date, _run, "APCP-24hr", "3hr");
                //runSagaCommandZonalForPolygonsGFS0p13(apcp1hr, _date, _run, "1hr", "APCP", resource._2_SHPAE_FILE_);
                //runSagaCommandZonalForPolygonsGFS0p13(apcp3hr, _date, _run, "3hr", "APCP", resource._2_SHPAE_FILE_);
                //runSagaCommandZonalForPolygonsGFS0p13(apcp1hr, _date, _run, "1hr", "APCP", resource._5_SHAPE_FILE);
                //runSagaCommandZonalForPolygonsGFS0p13(apcp3hr, _date, _run, "3hr", "APCP", resource._5_SHAPE_FILE);
                //runSagaCommandZonalForPolygonsRainSum_12_24GFS0p13(apcpSum12hr, _date, _run, "3hr", "APCP-12hr", resource._2_SHPAE_FILE_, "12hr");
                //runSagaCommandZonalForPolygonsRainSum_12_24GFS0p13(apcpSum24hr, _date, _run, "3hr", "APCP-24hr", resource._2_SHPAE_FILE_, "24hr");
                var t11 = Task.Factory.StartNew(() => runSagaCommandZonalForPolygonsRainSum_12_24ICON(apcp3hr, _date, _run, "3hr", "APCP", resource._2_SHPAE_FILE_, "ALL"));
                Task.Factory.StartNew(() => runSagaCommandZonalForPolygonsRainSum_12_24ICON(apcp3hr, _date, _run, "3hr", "APCP", resource._2_SHPAE_FILE_, "ALL_MAX"));
                //runSagaCommandZonalForPolygonsRainSum_12_24GFS0p13(apcp1hr, _date, _run, "1hr", "APCP", resource._2_SHPAE_FILE_, "ALL");
                //runSagaCommandZonalForPolygonsRainSum_12_24GFS0p13(apcpSum12hr, _date, _run, "3hr", "APCP-12hr", resource._5_SHAPE_FILE, "12hr");
                //runSagaCommandZonalForPolygonsRainSum_12_24GFS0p13(apcpSum24hr, _date, _run, "3hr", "APCP-24hr", resource._5_SHAPE_FILE, "24hr");
                var t22 = Task.Factory.StartNew(() => runSagaCommandZonalForPolygonsRainSum_12_24ICON(apcp3hr, _date, _run, "3hr", "APCP", resource._5_SHAPE_FILE, "ALL"));
                var t33 = Task.Factory.StartNew(() => runSagaCommandZonalForPolygonsRainSum_12_24ICON(apcp3hr, _date, _run, "3hr", "APCP", resource._5_SHAPE_FILE, "ALL_MAX"));
                //runSagaCommandZonalForPolygonsRainSum_12_24GFS0p13(apcp1hr, _date, _run, "1hr", "APCP", resource._5_SHAPE_FILE, "ALL");
                Task.WaitAll(new Task[] { t11, t22, t33 });
                return status;
            }
            //runSagaCommandZonalForPolygonsGFS0p13(temp1hr, _date, _run, "1hr", "TEMP", resource._2_SHPAE_FILE_);
            //runSagaCommandZonalForPolygonsGFS0p13(temp3hr, _date, _run, "3hr", "TEMP", resource._2_SHPAE_FILE_);
            //runSagaCommandZonalForPolygonsGFS0p13(temp1hr, _date, _run, "1hr", "TEMP", resource._5_SHAPE_FILE);
            //runSagaCommandZonalForPolygonsGFS0p13(temp3hr, _date, _run, "3hr", "TEMP", resource._5_SHAPE_FILE);
            //runSagaCommandZonalForPolygonsGFS0p13(temp3hr, _date, _run, "3hr", "TEMP", resource._5_SHAPE_FILE);
            //runSagaCommandZonalForPolygonsRainSum_12_24GFS0p13(rainSum12hr, _date, _run, "3hr", "RAIN-12hr", resource._2_SHPAE_FILE_, "12hr");
            //runSagaCommandZonalForPolygonsRainSum_12_24GFS0p13(rainSum24hr, _date, _run, "3hr", "RAIN-24hr", resource._2_SHPAE_FILE_, "24hr");
            var t1 = Task.Factory.StartNew(() => runSagaCommandZonalForPolygonsRainSum_12_24ICON(rain3hr, _date, _run, "3hr", "RAIN", resource._2_SHPAE_FILE_, "ALL"));
            Task.Factory.StartNew(() => runSagaCommandZonalForPolygonsRainSum_12_24ICON(rain3hr, _date, _run, "3hr", "RAIN", resource._2_SHPAE_FILE_, "ALL_MAX"));
            //runSagaCommandZonalForPolygonsRainSum_12_24GFS0p13(rain1hr, _date, _run, "1hr", "RAIN", resource._2_SHPAE_FILE_, "ALL");
            //runSagaCommandZonalForPolygonsRainSum_12_24GFS0p13(rainSum12hr, _date, _run, "3hr", "RAIN-12hr", resource._5_SHAPE_FILE, "12hr");
            //runSagaCommandZonalForPolygonsRainSum_12_24GFS0p13(rainSum24hr, _date, _run, "3hr", "RAIN-24hr", resource._5_SHAPE_FILE, "24hr");
            var t2 = Task.Factory.StartNew(() => runSagaCommandZonalForPolygonsRainSum_12_24ICON(rain3hr, _date, _run, "3hr", "RAIN", resource._5_SHAPE_FILE, "ALL"));
            var t3 = Task.Factory.StartNew(() => runSagaCommandZonalForPolygonsRainSum_12_24ICON(rain3hr, _date, _run, "3hr", "RAIN", resource._5_SHAPE_FILE, "ALL_MAX"));
            //runSagaCommandZonalForPolygonsRainSum_12_24GFS0p13(rain1hr, _date, _run, "1hr", "RAIN", resource._5_SHAPE_FILE, "ALL");
            Task.WaitAll(new Task[] { t1, t2, t3 });
            return status;
        }

        public static bool zonalForPolygonsWRF(string _date, string _run, string _variable = "RAIN")
        {
            bool status = true;
            string[] modelName = { "ensemble", "ferrier", "lin", "wsm6" };
            for (int i = 0; i < 4; i++)
            {
                if (_variable == "APCP")
                {
                    string apcp3hr = extractFileNamesWRF0p11(_date, _run, modelName[i], "APCP", "3hr");
                    //string apcpSum12hr = extractFileNamesWRF0p11(_date, _run, modelName[i], "APCP-12hr", "3hr");
                    //string apcpSum24hr = extractFileNamesWRF0p11(_date, _run, modelName[i], "APCP-24hr", "3hr");

                    var t1 = Task.Factory.StartNew(()=>runSagaCommandZonalForPolygonsWRF0p11(apcp3hr, _date, _run, modelName[i], "3hr", "APCP", resource._2_SHPAE_FILE_));
                    var t2 = Task.Factory.StartNew(() => runSagaCommandZonalForPolygonsWRF0p11(apcp3hr, _date, _run, modelName[i], "3hr", "APCP", resource._5_SHAPE_FILE));
                    //runSagaCommandZonalForPolygonsRainSum_12_24WRF0p11(apcpSum12hr, _date, _run, modelName[i], "3hr", "APCP-12hr", resource._2_SHPAE_FILE_, "12hr");
                    //runSagaCommandZonalForPolygonsRainSum_12_24WRF0p11(apcpSum24hr, _date, _run, modelName[i], "3hr", "APCP-24hr", resource._2_SHPAE_FILE_, "24hr");
                    var t5 = Task.Factory.StartNew(() => runSagaCommandZonalForPolygonsRainSum_12_24WRF0p11(apcp3hr, _date, _run, modelName[i], "3hr", "APCP", resource._2_SHPAE_FILE_, "ALL"));
                    Task.Factory.StartNew(() => runSagaCommandZonalForPolygonsRainSum_12_24WRF0p11(apcp3hr, _date, _run, modelName[i], "3hr", "APCP", resource._2_SHPAE_FILE_, "ALL_MAX"));
                    //runSagaCommandZonalForPolygonsRainSum_12_24WRF0p11(apcpSum12hr, _date, _run, modelName[i], "3hr", "APCP-12hr", resource._5_SHAPE_FILE, "12hr");
                    //runSagaCommandZonalForPolygonsRainSum_12_24WRF0p11(apcpSum24hr, _date, _run, modelName[i], "3hr", "APCP-24hr", resource._5_SHAPE_FILE, "24hr");
                    var t3 = Task.Factory.StartNew(() => runSagaCommandZonalForPolygonsRainSum_12_24WRF0p11(apcp3hr, _date, _run, modelName[i], "3hr", "APCP", resource._5_SHAPE_FILE, "ALL"));
                    var t4 = Task.Factory.StartNew(() => runSagaCommandZonalForPolygonsRainSum_12_24WRF0p11(apcp3hr, _date, _run, modelName[i], "3hr", "APCP", resource._5_SHAPE_FILE, "ALL_MAX"));
                    Task.WaitAll(new Task[] { t1, t2,t3,t4,t5});
                }
                else
                {
                    //string temp3hr = extractFileNamesWRF0p11(_date, _run, modelName[i], "TEMP", "3hr");
                    string rain3hr = extractFileNamesWRF0p11(_date, _run, modelName[i], "RAIN", "3hr");
                    //string rainSum12hr = extractFileNamesWRF0p11(_date, _run, modelName[i], "RAIN-12hr", "3hr");
                    //string rainSum24hr = extractFileNamesWRF0p11(_date, _run, modelName[i], "RAIN-24hr", "3hr");

                    //runSagaCommandZonalForPolygonsWRF0p11(temp3hr, _date, _run, modelName[i], "3hr", "TEMP", resource._2_SHPAE_FILE_);
                    //runSagaCommandZonalForPolygonsWRF0p11(temp3hr, _date, _run, modelName[i], "3hr", "TEMP", resource._5_SHAPE_FILE);
                    //runSagaCommandZonalForPolygonsRainSum_12_24WRF0p11(rainSum12hr, _date, _run, modelName[i], "3hr", "RAIN-12hr", resource._2_SHPAE_FILE_, "12hr");
                    //runSagaCommandZonalForPolygonsRainSum_12_24WRF0p11(rainSum24hr, _date, _run, modelName[i], "3hr", "RAIN-24hr", resource._2_SHPAE_FILE_, "24hr");
                    var t1 = Task.Factory.StartNew(() => runSagaCommandZonalForPolygonsRainSum_12_24WRF0p11(rain3hr, _date, _run, modelName[i], "3hr", "RAIN", resource._2_SHPAE_FILE_, "ALL"));
                    var t2 = Task.Factory.StartNew(() => runSagaCommandZonalForPolygonsRainSum_12_24WRF0p11(rain3hr, _date, _run, modelName[i], "3hr", "RAIN", resource._2_SHPAE_FILE_, "ALL_MAX"));
                    //runSagaCommandZonalForPolygonsRainSum_12_24WRF0p11(rainSum12hr, _date, _run, modelName[i], "3hr", "RAIN-12hr", resource._5_SHAPE_FILE, "12hr");
                    //runSagaCommandZonalForPolygonsRainSum_12_24WRF0p11(rainSum24hr, _date, _run, modelName[i], "3hr", "RAIN-24hr", resource._5_SHAPE_FILE, "24hr");
                    var t3 = Task.Factory.StartNew(() => runSagaCommandZonalForPolygonsRainSum_12_24WRF0p11(rain3hr, _date, _run, modelName[i], "3hr", "RAIN", resource._5_SHAPE_FILE, "ALL"));
                    var t4 = Task.Factory.StartNew(() => runSagaCommandZonalForPolygonsRainSum_12_24WRF0p11(rain3hr, _date, _run, modelName[i], "3hr", "RAIN", resource._5_SHAPE_FILE, "ALL_MAX"));

                    Task.WaitAll(new Task[] { t1, t2, t3, t4 });
                }
            }
            return status;
        }

    }
}
