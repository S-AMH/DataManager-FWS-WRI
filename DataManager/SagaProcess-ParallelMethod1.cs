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

            foreach (var file in files)
                result = result + file.Name + sperator;

            return result;
        }
        public static bool runSagaCommandZonalForPointsGFS0p13
            (string gridNames, string _date, string _run, string timeInterval, string variable, string shapeFile)
        {
            string target = "";
            string csvName = "";
            string year = _date.Substring(0, 4);
            string month = _date.Substring(4, 2);
            string day = _date.Substring(6, 2);
            string workingDir = "";
            string sagaCmd = "";
            Process cmd = new Process();
            cmd.StartInfo.UseShellExecute = false;
            cmd.StartInfo.CreateNoWindow = true;
            cmd.StartInfo.WindowStyle = ProcessWindowStyle.Hidden;

            target = Path.Combine(resource.GFS0p13ProcessedDB, year, month, day, _run, timeInterval, variable);
            Directory.CreateDirectory(target);
            csvName = Path.Combine(target, Path.GetFileNameWithoutExtension(shapeFile) + "_" + year + month + day + _run + ".csv");
            target = Path.Combine(target, Path.GetFileNameWithoutExtension(shapeFile) + "_" + year + month + day + _run + ".shp");
            do
            {
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
                cmd.Start();
                cmd.WaitForExit();

                sagaCmd = sagaCmd = resource.SagaCmdDir + " -f=s io_table 0 -TABLE=" + target + " -SEPARATOR=2 -FILENAME=" + csvName;
                cmd.StartInfo.Arguments = @"/C " + sagaCmd;
                cmd.Start();
                cmd.WaitForExit();
                if (File.Exists(csvName))
                    break;
            } while (!File.Exists(csvName));

            return true;
        }
        public static bool runSagaCommandZonalForPointsWRF0p11
            (string gridNames, string _date, string _run,string modelName, string timeInterval, string variable, string shapeFile)
        {
            string target = "";
            string csvName = "";
            string year = _date.Substring(0, 4);
            string month = _date.Substring(4, 2);
            string day = _date.Substring(6, 2);
            string workingDir = "";
            string sagaCmd = "";
            Process cmd = new Process();
            cmd.StartInfo.UseShellExecute = false;
            cmd.StartInfo.CreateNoWindow = true;
            cmd.StartInfo.WindowStyle = ProcessWindowStyle.Hidden;

            target = Path.Combine(resource.wrfProccessedDB, modelName, year, month, day, _run, timeInterval, variable);
            Directory.CreateDirectory(target);
            csvName = Path.Combine(target, Path.GetFileNameWithoutExtension(shapeFile) + "_" + year + month + day + _run + ".csv");
            target = Path.Combine(target, Path.GetFileNameWithoutExtension(shapeFile) + "_" + year + month + day + _run + ".shp");
            do
            {
                workingDir = Path.Combine(resource.wrfRawDB, modelName, year, month, day, _run, timeInterval, variable);

                sagaCmd = resource.SagaCmdDir + " -f=s shapes_grid 0 -SHAPES=";
                sagaCmd = sagaCmd + shapeFile;
                sagaCmd = sagaCmd + " -GRIDS=" + gridNames.Substring(0, gridNames.Length - 1);
                sagaCmd = sagaCmd + " -RESULT=" + target + " -RESAMPLING=1";

                Directory.SetCurrentDirectory(workingDir);

                cmd.StartInfo.FileName = @"cmd.exe";
                cmd.StartInfo.Arguments = @"/C " + sagaCmd;
                cmd.Start();
                cmd.WaitForExit();

                sagaCmd = sagaCmd = resource.SagaCmdDir + " -f=s io_table 0 -TABLE=" + target + " -SEPARATOR=2 -FILENAME=" + csvName;
                cmd.StartInfo.Arguments = @"/C " + sagaCmd;
                cmd.Start();
                cmd.WaitForExit();
                if (File.Exists(csvName))
                    break;
            } while (!File.Exists(csvName));

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
            string workingDir = "";
            string sagaCmd = "";
            Process cmd = new Process();
            cmd.StartInfo.UseShellExecute = false;
            cmd.StartInfo.CreateNoWindow = true;
            cmd.StartInfo.WindowStyle = ProcessWindowStyle.Hidden;

            target = Path.Combine(resource.GFS0p13ProcessedDB, year, month, day, _run, timeInterval, variable);
            Directory.CreateDirectory(target);
            csvName = Path.Combine(target, Path.GetFileNameWithoutExtension(shapefile) + "_" + time + "_" + year + month + day + _run + ".csv");
            target = Path.Combine(target, Path.GetFileNameWithoutExtension(shapefile) + "_" + time + "_" + year + month + day + _run + ".shp");
            do
            {
                workingDir = Path.Combine(resource.GFS0p13RawDataDB, year, month, day, _run, timeInterval, variable);

                Directory.SetCurrentDirectory(workingDir);

                sagaCmd = resource.SagaCmdDir + " -f=s shapes_grid 0 -SHAPES=";
                sagaCmd = sagaCmd + shapefile;
                sagaCmd = sagaCmd + " -GRIDS=" + gridNames.Substring(0, gridNames.Length - 1);
                sagaCmd = sagaCmd + " -RESULT=" + target + " -RESAMPLING=1";

                cmd.StartInfo.FileName = @"cmd.exe";
                cmd.StartInfo.Arguments = @"/C " + sagaCmd;
                cmd.Start();
                cmd.WaitForExit();

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
                cmd.Start();
                cmd.WaitForExit();

                sagaCmd = sagaCmd = resource.SagaCmdDir + " -f=s io_table 0 -TABLE=" + target + " -SEPARATOR=2 -FILENAME=" + csvName;
                cmd.StartInfo.Arguments = @"/C " + sagaCmd;
                cmd.Start();
                cmd.WaitForExit();
                if (File.Exists(csvName))
                    break;
            } while (!File.Exists(csvName));


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
            string workingDir = "";
            string sagaCmd = "";
            Process cmd = new Process();
            cmd.StartInfo.UseShellExecute = false;
            cmd.StartInfo.CreateNoWindow = true;
            cmd.StartInfo.WindowStyle = ProcessWindowStyle.Hidden;

            target = Path.Combine(resource.wrfProccessedDB, modelName, year, month, day, _run, timeInterval, variable);
            Directory.CreateDirectory(target);
            csvName = Path.Combine(target, Path.GetFileNameWithoutExtension(shapefile) + "_" + time + "_" + year + month + day + _run + ".csv");
            target = Path.Combine(target, Path.GetFileNameWithoutExtension(shapefile) + "_" + time + "_" + year + month + day + _run + ".shp");
            do
            {
                workingDir = Path.Combine(resource.wrfRawDB, modelName, year, month, day, _run, timeInterval, variable);

                Directory.SetCurrentDirectory(workingDir);

                sagaCmd = resource.SagaCmdDir + " -f=s shapes_grid 0 -SHAPES=";
                sagaCmd = sagaCmd + shapefile;
                sagaCmd = sagaCmd + " -GRIDS=" + gridNames.Substring(0, gridNames.Length - 1);
                sagaCmd = sagaCmd + " -RESULT=" + target + " -RESAMPLING=1";

                cmd.StartInfo.FileName = @"cmd.exe";
                cmd.StartInfo.Arguments = @"/C " + sagaCmd;
                cmd.Start();
                cmd.WaitForExit();

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
                cmd.Start();
                cmd.WaitForExit();

                sagaCmd = sagaCmd = resource.SagaCmdDir + " -f=s io_table 0 -TABLE=" + target + " -SEPARATOR=2 -FILENAME=" + csvName;
                cmd.StartInfo.Arguments = @"/C " + sagaCmd;
                cmd.Start();
                cmd.WaitForExit();
                if (File.Exists(csvName))
                    break;
            } while (!File.Exists(csvName));
            return true;
        }

        public static bool zonalForPoints(string _date, string _run, string var="RAIN")
        { 
            string[] modelName = { "ensemble", "ferrier", "lin", "wsm6" };

            string temp1hr = extractFileNamesGFS0p13(_date, _run, "TEMP", "1hr");
            string rain1hr = extractFileNamesGFS0p13(_date, _run, "RAIN", "1hr");
            string temp3hr = extractFileNamesGFS0p13(_date, _run, "TEMP", "3hr");
            string rain3hr = extractFileNamesGFS0p13(_date, _run, "RAIN", "3hr");
            string rainSum12hr = extractFileNamesGFS0p13(_date, _run, "RAIN-12hr", "3hr");
            string rainSum24hr = extractFileNamesGFS0p13(_date, _run, "RAIN-24hr", "3hr");


            if (var == "APCP")
            {
                string apcp1hr = extractFileNamesGFS0p13(_date, _run, "APCP", "1hr");
                string apcpSum12hr = extractFileNamesGFS0p13(_date, _run, "APCP-12hr", "3hr");
                string apcpSum24hr = extractFileNamesGFS0p13(_date, _run, "APCP-24hr", "3hr");
                string apcp3hr = extractFileNamesGFS0p13(_date, _run, "APCP", "3hr");

                Task t1 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsGFS0p13(apcp1hr, _date, _run, "1hr", "APCP", resource._1_SHAPE_FILE_); });
                Task t2 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsGFS0p13(apcp3hr, _date, _run, "3hr", "APCP", resource._1_SHAPE_FILE_); });
                Task t3 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsGFS0p13(apcp1hr, _date, _run, "1hr", "APCP", resource._3_SHAPE_FILE_); });
                Task t4 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsGFS0p13(apcp3hr, _date, _run, "3hr", "APCP", resource._3_SHAPE_FILE_); });
                Task t5 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsGFS0p13(apcp1hr, _date, _run, "1hr", "APCP", resource._4_SHAPE_FILE_); });
                Task t6 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsGFS0p13(apcp3hr, _date, _run, "3hr", "APCP", resource._4_SHAPE_FILE_); });

                Task t7 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsRainSum_12_24GFS0p13(apcpSum12hr, _date, _run, "3hr", "APCP-12hr", resource._1_SHAPE_FILE_, "12hr"); });
                Task t8 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsRainSum_12_24GFS0p13(apcpSum24hr, _date, _run, "3hr", "APCP-24hr", resource._1_SHAPE_FILE_, "24hr"); });
                Task t9 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsRainSum_12_24GFS0p13(apcpSum24hr, _date, _run, "3hr", "APCP", resource._1_SHAPE_FILE_, "ALL"); });
                Task t10 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsRainSum_12_24GFS0p13(apcpSum24hr, _date, _run, "1hr", "APCP", resource._1_SHAPE_FILE_, "ALL"); });

                Task t11 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsRainSum_12_24GFS0p13(apcpSum12hr, _date, _run, "3hr", "APCP-12hr", resource._3_SHAPE_FILE_, "12hr"); });
                Task t12 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsRainSum_12_24GFS0p13(apcpSum24hr, _date, _run, "3hr", "APCP-24hr", resource._3_SHAPE_FILE_, "24hr"); });
                Task t13 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsRainSum_12_24GFS0p13(apcpSum24hr, _date, _run, "3hr", "APCP", resource._3_SHAPE_FILE_, "ALL"); });
                Task t14 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsRainSum_12_24GFS0p13(apcpSum24hr, _date, _run, "1hr", "APCP", resource._3_SHAPE_FILE_, "ALL"); });

                Task t15 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsRainSum_12_24GFS0p13(apcpSum12hr, _date, _run, "3hr", "APCP-12hr", resource._4_SHAPE_FILE_, "12hr"); });
                Task t16 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsRainSum_12_24GFS0p13(apcpSum24hr, _date, _run, "3hr", "APCP-24hr", resource._4_SHAPE_FILE_, "24hr"); });
                Task t17 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsRainSum_12_24GFS0p13(apcpSum24hr, _date, _run, "3hr", "APCP", resource._4_SHAPE_FILE_, "ALL"); });
                Task t18 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsRainSum_12_24GFS0p13(apcpSum24hr, _date, _run, "1hr", "APCP", resource._4_SHAPE_FILE_, "ALL"); });

                Task.WaitAll(t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12, t13, t14, t15, t16, t17, t18);

                return true;
            }

            Task tt1 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsGFS0p13(rain1hr, _date, _run, "1hr", "RAIN", resource._1_SHAPE_FILE_); });
            Task tt2 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsGFS0p13(temp1hr, _date, _run, "1hr", "TEMP", resource._1_SHAPE_FILE_); });

            Task tt3 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsGFS0p13(rain3hr, _date, _run, "3hr", "RAIN", resource._1_SHAPE_FILE_); });
            Task tt4 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsGFS0p13(temp3hr, _date, _run, "3hr", "TEMP", resource._1_SHAPE_FILE_); });

            Task tt5 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsRainSum_12_24GFS0p13(rainSum12hr, _date, _run, "3hr", "RAIN-12hr", resource._1_SHAPE_FILE_, "12hr"); });
            Task tt6 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsRainSum_12_24GFS0p13(rainSum24hr, _date, _run, "3hr", "RAIN-24hr", resource._1_SHAPE_FILE_, "24hr"); });
            Task tt7 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsRainSum_12_24GFS0p13(rainSum24hr, _date, _run, "3hr", "RAIN", resource._1_SHAPE_FILE_, "ALL"); });
            Task tt8 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsRainSum_12_24GFS0p13(rainSum24hr, _date, _run, "1hr", "RAIN", resource._1_SHAPE_FILE_, "ALL"); });
            Task tt9 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsGFS0p13(rain1hr, _date, _run, "1hr", "RAIN", resource._3_SHAPE_FILE_); });
            Task tt10 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsGFS0p13(temp1hr, _date, _run, "1hr", "TEMP", resource._3_SHAPE_FILE_); });

            Task tt11 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsGFS0p13(rain3hr, _date, _run, "3hr", "RAIN", resource._3_SHAPE_FILE_); });
            Task tt12 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsGFS0p13(temp3hr, _date, _run, "3hr", "TEMP", resource._3_SHAPE_FILE_); });

            Task tt13 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsRainSum_12_24GFS0p13(rainSum12hr, _date, _run, "3hr", "RAIN-12hr", resource._3_SHAPE_FILE_, "12hr"); });
            Task tt14 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsRainSum_12_24GFS0p13(rainSum24hr, _date, _run, "3hr", "RAIN-24hr", resource._3_SHAPE_FILE_, "24hr"); });
            Task tt15 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsRainSum_12_24GFS0p13(rainSum24hr, _date, _run, "3hr", "RAIN", resource._3_SHAPE_FILE_, "ALL"); });
            Task tt16 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsRainSum_12_24GFS0p13(rainSum24hr, _date, _run, "1hr", "RAIN", resource._3_SHAPE_FILE_, "ALL"); });
            Task tt17 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsGFS0p13(rain1hr, _date, _run, "1hr", "RAIN", resource._4_SHAPE_FILE_); });
            Task tt18 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsGFS0p13(temp1hr, _date, _run, "1hr", "TEMP", resource._4_SHAPE_FILE_); });

            Task tt19 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsGFS0p13(rain3hr, _date, _run, "3hr", "RAIN", resource._4_SHAPE_FILE_); });
            Task tt20 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsGFS0p13(temp3hr, _date, _run, "3hr", "TEMP", resource._4_SHAPE_FILE_); });


            Task tt21 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsRainSum_12_24GFS0p13(rainSum12hr, _date, _run, "3hr", "RAIN-12hr", resource._4_SHAPE_FILE_, "12hr"); });
            Task tt22 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsRainSum_12_24GFS0p13(rainSum24hr, _date, _run, "3hr", "RAIN-24hr", resource._4_SHAPE_FILE_, "24hr"); });
            Task tt23 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsRainSum_12_24GFS0p13(rainSum24hr, _date, _run, "3hr", "RAIN", resource._4_SHAPE_FILE_, "ALL"); });
            Task tt24 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsRainSum_12_24GFS0p13(rainSum24hr, _date, _run, "1hr", "RAIN", resource._4_SHAPE_FILE_, "ALL"); });

            Task.WaitAll(tt1, tt2, tt3, tt4, tt5, tt6, tt7, tt8, tt9, tt10, tt11, tt12, tt13, tt14, tt15, tt16, tt17, tt18, tt19, tt20, tt21, tt22, tt23, tt24);
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
                    string apcpSum12hr = extractFileNamesWRF0p11(_date, _run, modelName[i], "APCP-12hr", "3hr");
                    string apcpSum24hr = extractFileNamesWRF0p11(_date, _run, modelName[i], "APCP-24hr", "3hr");
                    Task t1 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsWRF0p11(apcp3hr, _date, _run, modelName[i], "3hr", "APCP", resource._1_SHAPE_FILE_); });
                    Task t2 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsWRF0p11(apcp3hr, _date, _run, modelName[i], "3hr", "APCP", resource._3_SHAPE_FILE_); });
                    Task t3 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsWRF0p11(apcp3hr, _date, _run, modelName[i], "3hr", "APCP", resource._4_SHAPE_FILE_); });
                    Task t4 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsRainSum_12_24WRF0p11(apcpSum12hr, _date, _run, modelName[i], "3hr", "APCP-12hr", resource._1_SHAPE_FILE_, "12hr"); });
                    Task t5 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsRainSum_12_24WRF0p11(apcpSum24hr, _date, _run, modelName[i], "3hr", "APCP-24hr", resource._1_SHAPE_FILE_, "24hr"); });
                    Task t6 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsRainSum_12_24WRF0p11(apcpSum24hr, _date, _run, modelName[i], "3hr", "APCP", resource._1_SHAPE_FILE_, "ALL"); });
                    Task t7 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsRainSum_12_24WRF0p11(apcpSum12hr, _date, _run, modelName[i], "3hr", "APCP-12hr", resource._3_SHAPE_FILE_, "12hr"); });
                    Task t8 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsRainSum_12_24WRF0p11(apcpSum24hr, _date, _run, modelName[i], "3hr", "APCP-24hr", resource._3_SHAPE_FILE_, "24hr"); });
                    Task t9 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsRainSum_12_24WRF0p11(apcpSum24hr, _date, _run, modelName[i], "3hr", "APCP", resource._3_SHAPE_FILE_, "ALL"); });
                    Task t10 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsRainSum_12_24WRF0p11(apcpSum12hr, _date, _run, modelName[i], "3hr", "APCP-12hr", resource._4_SHAPE_FILE_, "12hr"); });
                    Task t11 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsRainSum_12_24WRF0p11(apcpSum24hr, _date, _run, modelName[i], "3hr", "APCP-24hr", resource._4_SHAPE_FILE_, "24hr"); });
                    Task t12 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsRainSum_12_24WRF0p11(apcpSum24hr, _date, _run, modelName[i], "3hr", "APCP", resource._4_SHAPE_FILE_, "ALL"); });
                    Task.WaitAll(t1, t2, t3, t4, t5, t6, t7, t8, t9, t10, t11, t12);
                }
                else
                {

                    string temp3hr = extractFileNamesWRF0p11(_date, _run, modelName[i], "TEMP", "3hr");
                    string rain3hr = extractFileNamesWRF0p11(_date, _run, modelName[i], "RAIN", "3hr");
                    string rainSum12hr = extractFileNamesWRF0p11(_date, _run, modelName[i], "RAIN-12hr", "3hr");
                    string rainSum24hr = extractFileNamesWRF0p11(_date, _run, modelName[i], "RAIN-24hr", "3hr");

                    Task tt1 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsWRF0p11(rain3hr, _date, _run, modelName[i], "3hr", "RAIN", resource._1_SHAPE_FILE_); });
                    Task tt2 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsWRF0p11(temp3hr, _date, _run, modelName[i], "3hr", "TEMP", resource._1_SHAPE_FILE_); });

                    Task tt3 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsRainSum_12_24WRF0p11(rainSum12hr, _date, _run, modelName[i], "3hr", "RAIN-12hr", resource._1_SHAPE_FILE_, "12hr"); });
                    Task tt4 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsRainSum_12_24WRF0p11(rainSum24hr, _date, _run, modelName[i], "3hr", "RAIN-24hr", resource._1_SHAPE_FILE_, "24hr"); });
                    Task tt5 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsRainSum_12_24WRF0p11(rainSum24hr, _date, _run, modelName[i], "3hr", "RAIN", resource._1_SHAPE_FILE_, "ALL"); });
                    Task tt6 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsWRF0p11(rain3hr, _date, _run, modelName[i], "3hr", "RAIN", resource._3_SHAPE_FILE_); });
                    Task tt7 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsWRF0p11(temp3hr, _date, _run, modelName[i], "3hr", "TEMP", resource._3_SHAPE_FILE_); });

                    Task tt8 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsRainSum_12_24WRF0p11(rainSum12hr, _date, _run, modelName[i], "3hr", "RAIN-12hr", resource._3_SHAPE_FILE_, "12hr"); });
                    Task tt9 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsRainSum_12_24WRF0p11(rainSum24hr, _date, _run, modelName[i], "3hr", "RAIN-24hr", resource._3_SHAPE_FILE_, "24hr"); });
                    Task tt10 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsRainSum_12_24WRF0p11(rainSum24hr, _date, _run, modelName[i], "3hr", "RAIN", resource._3_SHAPE_FILE_, "ALL"); });
                    Task tt11 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsWRF0p11(rain3hr, _date, _run, modelName[i], "3hr", "RAIN", resource._4_SHAPE_FILE_); });
                    Task tt12 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsWRF0p11(temp3hr, _date, _run, modelName[i], "3hr", "TEMP", resource._4_SHAPE_FILE_); });

                    Task tt13 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsRainSum_12_24WRF0p11(rainSum12hr, _date, _run, modelName[i], "3hr", "RAIN-12hr", resource._4_SHAPE_FILE_, "12hr"); });
                    Task tt14 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsRainSum_12_24WRF0p11(rainSum24hr, _date, _run, modelName[i], "3hr", "RAIN-24hr", resource._4_SHAPE_FILE_, "24hr"); });
                    Task tt15 = Task.Factory.StartNew(() => { runSagaCommandZonalForPointsRainSum_12_24WRF0p11(rainSum24hr, _date, _run, modelName[i], "3hr", "RAIN", resource._4_SHAPE_FILE_, "ALL"); });
					Task.WaitAll(tt1,tt2,tt3,tt4,tt5,tt6,tt7,tt8,tt9,tt10,tt11,tt12,tt13,tt14,tt15);
                }
            }

            return true;
        }
        public static bool runSagaCommandZonalForPolygonsGFS0p13(string gridNames, string _date, string _run, string timeInterval, string variable, string shapefile)
        {
            string year = _date.Substring(0, 4);
            string month = _date.Substring(4, 2);
            string day = _date.Substring(6, 2);
            string target = "";
            string workingDir = "";
            string csvName = "";
            string sagaCmd = "";

            target = Path.Combine(resource.GFS0p13ProcessedDB, year, month, day, _run, timeInterval, variable);
            Directory.CreateDirectory(target);
            csvName = Path.Combine(target, Path.GetFileNameWithoutExtension(shapefile) + "_" + year + month + day + _run + ".csv");
            target = Path.Combine(target, Path.GetFileNameWithoutExtension(shapefile) + "_" + year + month + day + _run + ".shp");

            do
            {
                workingDir = Path.Combine(resource.GFS0p13RawDataDB, year, month, day, _run, timeInterval, variable);

                Directory.SetCurrentDirectory(workingDir);

                sagaCmd = resource.SagaCmdDir + " -f=s shapes_grid 2";
                sagaCmd = sagaCmd + " -GRIDS=" + gridNames.Substring(0, gridNames.Length - 1);
                sagaCmd = sagaCmd + " -POLYGONS=" + shapefile;
                sagaCmd = sagaCmd + " -NAMING=1 -METHOD=3 -PARALLELIZED=1 -RESULT=" + target + " -COUNT=0 -MIN=0 -MAX=0 -RANGE=0 -SUM=0 -MEAN=1 -VAR=0 -STDDEV=0 -QUANTILE=0";
                Directory.SetCurrentDirectory(workingDir);

                Process cmd = new Process();
                cmd.StartInfo.WindowStyle = ProcessWindowStyle.Hidden;
                cmd.StartInfo.CreateNoWindow = true;
                cmd.StartInfo.FileName = @"cmd.exe";
                cmd.StartInfo.Arguments = @"/C " + sagaCmd;
                cmd.Start();
                cmd.WaitForExit();

                sagaCmd = resource.SagaCmdDir + " -f=s io_table 0 -TABLE=" + target + " -SEPARATOR=2 -FILENAME=" + csvName;
                cmd.StartInfo.Arguments = @"/C " + sagaCmd;
                cmd.Start();
                cmd.WaitForExit();
            } while (!File.Exists(csvName));
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
            string sagaCmd = "";

            target = Path.Combine(resource.wrfProccessedDB, modelname, year, month, day, _run, timeInterval, variable);
            Directory.CreateDirectory(target);
            csvName = Path.Combine(target, Path.GetFileNameWithoutExtension(shapefile) + "_" + year + month + day + _run + ".csv");
            target = Path.Combine(target, Path.GetFileNameWithoutExtension(shapefile) + "_" + year + month + day + _run + ".shp");
            do
            {
                workingDir = Path.Combine(resource.wrfRawDB, modelname, year, month, day, _run, timeInterval, variable);

                Directory.SetCurrentDirectory(workingDir);

                sagaCmd = resource.SagaCmdDir + " -f=s shapes_grid 2";
                sagaCmd = sagaCmd + " -GRIDS=" + gridNames.Substring(0, gridNames.Length - 1);
                sagaCmd = sagaCmd + " -POLYGONS=" + shapefile;
                sagaCmd = sagaCmd + " -NAMING=1 -METHOD=3 -PARALLELIZED=1 -RESULT=" + target + " -COUNT=0 -MIN=0 -MAX=0 -RANGE=0 -SUM=0 -MEAN=1 -VAR=0 -STDDEV=0 -QUANTILE=0";
                Directory.SetCurrentDirectory(workingDir);

                Process cmd = new Process();
                cmd.StartInfo.WindowStyle = ProcessWindowStyle.Hidden;
                cmd.StartInfo.CreateNoWindow = true;
                cmd.StartInfo.FileName = @"cmd.exe";
                cmd.StartInfo.Arguments = @"/C " + sagaCmd;
                cmd.Start();
                cmd.WaitForExit();

                sagaCmd = resource.SagaCmdDir + " -f=s io_table 0 -TABLE=" + target + " -SEPARATOR=2 -FILENAME=" + csvName;
                cmd.StartInfo.Arguments = @"/C " + sagaCmd;
                cmd.Start();
                cmd.WaitForExit();
                if (File.Exists(csvName))
                    break;
            } while (!File.Exists(csvName));
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
            string workingDir = "";
            string sagaCmd = "";
            Process cmd = new Process();
            cmd.StartInfo.WindowStyle = ProcessWindowStyle.Hidden;
            cmd.StartInfo.CreateNoWindow = true;
            cmd.StartInfo.FileName = @"cmd.exe";

            target = Path.Combine(resource.GFS0p13ProcessedDB, year, month, day, _run, timeInterval, variable);
            Directory.CreateDirectory(target);
            csvName = Path.Combine(target, Path.GetFileNameWithoutExtension(shapefile) + "_" + time + "_" + year + month + day + _run + ".csv");
            target = Path.Combine(target, Path.GetFileNameWithoutExtension(shapefile) + "_" + time + "_" + year + month + day + _run + ".shp");
            do
            {
                workingDir = Path.Combine(resource.GFS0p13RawDataDB, year, month, day, _run, timeInterval, variable);

                Directory.SetCurrentDirectory(workingDir);

                sagaCmd = resource.SagaCmdDir + " -f=s shapes_grid 2";
                sagaCmd = sagaCmd + " -GRIDS=" + gridNames.Substring(0, gridNames.Length - 1);
                sagaCmd = sagaCmd + " -POLYGONS=" + shapefile;
                if (time == "ALL")
                    sagaCmd = sagaCmd + " -NAMING=1 -METHOD=3 -PARALLELIZED=1 -RESULT=" + target + " -COUNT=0 -MIN=0 -MAX=0 -RANGE=0 -SUM=0 -MEAN=1 -VAR=0 -STDDEV=0 -QUANTILE=0";
                else
                    sagaCmd = sagaCmd + " -NAMING=1 -METHOD=3 -PARALLELIZED=1 -RESULT=" + target + " -COUNT=0 -MIN=0 -MAX=1 -RANGE=0 -SUM=0 -MEAN=0 -VAR=0 -STDDEV=0 -QUANTILE=0";

                cmd.StartInfo.FileName = @"cmd.exe";
                cmd.StartInfo.Arguments = @"/C " + sagaCmd;
                cmd.Start();
                cmd.WaitForExit();

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
                cmd.Start();
                cmd.WaitForExit();

                sagaCmd = sagaCmd = resource.SagaCmdDir + " -f=s io_table 0 -TABLE=" + target + " -SEPARATOR=2 -FILENAME=" + csvName;
                cmd.StartInfo.Arguments = @"/C " + sagaCmd;
                cmd.Start();
                cmd.WaitForExit();
                if (File.Exists(csvName))
                    break;
            } while (!File.Exists(csvName));
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
            string workingDir = "";
            string sagaCmd = "";
            Process cmd = new Process();
            cmd.StartInfo.FileName = @"cmd.exe";
            cmd.StartInfo.UseShellExecute = false;
            cmd.StartInfo.CreateNoWindow = true;
            cmd.StartInfo.WindowStyle = ProcessWindowStyle.Hidden;

            target = Path.Combine(resource.wrfProccessedDB, modelNames, year, month, day, _run, timeInterval, variable);
            Directory.CreateDirectory(target);
            csvName = Path.Combine(target, Path.GetFileNameWithoutExtension(shapefile) + "_" + time + "_" + year + month + day + _run + ".csv");
            target = Path.Combine(target, Path.GetFileNameWithoutExtension(shapefile) + "_" + time + "_" + year + month + day + _run + ".shp");
            do
            {
                workingDir = Path.Combine(resource.wrfRawDB, modelNames, year, month, day, _run, timeInterval, variable);

                Directory.SetCurrentDirectory(workingDir);

                sagaCmd = resource.SagaCmdDir + " -f=s shapes_grid 2";
                sagaCmd = sagaCmd + " -GRIDS=" + gridNames.Substring(0, gridNames.Length - 1);
                sagaCmd = sagaCmd + " -POLYGONS=" + shapefile;
                if (time == "ALL")
                    sagaCmd = sagaCmd + " -NAMING=1 -METHOD=3 -PARALLELIZED=1 -RESULT=" + target + " -COUNT=0 -MIN=0 -MAX=0 -RANGE=0 -SUM=0 -MEAN=1 -VAR=0 -STDDEV=0 -QUANTILE=0";
                else
                    sagaCmd = sagaCmd + " -NAMING=1 -METHOD=3 -PARALLELIZED=1 -RESULT=" + target + " -COUNT=0 -MIN=0 -MAX=1 -RANGE=0 -SUM=0 -MEAN=0 -VAR=0 -STDDEV=0 -QUANTILE=0";

                cmd.StartInfo.FileName = @"cmd.exe";
                cmd.StartInfo.Arguments = @"/C " + sagaCmd;
                cmd.Start();
                cmd.WaitForExit();

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
                cmd.Start();
                cmd.WaitForExit();

                sagaCmd = sagaCmd = resource.SagaCmdDir + " -f=s io_table 0 -TABLE=" + target + " -SEPARATOR=2 -FILENAME=" + csvName;
                cmd.StartInfo.Arguments = @"/C " + sagaCmd;
                cmd.Start();
                cmd.WaitForExit();
                if (File.Exists(csvName))
                    break;
            } while (!File.Exists(csvName));
            return true;
        }

        public static bool zonalForPolygons(string _date, string _run, string _var = "RAIN")
        {
            bool status = true;

            string temp1hr = extractFileNamesGFS0p13(_date, _run, "TEMP", "1hr");
            string rain1hr = extractFileNamesGFS0p13(_date, _run, "RAIN", "1hr");
            string temp3hr = extractFileNamesGFS0p13(_date, _run, "TEMP", "3hr");
            string rain3hr = extractFileNamesGFS0p13(_date, _run, "RAIN", "3hr");
            string rainSum12hr = extractFileNamesGFS0p13(_date, _run, "RAIN-12hr", "3hr");
            string rainSum24hr = extractFileNamesGFS0p13(_date, _run, "RAIN-24hr", "3hr");


            if (_var == "APCP")
            {
                string apcp1hr = extractFileNamesGFS0p13(_date, _run, "APCP", "1hr");
                string apcp3hr = extractFileNamesGFS0p13(_date, _run, "APCP", "3hr");
                string apcpSum12hr = extractFileNamesGFS0p13(_date, _run, "APCP-12hr", "3hr");
                string apcpSum24hr = extractFileNamesGFS0p13(_date, _run, "APCP-24hr", "3hr");

                Task t1 = Task.Factory.StartNew(() => { runSagaCommandZonalForPolygonsGFS0p13(apcp1hr, _date, _run, "1hr", "APCP", resource._2_SHPAE_FILE_); });
                Task t2 = Task.Factory.StartNew(() => { runSagaCommandZonalForPolygonsGFS0p13(apcp3hr, _date, _run, "3hr", "APCP", resource._2_SHPAE_FILE_); });
                Task t3 = Task.Factory.StartNew(() => { runSagaCommandZonalForPolygonsGFS0p13(apcp1hr, _date, _run, "1hr", "APCP", resource._5_SHAPE_FILE); });
                Task t4 = Task.Factory.StartNew(() => { runSagaCommandZonalForPolygonsGFS0p13(apcp3hr, _date, _run, "3hr", "APCP", resource._5_SHAPE_FILE); });

                Task t5 = Task.Factory.StartNew(() => { runSagaCommandZonalForPolygonsRainSum_12_24GFS0p13(apcpSum12hr, _date, _run, "3hr", "APCP-12hr", resource._2_SHPAE_FILE_, "12hr"); });
                Task t6 = Task.Factory.StartNew(() => { runSagaCommandZonalForPolygonsRainSum_12_24GFS0p13(apcpSum24hr, _date, _run, "3hr", "APCP-24hr", resource._2_SHPAE_FILE_, "24hr"); });
                Task t7 = Task.Factory.StartNew(() => { runSagaCommandZonalForPolygonsRainSum_12_24GFS0p13(apcp3hr, _date, _run, "3hr", "APCP", resource._2_SHPAE_FILE_, "ALL"); });
                Task t8 = Task.Factory.StartNew(() => { runSagaCommandZonalForPolygonsRainSum_12_24GFS0p13(apcp3hr, _date, _run, "3hr", "APCP", resource._2_SHPAE_FILE_, "ALL_MAX"); });
                Task t9 = Task.Factory.StartNew(() => { runSagaCommandZonalForPolygonsRainSum_12_24GFS0p13(apcp1hr, _date, _run, "1hr", "APCP", resource._2_SHPAE_FILE_, "ALL"); });
                Task t10 = Task.Factory.StartNew(() => { runSagaCommandZonalForPolygonsRainSum_12_24GFS0p13(apcpSum12hr, _date, _run, "3hr", "APCP-12hr", resource._5_SHAPE_FILE, "12hr"); });
                Task t11 = Task.Factory.StartNew(() => { runSagaCommandZonalForPolygonsRainSum_12_24GFS0p13(apcpSum24hr, _date, _run, "3hr", "APCP-24hr", resource._5_SHAPE_FILE, "24hr"); });
                Task t12 = Task.Factory.StartNew(() => { runSagaCommandZonalForPolygonsRainSum_12_24GFS0p13(apcp3hr, _date, _run, "3hr", "APCP", resource._5_SHAPE_FILE, "ALL"); });
                Task t13 = Task.Factory.StartNew(() => { runSagaCommandZonalForPolygonsRainSum_12_24GFS0p13(apcp3hr, _date, _run, "3hr", "APCP", resource._5_SHAPE_FILE, "ALL_MAX"); });
                Task t14 = Task.Factory.StartNew(() => { runSagaCommandZonalForPolygonsRainSum_12_24GFS0p13(apcp1hr, _date, _run, "1hr", "APCP", resource._5_SHAPE_FILE, "ALL"); });
				Task.WaitAll(t1,t2,t3,t4,t5,t6,t7,t8,t9,t10,t11,t12,t13,t14);
            }

            
            Task tt1 = Task.Factory.StartNew(() => { runSagaCommandZonalForPolygonsGFS0p13(temp1hr, _date, _run, "1hr", "TEMP", resource._2_SHPAE_FILE_); });
            
            Task tt2 = Task.Factory.StartNew(() => { runSagaCommandZonalForPolygonsGFS0p13(temp3hr, _date, _run, "3hr", "TEMP", resource._2_SHPAE_FILE_); });
            
            Task tt3 = Task.Factory.StartNew(() => { runSagaCommandZonalForPolygonsGFS0p13(temp1hr, _date, _run, "1hr", "TEMP", resource._5_SHAPE_FILE); });
            Task tt4 = Task.Factory.StartNew(() => { runSagaCommandZonalForPolygonsGFS0p13(temp3hr, _date, _run, "3hr", "TEMP", resource._5_SHAPE_FILE); });
            Task tt5 = Task.Factory.StartNew(() => { runSagaCommandZonalForPolygonsGFS0p13(temp3hr, _date, _run, "3hr", "TEMP", resource._5_SHAPE_FILE); });
            Task tt6 = Task.Factory.StartNew(() => { runSagaCommandZonalForPolygonsRainSum_12_24GFS0p13(rainSum12hr, _date, _run, "3hr", "RAIN-12hr", resource._2_SHPAE_FILE_, "12hr"); });
            Task tt7 = Task.Factory.StartNew(() => { runSagaCommandZonalForPolygonsRainSum_12_24GFS0p13(rainSum24hr, _date, _run, "3hr", "RAIN-24hr", resource._2_SHPAE_FILE_, "24hr"); });
            Task tt8 = Task.Factory.StartNew(() => { runSagaCommandZonalForPolygonsRainSum_12_24GFS0p13(rain3hr, _date, _run, "3hr", "RAIN", resource._2_SHPAE_FILE_, "ALL"); });
            Task tt9 = Task.Factory.StartNew(() => { runSagaCommandZonalForPolygonsRainSum_12_24GFS0p13(rain3hr, _date, _run, "3hr", "RAIN", resource._2_SHPAE_FILE_, "ALL_MAX"); });
            Task tt10 = Task.Factory.StartNew(() => { runSagaCommandZonalForPolygonsRainSum_12_24GFS0p13(rain1hr, _date, _run, "1hr", "RAIN", resource._2_SHPAE_FILE_, "ALL"); });
            Task tt11 = Task.Factory.StartNew(() => { runSagaCommandZonalForPolygonsRainSum_12_24GFS0p13(rainSum12hr, _date, _run, "3hr", "RAIN-12hr", resource._5_SHAPE_FILE, "12hr"); });
            Task tt12 = Task.Factory.StartNew(() => { runSagaCommandZonalForPolygonsRainSum_12_24GFS0p13(rainSum24hr, _date, _run, "3hr", "RAIN-24hr", resource._5_SHAPE_FILE, "24hr"); });
            Task tt13 = Task.Factory.StartNew(() => { runSagaCommandZonalForPolygonsRainSum_12_24GFS0p13(rain3hr, _date, _run, "3hr", "RAIN", resource._5_SHAPE_FILE, "ALL"); });
            Task tt14 = Task.Factory.StartNew(() => { runSagaCommandZonalForPolygonsRainSum_12_24GFS0p13(rain3hr, _date, _run, "3hr", "RAIN", resource._5_SHAPE_FILE, "ALL_MAX"); });
            Task tt15 = Task.Factory.StartNew(() => { runSagaCommandZonalForPolygonsRainSum_12_24GFS0p13(rain1hr, _date, _run, "1hr", "RAIN", resource._5_SHAPE_FILE, "ALL"); });
			Task.WaitAll(tt1,tt2,tt3,tt4,tt5,tt6,tt7,tt8,tt9,tt10,tt11,tt12,tt13,tt14,tt15);
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
                    string apcpSum12hr = extractFileNamesWRF0p11(_date, _run, modelName[i], "APCP-12hr", "3hr");
                    string apcpSum24hr = extractFileNamesWRF0p11(_date, _run, modelName[i], "APCP-24hr", "3hr");
                    Task t1 = Task.Factory.StartNew(() => { runSagaCommandZonalForPolygonsWRF0p11(apcp3hr, _date, _run, modelName[i], "3hr", "APCP", resource._2_SHPAE_FILE_); });
                    Task t2 = Task.Factory.StartNew(() => { runSagaCommandZonalForPolygonsWRF0p11(apcp3hr, _date, _run, modelName[i], "3hr", "APCP", resource._5_SHAPE_FILE); });
                    Task t3 = Task.Factory.StartNew(() => { runSagaCommandZonalForPolygonsRainSum_12_24WRF0p11(apcpSum12hr, _date, _run, modelName[i], "3hr", "APCP-12hr", resource._2_SHPAE_FILE_, "12hr"); });
                    Task t4 = Task.Factory.StartNew(() => { runSagaCommandZonalForPolygonsRainSum_12_24WRF0p11(apcpSum24hr, _date, _run, modelName[i], "3hr", "APCP-24hr", resource._2_SHPAE_FILE_, "24hr"); });
                    Task t5 = Task.Factory.StartNew(() => { runSagaCommandZonalForPolygonsRainSum_12_24WRF0p11(apcp3hr, _date, _run, modelName[i], "3hr", "APCP", resource._2_SHPAE_FILE_, "ALL"); });
                    Task t6 = Task.Factory.StartNew(() => { runSagaCommandZonalForPolygonsRainSum_12_24WRF0p11(apcp3hr, _date, _run, modelName[i], "3hr", "APCP", resource._2_SHPAE_FILE_, "ALL_MAX"); });

                    Task t7 = Task.Factory.StartNew(() => { runSagaCommandZonalForPolygonsRainSum_12_24WRF0p11(apcpSum12hr, _date, _run, modelName[i], "3hr", "APCP-12hr", resource._5_SHAPE_FILE, "12hr"); });
                    Task t8 = Task.Factory.StartNew(() => { runSagaCommandZonalForPolygonsRainSum_12_24WRF0p11(apcpSum24hr, _date, _run, modelName[i], "3hr", "APCP-24hr", resource._5_SHAPE_FILE, "24hr"); });
                    Task t9 = Task.Factory.StartNew(() => { runSagaCommandZonalForPolygonsRainSum_12_24WRF0p11(apcp3hr, _date, _run, modelName[i], "3hr", "APCP", resource._5_SHAPE_FILE, "ALL"); });
                    Task t10 = Task.Factory.StartNew(() => { runSagaCommandZonalForPolygonsRainSum_12_24WRF0p11(apcp3hr, _date, _run, modelName[i], "3hr", "APCP", resource._5_SHAPE_FILE, "ALL_MAX"); });
					Task.WaitAll(t1,t2,t3,t4,t5,t6,t7,t8,t9,t10);
                }
                else
                {
                    string temp3hr = extractFileNamesWRF0p11(_date, _run, modelName[i], "TEMP", "3hr");
                    string rain3hr = extractFileNamesWRF0p11(_date, _run, modelName[i], "RAIN", "3hr");
                    string rainSum12hr = extractFileNamesWRF0p11(_date, _run, modelName[i], "RAIN-12hr", "3hr");
                    string rainSum24hr = extractFileNamesWRF0p11(_date, _run, modelName[i], "RAIN-24hr", "3hr");




                    Task tt1 = Task.Factory.StartNew(() => { runSagaCommandZonalForPolygonsWRF0p11(temp3hr, _date, _run, modelName[i], "3hr", "TEMP", resource._2_SHPAE_FILE_); });

                    Task tt2 = Task.Factory.StartNew(() => { runSagaCommandZonalForPolygonsWRF0p11(temp3hr, _date, _run, modelName[i], "3hr", "TEMP", resource._5_SHAPE_FILE); });
                    Task tt3 = Task.Factory.StartNew(() => { runSagaCommandZonalForPolygonsRainSum_12_24WRF0p11(rainSum12hr, _date, _run, modelName[i], "3hr", "RAIN-12hr", resource._2_SHPAE_FILE_, "12hr"); });
                    Task tt4 = Task.Factory.StartNew(() => { runSagaCommandZonalForPolygonsRainSum_12_24WRF0p11(rainSum24hr, _date, _run, modelName[i], "3hr", "RAIN-24hr", resource._2_SHPAE_FILE_, "24hr"); });
                    Task tt5 = Task.Factory.StartNew(() => { runSagaCommandZonalForPolygonsRainSum_12_24WRF0p11(rain3hr, _date, _run, modelName[i], "3hr", "RAIN", resource._2_SHPAE_FILE_, "ALL"); });
                    Task tt6 = Task.Factory.StartNew(() => { runSagaCommandZonalForPolygonsRainSum_12_24WRF0p11(rain3hr, _date, _run, modelName[i], "3hr", "RAIN", resource._2_SHPAE_FILE_, "ALL_MAX"); });



                    Task tt7 = Task.Factory.StartNew(() => { runSagaCommandZonalForPolygonsRainSum_12_24WRF0p11(rainSum12hr, _date, _run, modelName[i], "3hr", "RAIN-12hr", resource._5_SHAPE_FILE, "12hr"); });
                    Task tt8 = Task.Factory.StartNew(() => { runSagaCommandZonalForPolygonsRainSum_12_24WRF0p11(rainSum24hr, _date, _run, modelName[i], "3hr", "RAIN-24hr", resource._5_SHAPE_FILE, "24hr"); });
                    Task tt9 = Task.Factory.StartNew(() => { runSagaCommandZonalForPolygonsRainSum_12_24WRF0p11(rain3hr, _date, _run, modelName[i], "3hr", "RAIN", resource._5_SHAPE_FILE, "ALL"); });
                    Task tt10 = Task.Factory.StartNew(() => { runSagaCommandZonalForPolygonsRainSum_12_24WRF0p11(rain3hr, _date, _run, modelName[i], "3hr", "RAIN", resource._5_SHAPE_FILE, "ALL_MAX"); });
					Task.WaitAll(tt1,tt2,tt3,tt4,tt5,tt6,tt7,tt8,tt9,tt10);
                }
            }
            return status;
        }

    }
}
