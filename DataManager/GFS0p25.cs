using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataManager
{
    class GFS0p25
    {
         List<string> variables = resource.GFS0p25VariablesList.Split(',').ToList();
         List<string> levels = resource.GFS0p25HeightList.Split(',').ToList();
        static string threeDigitNumber(int _num)
        {
            if (_num < 10)
                return "00" + Convert.ToString(_num);
            else if (_num < 100)
                return "0" + Convert.ToString(_num);
            else
                return Convert.ToString(_num);
        }
        static string twoDigitNumber(int _num)
        {
            if (_num < 10)
                return "0" + Convert.ToString(_num);
            else
                return Convert.ToString(_num);
        }
        static public Queue<string> urlGenerator(string _date, string _run, int isInit = 0)
        {
            string url = "";
            Queue<string> urls = new Queue<string>();
            if (isInit == 0)
            {
                List<string> variables = resource.GFS0p25VariablesList.Split(',').ToList();
                List<string> levels = resource.GFS0p25HeightList.Split(',').ToList();
                for (int i = 1; i < 121; i++)
                {
                    url = @"http://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?file=gfs.t";
                    url += _run;
                    url += @"z.pgrb2.0p25.f";
                    url += threeDigitNumber(i);
                    url += "&";

                    for (int j = 0; j < levels.Count; j++)
                        url += levels[j] + "=on&";
                    for (int j = 0; j < variables.Count; j++)
                        url += variables[j] + "=on&";

                    url += "subregion=&leftlon=";
                    url += resource.leftlon;
                    url += "&rightlon=";
                    url += resource.rightlon;
                    url += "&toplat=";
                    url += resource.toplat;
                    url += "&bottomlat=";
                    url += resource.bottomlat;
                    url += @"&dir=%2Fgfs.";
                    url += _date + _run;
                    urls.Enqueue(url);
                }
                for (int i = 123; i < 243; i += 3)
                {
                    url = @"http://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?file=gfs.t";
                    url += _run;
                    url += @"z.pgrb2.0p25.f";
                    url += threeDigitNumber(i);
                    url += "&";
                    for (int j = 0; j < levels.Count; j++)
                        url += levels[j] + "=on&";
                    for (int j = 0; j < variables.Count; j++)
                        url += variables[j] + "=on&";
                    url += "subregion=&leftlon=";
                    url += resource.leftlon;
                    url += "&rightlon=";
                    url += resource.rightlon;
                    url += "&toplat=";
                    url += resource.toplat;
                    url += "&bottomlat=";
                    url += resource.bottomlat;
                    url += @"&dir=%2Fgfs.";
                    url += _date + _run;
                    urls.Enqueue(url);
                }
            }
            else
            {
                List<string> variables = resource.gfs0p25InitVarList.Split(',').ToList();
                List<string> levels = resource.gfs0p25InitHeightList.Split(',').ToList();

                url = @"http://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_0p25.pl?file=gfs.t";
                url += _run;
                url += @"z.pgrb2.0p25.f";
                url += threeDigitNumber(1);
                url += "&";

                for (int j = 0; j < levels.Count; j++)
                    url += levels[j] + "=on&";
                for (int j = 0; j < variables.Count; j++)
                    url += variables[j] + "=on&";

                url += "subregion=&leftlon=";
                url += resource.leftlon;
                url += "&rightlon=";
                url += resource.rightlon;
                url += "&toplat=";
                url += resource.toplat;
                url += "&bottomlat=";
                url += resource.bottomlat;
                url += @"&dir=%2Fgfs.";
                url += _date + _run;
                urls.Enqueue(url);
            }
            
            return urls;
        }
        public static Queue<Tuple<string,string>> getFileNamesDownloaded(string _var, int init = 0)
        {
            Queue<Tuple<string, string>> fileNames = new Queue<Tuple<string, string>>();
            DirectoryInfo dirInfo = new DirectoryInfo(resource.GFS0p25DownloadOutputDir);
            if (init != 0)
                dirInfo = new DirectoryInfo(resource.GFS0p25InitDownloadOutputDir);
            FileInfo[] fileInfo = dirInfo.GetFiles().Select(fn => new FileInfo(fn.FullName)).OrderBy(f => f.Name).ToArray();
            Array.Sort(fileInfo, ATG.atgMethods.CompareNatural);
            string outAdress = "";
            if (_var == "TEMP")
                outAdress = resource.GFS0p25TiffDirTEMP;
            else if (_var == "APCP")
                outAdress = resource.GFS0p25TiffDirAPCP;
            else if (_var == "SNOW")
                outAdress = resource.GFS0p25TiffDirSNOW;

            if (_var != "SOILW")
                for (int i = 0; i < fileInfo.Length; i++)
                    fileNames.Enqueue(new Tuple<string, string>(fileInfo[i].FullName, Path.Combine(outAdress, fileInfo[i].Name + ".tif")));

            return fileNames;
        }
        public static bool convertAPCP2RAIN()
        {
            FileInfo[] tempFiles = new DirectoryInfo(resource.GFS0p25TiffDirTEMP).GetFiles().Select(fn => new FileInfo(fn.FullName)).OrderBy(f => f.Name).ToArray();
            Array.Sort(tempFiles, ATG.atgMethods.CompareNatural);
            FileInfo[] apcpFiles = new DirectoryInfo(resource.GFS0p25TiffDirAPCP).GetFiles().Select(fn => new FileInfo(fn.FullName)).OrderBy(f => f.Name).ToArray();
            Array.Sort(apcpFiles, ATG.atgMethods.CompareNatural);
            Queue<Tuple<string, string, string>> inputs = new Queue<Tuple<string, string, string>>();
            for (int i = 0; i < tempFiles.Length; i++)
                if (tempFiles[i].Name == apcpFiles[i].Name)
                    inputs.Enqueue(new Tuple<string, string, string>(tempFiles[i].FullName, apcpFiles[i].FullName, apcpFiles[i].Name));
                else
                    throw new Exception("Tempreture File Name does not match with apcp file name.");

            using (System.Threading.SemaphoreSlim concurrencySemaphore = new System.Threading.SemaphoreSlim(100))
            {
                List<Task> tasks = new List<Task>();
                foreach (var f in inputs)
                {
                    concurrencySemaphore.Wait();
                    var t = Task.Factory.StartNew(() =>
                    {
                        try
                        {
                            rasterCalculation.convertAPCP2RAIN(f.Item2, f.Item1, Path.Combine(resource.GFS0p25TiffDirRAIN, f.Item3));
                        }
                        finally
                        {
                            concurrencySemaphore.Release();
                        }
                    });
                    tasks.Add(t);
                }
                Task.WaitAll(tasks.ToArray());
            }
        return true;
        }
        private static bool comcumulative2Single1hr(string _year, string _month, string _day, string _run, string _var)
        {
            string inputDir = "";
            string outputDir = "";
            string year = _year;
            string month = _month;
            string day = _day;

            if (_var == "APCP")
                inputDir = resource.GFS0p25TiffDirAPCP;
            else if (_var == "RAIN")
                inputDir = resource.GFS0p25TiffDirRAIN;
            else
                throw new Exception("Error: selected variable does not exist.");

            outputDir = Path.Combine(resource.gfs0p25RawDataDB, year, month, day, _run, "1hr", _var);
            Directory.CreateDirectory(outputDir);

            FileInfo[] inputFiles = new DirectoryInfo(inputDir).GetFiles().Select(fn => new FileInfo(fn.FullName)).OrderBy(f => f.Name).ToArray();
            Array.Sort(inputFiles, ATG.atgMethods.CompareNatural);
            DateTime represetedTime = new DateTime(Convert.ToInt16(_year), Convert.ToInt16(_month), Convert.ToInt16(_day), Convert.ToInt16(_run), 0, 0);
            for (int i = 0; i < 120; i++)
            {
                represetedTime = represetedTime.AddHours(1);
                if ((i+1) % 6 != 1)
                rasterCalculation.substraction(inputFiles[i].FullName, inputFiles[i - 1].FullName, Path.Combine(outputDir,
                        "GFS0.25-RUN" + _year + _month + _day + _run + "-1hr-" + _var + "-" + represetedTime.Year.ToString()
                        + twoDigitNumber(represetedTime.Month) + twoDigitNumber(represetedTime.Day) + twoDigitNumber(represetedTime.Hour)) + ".tif");
                else
                    File.Copy(inputFiles[i].FullName, Path.Combine(outputDir,
                        "GFS0.25-RUN" + _year + _month + _day + _run + "-1hr-" + _var + "-" + represetedTime.Year.ToString()
                        + twoDigitNumber(represetedTime.Month) + twoDigitNumber(represetedTime.Day) + twoDigitNumber(represetedTime.Hour)) + ".tif");
            }
            return true;
        }
        private static bool comcumulative2Single3hr(string _year, string _month, string _day, string _run, string _var)
        {
            string inputDir = "";
            string outputDir = "";
            string year = _year;
            string month = _month;
            string day = _day;

            if (_var == "APCP")
                inputDir = resource.GFS0p25TiffDirAPCP;
            else if (_var == "RAIN")
                inputDir = resource.GFS0p25TiffDirRAIN;
            else
                throw new Exception("Error: selected variable does not exist.");

            outputDir = Path.Combine(resource.gfs0p25RawDataDB, year, month, day, _run, "3hr", _var);
            Directory.CreateDirectory(outputDir);

            DateTime represetedTime = new DateTime(Convert.ToInt16(_year), Convert.ToInt16(_month), Convert.ToInt16(_day), Convert.ToInt16(_run), 0, 0);

            FileInfo[] inputFiles = new DirectoryInfo(inputDir).GetFiles().Select(fn => new FileInfo(fn.FullName)).OrderBy(f => f.Name).ToArray();
            Array.Sort(inputFiles, ATG.atgMethods.CompareNatural);
            for (int i = 0; i <= 120; i += 3)
            {
                represetedTime = represetedTime.AddHours(3);
                if ((i+1) % 6 != 1)
                    rasterCalculation.substraction(inputFiles[i].FullName, inputFiles[i - 3].FullName, Path.Combine(outputDir,
                        "GFS0.25-RUN" + _year + _month + _day + _run + "-3hr-" + _var + "-" + represetedTime.Year.ToString()
                        + twoDigitNumber(represetedTime.Month) + twoDigitNumber(represetedTime.Day) + twoDigitNumber(represetedTime.Hour)) + ".tif");
                else
                    File.Copy(inputFiles[i].FullName, Path.Combine(outputDir,
                        "GFS0.25-RUN" + _year + _month + _day + _run + "-3hr-" + _var + "-" + represetedTime.Year.ToString()
                        + twoDigitNumber(represetedTime.Month) + twoDigitNumber(represetedTime.Day) + twoDigitNumber(represetedTime.Hour)) + ".tif");
            }
            for (int i = 1; i < 40; i++)
            {
                represetedTime = represetedTime.AddHours(3);
                if (((i * 3) + 120) % 6 != 1)
                    rasterCalculation.substraction(inputFiles[i + 120].FullName, inputFiles[i + 120 - 1].FullName, Path.Combine(outputDir,
                        "GFS0.25-RUN" + _year + _month + _day + _run + "-3hr-" + _var + "-" + represetedTime.Year.ToString()
                        + twoDigitNumber(represetedTime.Month) + twoDigitNumber(represetedTime.Day) + twoDigitNumber(represetedTime.Hour)) + ".tif");
                else
                    File.Copy(inputFiles[i + 120].FullName, Path.Combine(outputDir,
                        "GFS0.25-RUN" + _year + _month + _day + _run + "-3hr-" + _var + "-" + represetedTime.Year.ToString()
                        + twoDigitNumber(represetedTime.Month) + twoDigitNumber(represetedTime.Day) + twoDigitNumber(represetedTime.Hour)) + ".tif");
            }
            return true;
        }
        public static bool comcumulative2Single(string _date, string _run, int timeInterval, string _var)
        {
            string year = _date.Substring(0, 4);
            string month = _date.Substring(4, 2);
            string day = _date.Substring(6, 2);
            if (timeInterval == 1)
                comcumulative2Single1hr(year, month, day, _run, _var);
            else if (timeInterval == 3)
                comcumulative2Single3hr(year, month, day, _run, _var);
            else
                throw new Exception("Error, Entered time interval does not exist.");
            return true;
        }
        public static bool temp2RawDB(string _date, string _run)
        {
            string year = _date.Substring(0, 4);
            string month = _date.Substring(4, 2);
            string day = _date.Substring(6, 2);
            FileInfo[] inputFiles = new DirectoryInfo(resource.GFS0p25TiffDirTEMP).GetFiles().Select(fn => new FileInfo(fn.FullName)).OrderBy(f => f.Name).ToArray();
            Array.Sort(inputFiles, ATG.atgMethods.CompareNatural);
            DateTime represetedTime = new DateTime(Convert.ToInt16(year), Convert.ToInt16(month), Convert.ToInt16(day), Convert.ToInt16(_run), 0, 0);
            Directory.CreateDirectory(Path.Combine(resource.gfs0p25RawDataDB, year, month, day, _run, "3hr", "TEMP"));
            for (int i = 2; i < 123; i += 3)
            {
                represetedTime = represetedTime.AddHours(3);
                List<string> inputs = new List<string>();
                for (int j = 0; j < 3; j++)
                    inputs.Add(inputFiles[i - j].FullName);
                

                rasterCalculation.average(inputs, Path.Combine(resource.gfs0p25RawDataDB, year, month, day, _run, "3hr", "TEMP",
                    "GFS0.25-RUN" + year + month + day + _run + "-3hr-TEMP-" + represetedTime.Year.ToString()
                        + twoDigitNumber(represetedTime.Month) + twoDigitNumber(represetedTime.Day) + twoDigitNumber(represetedTime.Hour)) + ".tif");
                inputs.Clear();
            }
            for (int i = 1; i < 40; i++)
            {
                represetedTime = represetedTime.AddHours(3);
                    File.Copy(inputFiles[i + 120].FullName, Path.Combine(resource.gfs0p25RawDataDB, year, month, day, _run, "3hr", "TEMP",
                        "GFS0.25-RUN" + year + month + day + _run + "-3hr-TEMP-" + represetedTime.Year.ToString()
                        + twoDigitNumber(represetedTime.Month) + twoDigitNumber(represetedTime.Day) + twoDigitNumber(represetedTime.Hour)) + ".tif");
            }
            Directory.CreateDirectory(Path.Combine(resource.gfs0p25RawDataDB, year, month, day, _run, "1hr", "TEMP"));
            represetedTime = new DateTime(Convert.ToInt16(year), Convert.ToInt16(month), Convert.ToInt16(day), Convert.ToInt16(_run), 0, 0);
            for (int i = 0; i < 120; i ++)
            {
                represetedTime = represetedTime.AddHours(1);
                File.Copy(inputFiles[i].FullName, Path.Combine(resource.gfs0p25RawDataDB, year, month, day, _run, "1hr", "TEMP",
                        "GFS0.25-RUN" + year + month + day + _run + "-1hr-TEMP-" + represetedTime.Year.ToString()
                        + twoDigitNumber(represetedTime.Month) + twoDigitNumber(represetedTime.Day) + twoDigitNumber(represetedTime.Hour)) + ".tif");
            }
                
            return true;
        }

        public static bool snowSoildMoisture2RawDB(string _date, string _run)
        {
            string year = _date.Substring(0, 4);
            string month = _date.Substring(4, 2);
            string day = _date.Substring(6, 2);

            FileInfo[] files = new DirectoryInfo(resource.GFS0p25TiffDirSNOW).GetFiles().Select(fn => new FileInfo(fn.FullName)).OrderBy(f => f.Name).ToArray();
            Array.Sort(files, ATG.atgMethods.CompareNatural);
            Directory.CreateDirectory(Path.Combine(resource.gfs0p25RawDataDB, year, month, day, _run, "3hr", "SNOW"));
            foreach(var f in files)
                File.Copy(f.FullName, Path.Combine(resource.gfs0p25RawDataDB, year, month, day, _run, "3hr", "SNOW", f.Name));
            Directory.CreateDirectory(Path.Combine(resource.gfs0p25RawDataDB, year, month, day, _run, "3hr", "SOIL"));
            files = new DirectoryInfo(resource.GFS0p25TiffDirSOILW).GetFiles().Select(fn => new FileInfo(fn.FullName)).OrderBy(f => f.Name).ToArray();
            Array.Sort(files, ATG.atgMethods.CompareNatural);
            foreach (var f in files)
                File.Copy(f.FullName, Path.Combine(resource.gfs0p25RawDataDB, year, month, day, _run, "3hr", "SOIL", f.Name));

            return true;
        }


    }
}
