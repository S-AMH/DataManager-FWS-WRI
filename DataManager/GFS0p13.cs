using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataManager
{
    class GFS0p13
    {

        static string twoDigitNumber(int _num)
        {
            if (_num < 10)
                return "0" + Convert.ToString(_num);
            else
                return Convert.ToString(_num);
        }

        static public bool commulicativeRAIN(string _date, string _run)
        {
            string year = _date.Substring(0, 4);
            string month = _date.Substring(4, 2);
            string day = _date.Substring(6, 2);
            if (Directory.Exists(Path.Combine(resource.GFS0p13RawDataDB,
                    year, month, day, _run, "3hr", "CumulicativeRaster3hr")))
                Directory.Delete(Path.Combine(resource.GFS0p13RawDataDB,
                year, month, day, _run, "3hr", "CumulicativeRaster3hr"), true);
            Directory.CreateDirectory(Path.Combine(resource.GFS0p13RawDataDB,
                    year, month, day, _run, "3hr", "CumulicativeRaster3hr"));
            FileInfo[] inputs = new DirectoryInfo(Path.Combine(resource.GFS0p13RawDataDB, year, month, day, _run,"3hr","RAIN")).GetFiles("*.tif").Select(fn => new FileInfo(fn.FullName)).OrderBy(f => f.Name).ToArray();
            List<string> sumInputs = new List<string>();
            for (int i = 1; i < inputs.Count()+1; i++)
            {
                for (int j = 0; j < i; j++)
                    sumInputs.Add(inputs[j].FullName);
                rasterCalculation.sum(sumInputs, Path.Combine(resource.GFS0p13RawDataDB,
                    year, month, day, _run,"3hr", "CumulicativeRaster3hr","CumulicativeRaster-RAIN-"+i+"-"+
                    new FileInfo(sumInputs.Last()).Name.Substring(new FileInfo(sumInputs.Last()).Name.Length - 14, 10)+".tif"));
                sumInputs.Clear();
            }
            return true;
        }

        static public bool commulicativeAPCP(string _date, string _run)
        {
            string year = _date.Substring(0, 4);
            string month = _date.Substring(4, 2);
            string day = _date.Substring(6, 2);
            if (Directory.Exists(Path.Combine(resource.GFS0p13RawDataDB,
                    year, month, day, _run, "3hr", "CumulicativeRaster3hrAPCP")))
                Directory.Delete(Path.Combine(resource.GFS0p13RawDataDB,
                year, month, day, _run, "3hr", "CumulicativeRaster3hrAPCP"), true);
            Directory.CreateDirectory(Path.Combine(resource.GFS0p13RawDataDB,
                    year, month, day, _run, "3hr", "CumulicativeRaster3hrAPCP"));
            FileInfo[] inputs = new DirectoryInfo(Path.Combine(resource.GFS0p13RawDataDB, year, month, day, _run, "3hr", "APCP")).GetFiles("*.tif").Select(fn => new FileInfo(fn.FullName)).OrderBy(f => f.Name).ToArray();
            List<string> sumInputs = new List<string>();
            for (int i = 1; i < inputs.Count() + 1; i++)
            {
                for (int j = 0; j < i; j++)
                    sumInputs.Add(inputs[j].FullName);
                rasterCalculation.sum(sumInputs, Path.Combine(resource.GFS0p13RawDataDB,
                    year, month, day, _run, "3hr", "CumulicativeRaster3hrAPCP", "CumulicativeRaster-APCP-" + i + "-" +
                    new FileInfo(sumInputs.Last()).Name.Substring(new FileInfo(sumInputs.Last()).Name.Length - 14, 10) + ".tif"));
                sumInputs.Clear();
            }
            return true;
        }

        static public Queue<string> urlGenerator(string _date, string _run, int init = 0)
        {
            Queue<string> urls = new Queue<string>();
            string url;

            if (init == 0)
            {
                List<string> variables = resource.GFS0p13VariablesList.Split(',').ToList();
                List<string> levels = resource.GFS0p13HeightList.Split(',').ToList();
                for (int i = 1; i < 121; i++)
                {
                    url = @"https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_sflux.pl?file=gfs.t";
                    url += _run;
                    url += "z.sfluxgrbf";
                    url += twoDigitNumber(i);
                    url += ".grib2&";
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
                    url = @"https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_sflux.pl?file=gfs.t";
                    url += _run;
                    url += "z.sfluxgrbf";
                    url += twoDigitNumber(i);
                    url += ".grib2&";
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
                List<string> variables = resource.gfs0p13InitVarList.Split(',').ToList();
                List<string> levels = resource.gfs0p13InitHeightList.Split(',').ToList();
                url = @"https://nomads.ncep.noaa.gov/cgi-bin/filter_gfs_sflux.pl?file=gfs.t";
                url += _run;
                url += "z.sfluxgrbf";
                url += twoDigitNumber(1);
                url += ".grib2&";
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
        public static Queue<Tuple<string, string>> getFileNamesDownloaded(string _var, int init=0)
        {
            Queue<Tuple<string, string>> fileNames = new Queue<Tuple<string, string>>();
            DirectoryInfo dirInfo = new DirectoryInfo(resource.GFS0p13DownloadOutputDir);
            if (init != 0)
                dirInfo = new DirectoryInfo(resource.GFS0p13InitDownloadOutputDir);
            FileInfo[] fileInfo = dirInfo.GetFiles().Select(fn => new FileInfo(fn.FullName)).OrderBy(f => f.Name).ToArray();
            string outAdress = "";
            if (_var == "TEMP")
                outAdress = resource.GFS0p13TiffDirTEMP;
            else if (_var == "PRATE")
                outAdress = resource.GFS0p13TiffDirPRate;
            else if (_var == "SNOW")
                outAdress = resource.GFS0p13TiffDirSNOW;
            else if (_var == "SOILW")
                outAdress = resource.GFS0p13TiffDirSOILW;
            if(Directory.Exists(outAdress))
                Directory.Delete(outAdress,true);
            Directory.CreateDirectory(outAdress);

            if (_var != "SOILW")
                for (int i = 0; i < fileInfo.Length; i++)
                    fileNames.Enqueue(new Tuple<string, string>(fileInfo[i].FullName, Path.Combine(outAdress, fileInfo[i].Name + ".tif")));
            else if (_var == "SOILW")
            {
                fileNames.Enqueue(new Tuple<string, string>(fileInfo[0].FullName, Path.Combine(outAdress, fileInfo[0].Name + "0-0.1.tif")));
                fileNames.Enqueue(new Tuple<string, string>(fileInfo[0].FullName, Path.Combine(outAdress, fileInfo[0].Name + "0.1-0.4.tif")));
                fileNames.Enqueue(new Tuple<string, string>(fileInfo[0].FullName, Path.Combine(outAdress, fileInfo[0].Name + "0.4-1.tif")));
                fileNames.Enqueue(new Tuple<string, string>(fileInfo[0].FullName, Path.Combine(outAdress, fileInfo[0].Name + "1-2.tif")));
            }

            return fileNames;
        }
        public static bool ConvertPRATE2APCP()
        {
            DirectoryInfo dirPrate = new DirectoryInfo(resource.GFS0p13TiffDirPRate);
            FileInfo[] prateFiles = dirPrate.GetFiles().Select(fn => new FileInfo(fn.FullName)).OrderBy(f => f.Name).ToArray();

            if (Directory.Exists(resource.GFS0p13TiffDirAPCP))
                Directory.Delete(resource.GFS0p13TiffDirAPCP, true);
            Directory.CreateDirectory(resource.GFS0p13TiffDirAPCP);

                List<Task> tasks = new List<Task>();
                foreach (var f in prateFiles)
                {
                    var t = Task.Factory.StartNew(() =>
                    {
                        try
                        {
                            rasterCalculation.convertPrate2APCP(f.FullName, Path.Combine(resource.GFS0p13TiffDirAPCP, f.Name), 0.0);
                        }
                        catch
                        {
                            throw new Exception("Error: convertPrate2APCP");
                        }
                    });
                    tasks.Add(t);
                }
                Task.WaitAll(tasks.ToArray());
            return true;
        }
        public static bool convertAPCP2RAIN()
        {
            FileInfo[] tempFiles = new DirectoryInfo(resource.GFS0p13TiffDirTEMP).GetFiles().Select(fn => new FileInfo(fn.FullName)).OrderBy(f => f.Name).ToArray();
            FileInfo[] apcpFiles = new DirectoryInfo(resource.GFS0p13TiffDirAPCP).GetFiles().Select(fn => new FileInfo(fn.FullName)).OrderBy(f => f.Name).ToArray();
            if (Directory.Exists(resource.GFS0p13TiffDirRAIN))
                Directory.Delete(resource.GFS0p13TiffDirRAIN, true);
            Directory.CreateDirectory(resource.GFS0p13TiffDirRAIN);
            Queue<Tuple<string, string, string>> inputs = new Queue<Tuple<string, string, string>>();
            for (int i = 0; i < tempFiles.Length; i++)
                if (tempFiles[i].Name == apcpFiles[i].Name)
                    inputs.Enqueue(new Tuple<string, string, string>(tempFiles[i].FullName, apcpFiles[i].FullName, apcpFiles[i].Name));
                else
                    throw new Exception("Tempreture File Name does not match with apcp file name.");

                List<Task> tasks = new List<Task>();
                foreach (var f in inputs)
                {
                    var t = Task.Factory.StartNew(() =>
                    {
                        try
                        {
                            rasterCalculation.convertAPCP2RAIN(f.Item2, f.Item1, Path.Combine(resource.GFS0p13TiffDirRAIN, f.Item3));
                        }
                        catch
                        {
                            throw new Exception("Error: ConvertAPCP2RAIN");
                        }
                    });
                    tasks.Add(t);
                }
            Task.WaitAll(tasks.ToArray());
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
                inputDir = resource.GFS0p13TiffDirAPCP;
            else if (_var == "RAIN")
                inputDir = resource.GFS0p13TiffDirRAIN;
            else if (_var == "PRATE")
                inputDir = resource.GFS0p13TiffDirPRate;
            else
                throw new Exception("Error: selected variable does not exist.");

            outputDir = Path.Combine(resource.GFS0p13RawDataDB, year, month, day, _run, "1hr" , _var);
            if (Directory.Exists(outputDir))
                Directory.Delete(outputDir, true);
            Directory.CreateDirectory(outputDir);

            FileInfo[] inputFiles = new DirectoryInfo(inputDir).GetFiles().Select(fn => new FileInfo(fn.FullName)).OrderBy(f => f.Name).ToArray();

            DateTime represetedTime = new DateTime(Convert.ToInt16(_year), Convert.ToInt16(_month), Convert.ToInt16(_day), Convert.ToInt16(_run), 0, 0);


            for (int i = 0; i < 120; i ++)
            {
                represetedTime = represetedTime.AddHours(1);
                if(i % 6 == 0)
                    File.Copy(inputFiles[i].FullName, Path.Combine(outputDir,
                        "GFS0.13-RUN" + _year + _month + _day + _run + "-1hr-" + _var + "-" + represetedTime.Year.ToString()
                        + twoDigitNumber(represetedTime.Month) + twoDigitNumber(represetedTime.Day) + twoDigitNumber(represetedTime.Hour)) + ".tif");
                else
                    rasterCalculation.substraction(inputFiles[i].FullName, inputFiles[i - 1].FullName, Path.Combine(outputDir,
                        "GFS0.13-RUN" + _year + _month + _day + _run + "-1hr-" + _var + "-" + represetedTime.Year.ToString()
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
                inputDir = resource.GFS0p13TiffDirAPCP;
            else if (_var == "RAIN")
                inputDir = resource.GFS0p13TiffDirRAIN;
            else if (_var == "PRATE")
                inputDir = resource.GFS0p13TiffDirPRate;
            else
                throw new Exception("Error: selected variable does not exist.");

            outputDir = Path.Combine(resource.GFS0p13RawDataDB, year, month, day, _run, "3hr", _var);
            if (Directory.Exists(outputDir))
                Directory.Delete(outputDir, true);
            Directory.CreateDirectory(outputDir);

            FileInfo[] inputFiles = new DirectoryInfo(inputDir).GetFiles().Select(fn => new FileInfo(fn.FullName)).OrderBy(f => f.Name).ToArray();

            DateTime represetedTime = new DateTime(Convert.ToInt16(_year), Convert.ToInt16(_month), Convert.ToInt16(_day), Convert.ToInt16(_run), 0, 0);
            //DEBUG:
            int rn = 1;
            //DEBUG

            bool sub = false;
            for (int i = 2; i <= 120; i += 3)
            {
                represetedTime = represetedTime.AddHours(3);
                if(!sub)
                {
                    Console.WriteLine(rn+"_ Operation: Copy - Copied Raster: " + inputFiles[i].Name + " saved name: " + represetedTime.Year.ToString()
                    + twoDigitNumber(represetedTime.Month) + twoDigitNumber(represetedTime.Day) + twoDigitNumber(represetedTime.Hour)+".tiff");
                    //Console.ReadKey();
                    rn++;
                    File.Copy(inputFiles[i].FullName, Path.Combine(outputDir,
                    "GFS0.13-RUN" + _year + _month + _day + _run + "-3hr-" + _var + "-" + represetedTime.Year.ToString()
                    + twoDigitNumber(represetedTime.Month) + twoDigitNumber(represetedTime.Day) + twoDigitNumber(represetedTime.Hour)) + ".tif");
                    sub = !sub;
                }
                else
                {
                    Console.WriteLine(rn+"_ Operasion: Subtract - Subtracted Rasters: \n 1- " + inputFiles[i].Name + "\n 2- " + inputFiles[i-3].Name + "\n saved name: " + represetedTime.Year.ToString()
                        + twoDigitNumber(represetedTime.Month) + twoDigitNumber(represetedTime.Day) + twoDigitNumber(represetedTime.Hour) + ".tif" );
                    //Console.ReadKey();
                    rn++;
                    rasterCalculation.substraction(inputFiles[i].FullName, inputFiles[i - 3].FullName, Path.Combine(outputDir,
                        "GFS0.13-RUN" + _year + _month + _day + _run + "-3hr-" + _var + "-" + represetedTime.Year.ToString()
                        + twoDigitNumber(represetedTime.Month) + twoDigitNumber(represetedTime.Day) + twoDigitNumber(represetedTime.Hour)) + ".tif");
                    sub = !sub;
                }
            }
            for(int i = 0; i < 40; i ++)
            {
                represetedTime = represetedTime.AddHours(3);
                if (sub)
                {
                    Console.WriteLine(rn + "_ Operasion: Subtract - Subtracted Rasters: \n 1- " + inputFiles[i+120].Name + "\n 2- " + inputFiles[i +120 -1].Name + "\n saved name: " + represetedTime.Year.ToString()
                        + twoDigitNumber(represetedTime.Month) + twoDigitNumber(represetedTime.Day) + twoDigitNumber(represetedTime.Hour) + ".tif");
                    //Console.ReadKey();
                    rn++;
                    rasterCalculation.substraction(inputFiles[i + 120].FullName, inputFiles[i + 120 - 1].FullName, Path.Combine(outputDir,
                        "GFS0.13-RUN" + _year + _month + _day + _run + "-3hr-" + _var + "-" + represetedTime.Year.ToString()
                        + twoDigitNumber(represetedTime.Month) + twoDigitNumber(represetedTime.Day) + twoDigitNumber(represetedTime.Hour)) + ".tif");
                    sub = !sub;
                }
                else
                {
                    Console.WriteLine(rn + "_ Operation: Copy - Copied Raster: " + inputFiles[i+120].Name + " saved name: " + represetedTime.Year.ToString()
                        + twoDigitNumber(represetedTime.Month) + twoDigitNumber(represetedTime.Day) + twoDigitNumber(represetedTime.Hour) + ".tif");
                    //Console.ReadKey();
                    rn++;
                    File.Copy(inputFiles[i + 120].FullName, Path.Combine(outputDir,
                        "GFS0.13-RUN" + _year + _month + _day + _run + "-3hr-" + _var + "-" + represetedTime.Year.ToString()
                        + twoDigitNumber(represetedTime.Month) + twoDigitNumber(represetedTime.Day) + twoDigitNumber(represetedTime.Hour)) + ".tif");
                    sub = !sub;
                }
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
            FileInfo[] inputFiles = new DirectoryInfo(resource.GFS0p13TiffDirTEMP).GetFiles().Select(fn => new FileInfo(fn.FullName)).OrderBy(f => f.Name).ToArray();
            DateTime represetedTime = new DateTime(Convert.ToInt16(year), Convert.ToInt16(month), Convert.ToInt16(day), Convert.ToInt16(_run), 0, 0);
            if (Directory.Exists(Path.Combine(resource.GFS0p13RawDataDB, year, month, day, _run, "3hr", "TEMP")))
                Directory.Delete(Path.Combine(resource.GFS0p13RawDataDB, year, month, day, _run, "3hr", "TEMP"), true);
            Directory.CreateDirectory(Path.Combine(resource.GFS0p13RawDataDB, year, month, day, _run, "3hr", "TEMP"));

            for (int i = 2; i < 123; i += 3)
            {
                represetedTime = represetedTime.AddHours(3);
                List<string> inputs = new List<string>();
                for (int j = 0; j < 3; j++)
                    inputs.Add(inputFiles[i - j].FullName);
                rasterCalculation.average(inputs, Path.Combine(resource.GFS0p13RawDataDB, year, month, day, _run, "3hr", "TEMP",
                    "GFS0.13-RUN" + year + month + day + _run + "-3hr-TEMP-" + represetedTime.Year.ToString()
                        + twoDigitNumber(represetedTime.Month) + twoDigitNumber(represetedTime.Day) + twoDigitNumber(represetedTime.Hour)) + ".tif");
                inputs.Clear();
            }
            for (int i = 1; i < 40; i++)
            {
                represetedTime = represetedTime.AddHours(3);
                File.Copy(inputFiles[i + 120].FullName, Path.Combine(resource.GFS0p13RawDataDB, year, month, day, _run, "3hr", "TEMP",
                    "GFS0.13-RUN" + year + month + day + _run + "-3hr-TEMP-" + represetedTime.Year.ToString()
                    + twoDigitNumber(represetedTime.Month) + twoDigitNumber(represetedTime.Day) + twoDigitNumber(represetedTime.Hour)) + ".tif");
            }
            if (Directory.Exists(Path.Combine(resource.GFS0p13RawDataDB, year, month, day, _run, "1hr", "TEMP")))
                Directory.Delete(Path.Combine(resource.GFS0p13RawDataDB, year, month, day, _run, "1hr", "TEMP"), true);
            Directory.CreateDirectory(Path.Combine(resource.GFS0p13RawDataDB, year, month, day, _run, "1hr", "TEMP"));
            represetedTime = new DateTime(Convert.ToInt16(year), Convert.ToInt16(month), Convert.ToInt16(day), Convert.ToInt16(_run), 0, 0);
            for (int i = 0; i < 120; i++)
            {
                represetedTime = represetedTime.AddHours(1);
                File.Copy(inputFiles[i].FullName, Path.Combine(resource.GFS0p13RawDataDB, year, month, day, _run, "1hr", "TEMP",
                        "GFS0.13-RUN" + year + month + day + _run + "-1hr-TEMP-" + represetedTime.Year.ToString()
                        + twoDigitNumber(represetedTime.Month) + twoDigitNumber(represetedTime.Day) + twoDigitNumber(represetedTime.Hour)) + ".tif");
            }
            return true;
        }
        public static bool snowSoildMoisture2RawDB(string _date, string _run)
        {
            string year = _date.Substring(0, 4);
            string month = _date.Substring(4, 2);
            string day = _date.Substring(6, 2);

            FileInfo[] files = new DirectoryInfo(resource.GFS0p13TiffDirSNOW).GetFiles().Select(fn => new FileInfo(fn.FullName)).OrderBy(f => f.Name).ToArray();
            if (Directory.Exists(Path.Combine(resource.GFS0p13RawDataDB, year, month, day, _run, "3hr", "SNOW")))
                Directory.Delete(Path.Combine(resource.GFS0p13RawDataDB, year, month, day, _run, "3hr", "SNOW"), true);
            Directory.CreateDirectory(Path.Combine(resource.GFS0p13RawDataDB, year, month, day, _run, "3hr", "SNOW"));

            foreach (var f in files)
                File.Copy(f.FullName, Path.Combine(resource.GFS0p13RawDataDB, year, month, day, _run, "3hr", "SNOW", f.Name));
            if (Directory.Exists(Path.Combine(resource.GFS0p13RawDataDB, year, month, day, _run, "3hr", "SOIL")))
                Directory.Delete(Path.Combine(resource.GFS0p13RawDataDB, year, month, day, _run, "3hr", "SOIL"), true);
            Directory.CreateDirectory(Path.Combine(resource.GFS0p13RawDataDB, year, month, day, _run, "3hr", "SOIL"));

            files = new DirectoryInfo(resource.GFS0p13TiffDirSOILW).GetFiles().Select(fn => new FileInfo(fn.FullName)).OrderBy(f => f.Name).ToArray();

            foreach (var f in files)
                File.Copy(f.FullName, Path.Combine(resource.GFS0p13RawDataDB, year, month, day, _run, "3hr", "SOIL", f.Name));


            return true;
        }
        public static bool rain24hr(string _date, string _run)
        {
            string year = _date.Substring(0, 4);
            string month = _date.Substring(4, 2);
            string day = _date.Substring(6, 2);
            if (Directory.Exists(Path.Combine(resource.GFS0p13RawDataDB, year, month, day, _run, "3hr", "RAIN-24hr")))
                Directory.Delete(Path.Combine(resource.GFS0p13RawDataDB, year, month, day, _run, "3hr", "RAIN-24hr"),true);
            Directory.CreateDirectory(Path.Combine(resource.GFS0p13RawDataDB, year, month, day, _run, "3hr", "RAIN-24hr"));
            FileInfo[] files = new DirectoryInfo(Path.Combine(resource.GFS0p13RawDataDB, year, month, day, _run, "3hr", "RAIN")).GetFiles().Select(fn => new FileInfo(fn.FullName)).OrderBy(f => f.Name).ToArray();
            for (int i = 0; i <= (files.Length / 8) - 1; i ++)
            {
                List<string> input = new List<string>();
                string outputName = "";
                for (int j = i * 8; j < 8 + (i * 8); j++)
                    input.Add(files[j].FullName);
                outputName = Path.Combine(resource.GFS0p13RawDataDB, year, month, day, _run, "3hr" ,"RAIN-24hr", "GFS0.13-" 
                    + year + month + day + _run + "-RAIN-Sum24hr-" + Convert.ToString(i + 1) + "-" 
                    + files[i * 8].Name.Substring(files[i * 8].Name.Length - 14, 10) + "~" 
                    + files[i * 8 + 7].Name.Substring(files[i * 8 + 7].Name.Length - 14, 10) + ".tif");
                rasterCalculation.sum(input, outputName);
            }
            return true;
        }

        public static bool APCP24hr(string _date, string _run)
        {
            string year = _date.Substring(0, 4);
            string month = _date.Substring(4, 2);
            string day = _date.Substring(6, 2);
            if (Directory.Exists(Path.Combine(resource.GFS0p13RawDataDB, year, month, day, _run, "3hr", "APCP-24hr")))
                Directory.Delete(Path.Combine(resource.GFS0p13RawDataDB, year, month, day, _run, "3hr", "APCP-24hr"), true);
            Directory.CreateDirectory(Path.Combine(resource.GFS0p13RawDataDB, year, month, day, _run, "3hr", "APCP-24hr"));
            FileInfo[] files = new DirectoryInfo(Path.Combine(resource.GFS0p13RawDataDB, year, month, day, _run, "3hr", "APCP")).GetFiles().Select(fn => new FileInfo(fn.FullName)).OrderBy(f => f.Name).ToArray();
            for (int i = 0; i <= (files.Length / 8) - 1; i++)
            {
                List<string> input = new List<string>();
                string outputName = "";
                for (int j = i * 8; j < 8 + (i * 8); j++)
                    input.Add(files[j].FullName);
                outputName = Path.Combine(resource.GFS0p13RawDataDB, year, month, day, _run, "3hr", "APCP-24hr", "GFS0.13-"
                    + year + month + day + _run + "-APCP-Sum24hr-" + Convert.ToString(i + 1) + "-"
                    + files[i * 8].Name.Substring(files[i * 8].Name.Length - 14, 10) + "~"
                    + files[i * 8 + 7].Name.Substring(files[i * 8 + 7].Name.Length - 14, 10) + ".tif");
                rasterCalculation.sum(input, outputName);
            }
            return true;
        }

        public static bool rain12hr(string _date, string _run)
        {
            string year = _date.Substring(0, 4);
            string month = _date.Substring(4, 2);
            string day = _date.Substring(6, 2);
            if (Directory.Exists(Path.Combine(resource.GFS0p13RawDataDB, year, month, day, _run, "3hr", "RAIN-12hr")))
                Directory.Delete(Path.Combine(resource.GFS0p13RawDataDB, year, month, day, _run, "3hr", "RAIN-12hr"), true);
            Directory.CreateDirectory(Path.Combine(resource.GFS0p13RawDataDB, year, month, day, _run,"3hr", "RAIN-12hr"));
            FileInfo[] files = new DirectoryInfo(Path.Combine(resource.GFS0p13RawDataDB, year, month, day, _run,"3hr", "RAIN")).GetFiles().Select(fn => new FileInfo(fn.FullName)).OrderBy(f => f.Name).ToArray();
            for (int i = 0; i <= (files.Length / 4) - 1; i ++)
            {
                List<string> input = new List<string>();
                string outputName = "";
                for (int j = i * 4; j < 4 + (i * 4); j++)
                    input.Add(files[j].FullName);
                outputName = Path.Combine(resource.GFS0p13RawDataDB, year, month, day, _run, "3hr", "RAIN-12hr", "GFS0.13-" 
                    + year + month + day + _run + "-RAIN-Sum12hr-" + Convert.ToString(i + 1) + "-"
                    + files[i * 4].Name.Substring(files[i * 4].Name.Length - 14, 10) + "~"
                    + files[i * 4 + 3].Name.Substring(files[i * 4 + 3].Name.Length - 14, 10) + ".tif");
                rasterCalculation.sum(input, outputName);
            }
            return true;
        }
        public static bool APCP12hr(string _date, string _run)
        {
            string year = _date.Substring(0, 4);
            string month = _date.Substring(4, 2);
            string day = _date.Substring(6, 2);
            if (Directory.Exists(Path.Combine(resource.GFS0p13RawDataDB, year, month, day, _run, "3hr", "APCP-12hr")))
                Directory.Delete(Path.Combine(resource.GFS0p13RawDataDB, year, month, day, _run, "3hr", "APCP-12hr"), true);
            Directory.CreateDirectory(Path.Combine(resource.GFS0p13RawDataDB, year, month, day, _run, "3hr", "APCP-12hr"));
            FileInfo[] files = new DirectoryInfo(Path.Combine(resource.GFS0p13RawDataDB, year, month, day, _run, "3hr", "APCP")).GetFiles().Select(fn => new FileInfo(fn.FullName)).OrderBy(f => f.Name).ToArray();
            for (int i = 0; i <= (files.Length / 4) - 1; i++)
            {
                List<string> input = new List<string>();
                string outputName = "";
                for (int j = i * 4; j < 4 + (i * 4); j++)
                    input.Add(files[j].FullName);
                outputName = Path.Combine(resource.GFS0p13RawDataDB, year, month, day, _run, "3hr", "APCP-12hr", "GFS0.13-"
                    + year + month + day + _run + "-APCP-Sum12hr-" + Convert.ToString(i + 1) + "-"
                    + files[i * 4].Name.Substring(files[i * 4].Name.Length - 14, 10) + "~"
                    + files[i * 4 + 3].Name.Substring(files[i * 4 + 3].Name.Length - 14, 10) + ".tif");
                rasterCalculation.sum(input, outputName);
            }
            return true;
        }

        public static DateTime extractDateFromFileName(FileInfo input)
        {
            string name = input.Name;
            name = name.Substring(name.Length - 14, 10);
            DateTime result = new DateTime(Convert.ToInt16(name.Substring(0, 4)),
                                            Convert.ToInt16(name.Substring(4, 2)),
                                            Convert.ToInt16(name.Substring(6, 2)));
            return result;
        }
        public static bool daily(string _Date, string _run, string variable = "RAIN")
        {
            DateTime currnetDate = new DateTime(Convert.ToInt16(_Date.Substring(0, 4))
                                                , Convert.ToInt16(_Date.Substring(4, 2))
                                                , Convert.ToInt16(_Date.Substring(6, 2))
                                                , 0, 0, 0);
            currnetDate = currnetDate.AddDays(1);
            FileInfo[] inputFiles = Directory.GetFiles(
                Path.Combine(resource.GFS0p13RawDataDB,currnetDate.AddDays(-1).Year.ToString()
                ,twoDigitNumber(currnetDate.AddDays(-1).Month)
                ,twoDigitNumber(currnetDate.AddDays(-1).Day), _run, "3hr", variable) , "*.tif").Select(fn => new FileInfo(fn)).OrderBy(f => f.Name).ToArray();
            int index = 0;
            if (Directory.Exists(Path.Combine(resource.GFS0p13RawDataDB, currnetDate.AddDays(-1).Year.ToString()
                , twoDigitNumber(currnetDate.AddDays(-1).Month)
                , twoDigitNumber(currnetDate.AddDays(-1).Day), _run,"3hr", "Daily-"+variable)))
                Directory.Delete(Path.Combine(resource.GFS0p13RawDataDB, currnetDate.AddDays(-1).Year.ToString()
                , twoDigitNumber(currnetDate.AddDays(-1).Month)
                , twoDigitNumber(currnetDate.AddDays(-1).Day), _run,"3hr", "Daily-"+variable), true);

            Directory.CreateDirectory(Path.Combine(resource.GFS0p13RawDataDB, currnetDate.AddDays(-1).Year.ToString()
                , twoDigitNumber(currnetDate.AddDays(-1).Month)
                , twoDigitNumber(currnetDate.AddDays(-1).Day), _run,"3hr", "Daily-"+variable));

            string outputAddress = Path.Combine(resource.GFS0p13RawDataDB, currnetDate.AddDays(-1).Year.ToString()
                , twoDigitNumber(currnetDate.AddDays(-1).Month)
                , twoDigitNumber(currnetDate.AddDays(-1).Day), _run,"3hr", "Daily-"+variable);

            for (int i = 0; i < inputFiles.Length; i ++)
            {
                if (extractDateFromFileName(inputFiles[i]) == currnetDate)
                {
                    index = i;
                    break;
                }
            }
            for (int i = 0; i < 5; i++)
            {
                List<string> sumFiles = new List<string>();
                for (int j = index; j < index + 8; j++)
                    sumFiles.Add(inputFiles[j].FullName);
                index += 8;
                rasterCalculation.sum(sumFiles, Path.Combine(outputAddress, Convert.ToString(i + 1))+".tif");
            }
            return true;
        }
     
     }
}
