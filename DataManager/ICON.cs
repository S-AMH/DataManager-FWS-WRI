using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataManager
{
    class ICON
    {
        public static bool testUploadICON(string date, string run)
        {
            System.Diagnostics.Process cmd = new System.Diagnostics.Process();
            cmd.StartInfo.FileName = @"cmd.exe";
            cmd.StartInfo.Arguments = @"/C " + "python " + resource.testUploadICON + " " + date + run;
            cmd.Start();
            cmd.WaitForExit();
            return true;
        }
        public static Queue<Tuple<string, string>> getFileNamesDownloaded(string _var)
        {
            Queue<Tuple<string, string>> fileNames = new Queue<Tuple<string, string>>();


            DirectoryInfo dirInfo = new DirectoryInfo(Path.Combine(resource.ICON_Grb2,_var));

            FileInfo[] fileInfo = dirInfo.GetFiles().ToArray();
            Array.Sort(fileInfo, ATG.atgMethods.CompareNatural);

            string outAdress = Path.Combine(resource.ICON_Tiff, _var);

            if (Directory.Exists(outAdress))
                Directory.Delete(outAdress, true);
            Directory.CreateDirectory(outAdress);

                for (int i = 0; i < fileInfo.Length; i++)
                    fileNames.Enqueue(new Tuple<string, string>
                        (fileInfo[i].FullName, 
                        Path.Combine(outAdress, fileInfo[i].Name + ".tif")));

            return fileNames;
        }
        public static void createRainRasters()
        {
            FileInfo[] rain_con = new DirectoryInfo
                (Path.Combine(resource.ICON_Tiff, "RAIN_CON")).GetFiles();

            FileInfo[] rain_gsp = new DirectoryInfo
                (Path.Combine(resource.ICON_Tiff, "RAIN_GSP")).GetFiles();

            Array.Sort(rain_con, ATG.atgMethods.CompareNatural);
            Array.Sort(rain_gsp, ATG.atgMethods.CompareNatural);

            for(int i = 0; i < rain_con.Length; i ++)
            {
                List<string> tmp = new List<string>();
                tmp.Add(rain_con[i].FullName);
                tmp.Add(rain_gsp[i].FullName);
                rasterCalculation.sum(tmp, 
                    Path.Combine(resource.ICON_Tiff, "Rain", 
                    Convert.ToString(i * 3 + 3) + ".tif"));
            }
        }

        public static void convertCummToSingle(string _date, string _run, string _var)
        {
            Console.WriteLine("convertCummToSingle Date: " + _date + " : " + _run);
            string year = _date.Substring(0, 4);
            string month = _date.Substring(4, 2);
            string day = _date.Substring(6, 2);
            DateTime RunDate = new DateTime(Convert.ToInt32(year),
                Convert.ToInt32(month), Convert.ToInt32(day), 0, 0, 0);
            RunDate = RunDate.AddHours(Convert.ToInt32(_run));

            string inputFiles = "";
            string outputDir = 
                Path.Combine(resource.ICON_RawDB, 
                RunDate.Year.ToString(), 
                ATG.atgMethods.xDigitNum(RunDate.Month, 2),
                ATG.atgMethods.xDigitNum(RunDate.Day, 2), 
                ATG.atgMethods.xDigitNum(RunDate.Hour, 2), "3hr", _var);

            if (Directory.Exists(outputDir))
                Directory.Delete(outputDir, true);
            Directory.CreateDirectory(outputDir);

            if (_var == "APCP")
                inputFiles = Path.Combine(resource.ICON_Tiff, "TOT_PREC");
            if (_var == "RAIN")
                inputFiles = Path.Combine(resource.ICON_Tiff, "Rain");

            FileInfo[] inputs = new DirectoryInfo(inputFiles).GetFiles().ToArray();
            Array.Sort(inputs, ATG.atgMethods.CompareNatural);

            DateTime currentDate = RunDate.AddHours(3);
            File.Copy(inputs[0].FullName, Path.Combine(outputDir, "ICON0.125-RUN" + RunDate.Year.ToString() +
            ATG.atgMethods.xDigitNum(RunDate.Month, 2) +
            ATG.atgMethods.xDigitNum(RunDate.Day, 2) +
            ATG.atgMethods.xDigitNum(RunDate.Hour, 2) + "-" +
            _var.ToUpper() + "-" + currentDate.Year.ToString() +
            ATG.atgMethods.xDigitNum(currentDate.Month, 2) +
            ATG.atgMethods.xDigitNum(currentDate.Day, 2) +
            ATG.atgMethods.xDigitNum(currentDate.Hour, 2)+".tif"));
                
            for(int i = 1; i < inputs.Length; i ++)
            {
                currentDate = currentDate.AddHours(3);
                rasterCalculation.substraction(inputs[i].FullName, inputs[i-1].FullName,
                    Path.Combine(outputDir, "ICON0.125-RUN" + RunDate.Year.ToString() +
                    ATG.atgMethods.xDigitNum(RunDate.Month, 2) +
                    ATG.atgMethods.xDigitNum(RunDate.Day, 2) +
                    ATG.atgMethods.xDigitNum(RunDate.Hour, 2) + "-" +
                    _var.ToUpper() + "-" + currentDate.Year.ToString() +
                    ATG.atgMethods.xDigitNum(currentDate.Month, 2) +
                    ATG.atgMethods.xDigitNum(currentDate.Day, 2) +
                    ATG.atgMethods.xDigitNum(currentDate.Hour, 2)+".tif"));
            }

        }

        public static void cummRain(string _date, string _run)
        {
            string year = _date.Substring(0, 4);
            string month = _date.Substring(4, 2);
            string day = _date.Substring(6, 2);
            DateTime RunDate = new DateTime(Convert.ToInt32(year),
                Convert.ToInt32(month), Convert.ToInt32(day), 0, 0, 0);
            RunDate = RunDate.AddHours(Convert.ToInt32(_run));

            FileInfo[] inputs = new DirectoryInfo(Path.Combine(resource.ICON_Tiff, "Rain")).GetFiles("*.tif").ToArray();
            Array.Sort(inputs, ATG.atgMethods.CompareNatural);

            string outputDir =
               Path.Combine(resource.ICON_RawDB,
               RunDate.Year.ToString(),
               ATG.atgMethods.xDigitNum(RunDate.Month, 2),
               ATG.atgMethods.xDigitNum(RunDate.Day, 2),
               ATG.atgMethods.xDigitNum(RunDate.Hour, 2), "3hr", "CumulicativeRaster3hr");

            if (Directory.Exists(outputDir))
                Directory.Delete(outputDir, true);
            Directory.CreateDirectory(outputDir);

            for (int i = 0; i < inputs.Length; i ++)
            {
                File.Copy(inputs[i].FullName, Path.Combine(outputDir,
                    "CumulicativeRaster-RAIN-" + Convert.ToString(i + 1)
                    + "-" + RunDate.Year.ToString() +
                    ATG.atgMethods.xDigitNum(RunDate.Month, 2) +
                    ATG.atgMethods.xDigitNum(RunDate.Day, 2) +
                    ATG.atgMethods.xDigitNum(RunDate.Hour, 2) + ".tif"));
                RunDate = RunDate.AddHours(3);
            }


        }
        public static void cummAPCP(string _date, string _run)
        {
            string year = _date.Substring(0, 4);
            string month = _date.Substring(4, 2);
            string day = _date.Substring(6, 2);
            DateTime RunDate = new DateTime(Convert.ToInt32(year),
                Convert.ToInt32(month), Convert.ToInt32(day), 0, 0, 0);
            RunDate = RunDate.AddHours(Convert.ToInt32(_run));

            FileInfo[] inputs = new DirectoryInfo(Path.Combine(resource.ICON_Tiff, "TOT_PREC")).GetFiles("*.tif").ToArray();
            Array.Sort(inputs, ATG.atgMethods.CompareNatural);

            string outputDir =
               Path.Combine(resource.ICON_RawDB,
               RunDate.Year.ToString(),
               ATG.atgMethods.xDigitNum(RunDate.Month, 2),
               ATG.atgMethods.xDigitNum(RunDate.Day, 2),
               ATG.atgMethods.xDigitNum(RunDate.Hour, 2), "3hr", "CumulicativeRaster3hr-APCP");

            if (Directory.Exists(outputDir))
                Directory.Delete(outputDir, true);
            Directory.CreateDirectory(outputDir);

            for (int i = 0; i < inputs.Length; i++)
            {
                File.Copy(inputs[i].FullName, Path.Combine(outputDir,
                    "CumulicativeRaster-APCP-" + Convert.ToString(i + 1)
                    + "-" + RunDate.Year.ToString() +
                    ATG.atgMethods.xDigitNum(RunDate.Month, 2) +
                    ATG.atgMethods.xDigitNum(RunDate.Day, 2) +
                    ATG.atgMethods.xDigitNum(RunDate.Hour, 2) + ".tif"));
                RunDate = RunDate.AddHours(3);
            }
        }

        public static DateTime extractDateFromFileName(FileInfo input)
        {
            string name = input.Name;
            Console.WriteLine("extractDateFromFileName: FileName Length: " + name.Length);
            name = name.Substring(name.Length - 14, 10);
            Console.WriteLine("name: " + name);
            DateTime result = new DateTime(Convert.ToInt16(name.Substring(0, 4)),
                                            Convert.ToInt16(name.Substring(4, 2)),
                                            Convert.ToInt16(name.Substring(6, 2)));

            Console.WriteLine("extractDateFromFileName: output : " + name + "DateTime: " + result.ToLongDateString());

            return result;
        }

        public static void dailyRain(string _date, string _run)
        {
            {
                DateTime currnetDate = new DateTime(Convert.ToInt16(_date.Substring(0, 4))
                                                    , Convert.ToInt16(_date.Substring(4, 2))
                                                    , Convert.ToInt16(_date.Substring(6, 2))
                                                    , 0, 0, 0);
                currnetDate = currnetDate.AddDays(1);
                FileInfo[] inputFiles = Directory.GetFiles(
                    Path.Combine(resource.ICON_RawDB, currnetDate.AddDays(-1).Year.ToString()
                    , ATG.atgMethods.xDigitNum(currnetDate.AddDays(-1).Month,2)
                    , ATG.atgMethods.xDigitNum(currnetDate.AddDays(-1).Day,2), _run, "3hr", "Rain"), "*.tif").Select(fn => new FileInfo(fn)).OrderBy(f => f.Name).ToArray();
                Array.Sort(inputFiles, ATG.atgMethods.CompareNatural);

                int index = 0;
                if (Directory.Exists(Path.Combine(resource.ICON_RawDB, currnetDate.AddDays(-1).Year.ToString()
                    , ATG.atgMethods.xDigitNum(currnetDate.AddDays(-1).Month,2)
                    , ATG.atgMethods.xDigitNum(currnetDate.AddDays(-1).Day,2), _run, "3hr", "Daily-" + "Rain")))
                    Directory.Delete(Path.Combine(resource.ICON_RawDB, currnetDate.AddDays(-1).Year.ToString()
                    , ATG.atgMethods.xDigitNum(currnetDate.AddDays(-1).Month,2)
                    , ATG.atgMethods.xDigitNum(currnetDate.AddDays(-1).Day,2), _run, "3hr", "Daily-" + "Rain"), true);

                Directory.CreateDirectory(Path.Combine(resource.ICON_RawDB, currnetDate.AddDays(-1).Year.ToString()
                    , ATG.atgMethods.xDigitNum(currnetDate.AddDays(-1).Month,2)
                    , ATG.atgMethods.xDigitNum(currnetDate.AddDays(-1).Day,2), _run, "3hr", "Daily-" + "Rain"));

                string outputAddress = Path.Combine(resource.ICON_RawDB, currnetDate.AddDays(-1).Year.ToString()
                    , ATG.atgMethods.xDigitNum(currnetDate.AddDays(-1).Month,2)
                    , ATG.atgMethods.xDigitNum(currnetDate.AddDays(-1).Day,2), _run, "3hr", "Daily-" + "Rain");

                for (int i = 0; i < inputFiles.Length; i++)
                {
                    if (extractDateFromFileName(inputFiles[i]) == currnetDate)
                    {
                        index = i;
                        break;
                    }
                }
                for (int i = 0; i < 4; i++)
                {
                    List<string> sumFiles = new List<string>();
                    for (int j = index; j < index + 8; j++)
                        sumFiles.Add(inputFiles[j].FullName);
                    index += 8;
                    rasterCalculation.sum(sumFiles, Path.Combine(outputAddress, Convert.ToString(i + 1)) + ".tif");
                }
            }
        }
        public static void dailyAPCP(string _date, string _run)
        {
            {
                Console.WriteLine("dailyAPCP " + _date);
                DateTime currnetDate = new DateTime(Convert.ToInt16(_date.Substring(0, 4))
                                                    , Convert.ToInt16(_date.Substring(4, 2))
                                                    , Convert.ToInt16(_date.Substring(6, 2))
                                                    , 0, 0, 0);
                Console.WriteLine("dailyAPCP: " + currnetDate.ToLongDateString());
                currnetDate = currnetDate.AddDays(1);
                FileInfo[] inputFiles = Directory.GetFiles(
                    Path.Combine(resource.ICON_RawDB, currnetDate.AddDays(-1).Year.ToString()
                    , ATG.atgMethods.xDigitNum(currnetDate.AddDays(-1).Month, 2)
                    , ATG.atgMethods.xDigitNum(currnetDate.AddDays(-1).Day, 2), _run, "3hr", "APCP"), "*.tif").Select(fn => new FileInfo(fn)).OrderBy(f => f.Name).ToArray();
                Array.Sort(inputFiles, ATG.atgMethods.CompareNatural);
                Console.WriteLine(Path.Combine(resource.ICON_RawDB, currnetDate.AddDays(-1).Year.ToString()
                    , ATG.atgMethods.xDigitNum(currnetDate.AddDays(-1).Month, 2)
                    , ATG.atgMethods.xDigitNum(currnetDate.AddDays(-1).Day, 2), _run, "3hr", "APCP"));

                int index = 0;
                if (Directory.Exists(Path.Combine(resource.ICON_RawDB, currnetDate.AddDays(-1).Year.ToString()
                    , ATG.atgMethods.xDigitNum(currnetDate.AddDays(-1).Month, 2)
                    , ATG.atgMethods.xDigitNum(currnetDate.AddDays(-1).Day, 2), _run, "3hr", "Daily-" + "APCP")))
                    Directory.Delete(Path.Combine(resource.ICON_RawDB, currnetDate.AddDays(-1).Year.ToString()
                    , ATG.atgMethods.xDigitNum(currnetDate.AddDays(-1).Month, 2)
                    , ATG.atgMethods.xDigitNum(currnetDate.AddDays(-1).Day, 2), _run, "3hr", "Daily-" + "APCP"), true);

                Directory.CreateDirectory(Path.Combine(resource.ICON_RawDB, currnetDate.AddDays(-1).Year.ToString()
                    , ATG.atgMethods.xDigitNum(currnetDate.AddDays(-1).Month, 2)
                    , ATG.atgMethods.xDigitNum(currnetDate.AddDays(-1).Day, 2), _run, "3hr", "Daily-" + "APCP"));

                string outputAddress = Path.Combine(resource.ICON_RawDB, currnetDate.AddDays(-1).Year.ToString()
                    , ATG.atgMethods.xDigitNum(currnetDate.AddDays(-1).Month, 2)
                    , ATG.atgMethods.xDigitNum(currnetDate.AddDays(-1).Day, 2), _run, "3hr", "Daily-" + "APCP");
                Console.WriteLine("DailyAPCP: length input: " + inputFiles.Length);
                for (int i = 0; i < inputFiles.Length; i++)
                {
                    Console.WriteLine(i);
                    if (extractDateFromFileName(inputFiles[i]) == currnetDate)
                    {
                        index = i;
                        break;
                    }
                }
                for (int i = 0; i < 4; i++)
                {
                    List<string> sumFiles = new List<string>();
                    for (int j = index; j < index + 8; j++)
                    {
                        Console.WriteLine("DailyAPCP: fileNames: " + inputFiles[j].FullName);
                        sumFiles.Add(inputFiles[j].FullName);
                    }
                    index += 8;
                    rasterCalculation.sum(sumFiles, Path.Combine(outputAddress, Convert.ToString(i + 1)) + ".tif");
                }
            }
        }
    }
}
