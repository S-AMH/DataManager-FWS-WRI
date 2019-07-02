using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataManager
{
    class WRF0p11
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
            if (Directory.Exists(Path.Combine(resource.wrfRawDB, "ensemble",
                    year, month, day, _run, "3hr", "CumulicativeRaster3hr")))
                Directory.Delete(Path.Combine(resource.wrfRawDB, "ensemble",
                    year, month, day, _run, "3hr", "CumulicativeRaster3hr"), true);
            Directory.CreateDirectory(Path.Combine(resource.wrfRawDB, "ensemble",
                    year, month, day, _run, "3hr", "CumulicativeRaster3hr"));
            FileInfo[] inputs = new DirectoryInfo(Path.Combine(resource.wrfRawDB,"ensemble", year, month, day, _run,"3hr", "RAIN")).GetFiles("*.tif").ToArray();
            Array.Sort(inputs, ATG.atgMethods.CompareNatural);
            List<string> sumInputs = new List<string>();
            for (int i = 1; i < inputs.Count()+1; i++)
            {
                for (int j = 0; j < i; j++)
                    sumInputs.Add(inputs[j].FullName);
                rasterCalculation.sum(sumInputs, Path.Combine(resource.wrfRawDB, "ensemble",
                    year, month, day, _run,"3hr", "CumulicativeRaster3hr", "CumulicativeRaster-RAIN-" + i + "-" +
                    new FileInfo(sumInputs.Last()).Name.Substring(new FileInfo(sumInputs.Last()).Name.Length - 14, 10) + ".tif"));
                sumInputs.Clear();
            }
            return true;
        }
        static public bool commulicativeAPCP(string _date, string _run)
        {
            string year = _date.Substring(0, 4);
            string month = _date.Substring(4, 2);
            string day = _date.Substring(6, 2);
            if (Directory.Exists(Path.Combine(resource.wrfRawDB, "ensemble",
                    year, month, day, _run, "3hr", "CumulicativeRaster3hrAPCP")))
                Directory.Delete(Path.Combine(resource.wrfRawDB, "ensemble",
                    year, month, day, _run, "3hr", "CumulicativeRaster3hrAPCP"), true);
            Directory.CreateDirectory(Path.Combine(resource.wrfRawDB, "ensemble",
                    year, month, day, _run, "3hr", "CumulicativeRaster3hrAPCP"));
            FileInfo[] inputs = new DirectoryInfo(Path.Combine(resource.wrfRawDB, "ensemble", year, month, day, _run, "3hr", "APCP")).GetFiles("*.tif").Select(fn => new FileInfo(fn.FullName)).OrderBy(f => f.Name).ToArray();
            Array.Sort(inputs, ATG.atgMethods.CompareNatural);
            List<string> sumInputs = new List<string>();
            for (int i = 1; i < inputs.Count() + 1; i++)
            {
                for (int j = 0; j < i; j++)
                    sumInputs.Add(inputs[j].FullName);
                rasterCalculation.sum(sumInputs, Path.Combine(resource.wrfRawDB, "ensemble",
                    year, month, day, _run, "3hr", "CumulicativeRaster3hrAPCP", "CumulicativeRaster-APCP-" + i + "-" +
                    new FileInfo(sumInputs.Last()).Name.Substring(new FileInfo(sumInputs.Last()).Name.Length - 14, 10) + ".tif"));
                sumInputs.Clear();
            }
            return true;
        }
        public static bool convertAPCP2RAIN(string _date, string _run)
        {
            string year = _date.Substring(0, 4);
            string month = _date.Substring(4, 2);
            string day = _date.Substring(6, 2);
            List<string> wrfModelsAPCP = new List<string>();
            List<string> wrfModelsTEMP = new List<string>();
            List<string> wrfModelsRAIN = new List<string>();
            wrfModelsAPCP.Add(resource.wrfTiffDirEnsembleAPCP);
            wrfModelsAPCP.Add(resource.wrfTiffDirFerrierAPCP);
            wrfModelsAPCP.Add(resource.wrfTiffDirLinAPCP);
            wrfModelsAPCP.Add(resource.wrfTiffDirWsm6APCP);

            wrfModelsTEMP.Add(resource.wrfTiffDirEnsembleTEMP);
            wrfModelsTEMP.Add(resource.wrfTiffDirFerrierTEMP);
            wrfModelsTEMP.Add(resource.wrfTiffDirLinTEMP);
            wrfModelsTEMP.Add(resource.wrfTiffDirWsm6TEMP);

            for (int j = 0; j < 4; j++)
            {
                FileInfo[] tempFiles = new DirectoryInfo(wrfModelsTEMP[j]).GetFiles("*.tif").Select(fn => new FileInfo(fn.FullName)).OrderBy(f => f.Name).ToArray();
                Array.Sort(tempFiles, ATG.atgMethods.CompareNatural);
                FileInfo[] apcpFiles = new DirectoryInfo(wrfModelsAPCP[j]).GetFiles("*.tif").Select(fn => new FileInfo(fn.FullName)).OrderBy(f => f.Name).ToArray();
                Array.Sort(apcpFiles, ATG.atgMethods.CompareNatural);
                var splitedName = tempFiles[0].Name.Split('-');
                var modelName = splitedName[0];
                if (Directory.Exists((Path.Combine(resource.wrfRawDB, modelName, year, month, day, _run, "3hr", "RAIN"))))
                    Directory.Delete((Path.Combine(resource.wrfRawDB, modelName, year, month, day, _run, "3hr", "RAIN")), true);
                Directory.CreateDirectory(Path.Combine(resource.wrfRawDB, modelName, year, month, day, _run, "3hr", "RAIN"));
                Queue<Tuple<string, string, string>> inputs = new Queue<Tuple<string, string, string>>();
                for (int i = 0; i < tempFiles.Length; i++)
                    inputs.Enqueue(new Tuple<string, string, string>(tempFiles[i].FullName, apcpFiles[i].FullName, apcpFiles[i].Name.Replace("APCP","RAIN")));

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
                                rasterCalculation.convertAPCP2RAIN(f.Item2, f.Item1, Path.Combine(
                                    resource.wrfRawDB, modelName, year, month, day, _run, "3hr", "RAIN", f.Item3));
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
            }
            return true;
        }
        public static bool temp2RawDB(string _date, string _run)
        {
            string year = _date.Substring(0, 4);
            string month = _date.Substring(4, 2);
            string day = _date.Substring(6, 2);

            Console.WriteLine(year + "\n" + month + "\n" + day);

            List<string> wrfModelsTEMP = new List<string>();

            wrfModelsTEMP.Add(resource.wrfTiffDirEnsembleTEMP);
            wrfModelsTEMP.Add(resource.wrfTiffDirFerrierTEMP);
            wrfModelsTEMP.Add(resource.wrfTiffDirLinTEMP);
            wrfModelsTEMP.Add(resource.wrfTiffDirWsm6TEMP);
            for(int i = 0; i < 4; i ++)
            {
                FileInfo[] inputFiles = new DirectoryInfo(wrfModelsTEMP[i]).GetFiles("*.tif").Select(fn => new FileInfo(fn.FullName)).OrderBy(f => f.Name).ToArray();
                Array.Sort(inputFiles, ATG.atgMethods.CompareNatural);
                var splitedName = inputFiles[0].Name.Split('-');
                var modelName = splitedName[0];
                DateTime represetedTime = new DateTime(Convert.ToInt16(year), Convert.ToInt16(month), Convert.ToInt16(day), Convert.ToInt16(_run), 0, 0);
                if (Directory.Exists(Path.Combine(resource.wrfRawDB, modelName, year, month, day, _run, "3hr", "TEMP")))
                    Directory.Delete(Path.Combine(resource.wrfRawDB, modelName, year, month, day, _run, "3hr", "TEMP"), true);
                Directory.CreateDirectory(Path.Combine(resource.wrfRawDB,modelName, year, month, day, _run, "3hr", "TEMP"));
                for(int j = 0; j < inputFiles.Length; j++)
                {
                    represetedTime = represetedTime.AddHours(3);
                    rasterCalculation.setNoData(inputFiles.ElementAt(j).FullName,
                        Path.Combine(resource.wrfRawDB, modelName, year, month, day, _run, "3hr", "TEMP", "WRF0.11-RUN" 
                        + year + month + day + _run + "-3hr-TEMP-" + represetedTime.Year.ToString() + twoDigitNumber(represetedTime.Month) 
                        + twoDigitNumber(represetedTime.Day) + twoDigitNumber(represetedTime.Hour) + ".tif"));
                }
            }


            return true;
        }
        public static bool apcp2RawDB(string _date, string _run)
        {
            string year = _date.Substring(0, 4);
            string month = _date.Substring(4, 2);
            string day = _date.Substring(6, 2);

            List<string> wrfModelsAPCP = new List<string>();

            wrfModelsAPCP.Add(resource.wrfTiffDirEnsembleAPCP);
            wrfModelsAPCP.Add(resource.wrfTiffDirFerrierAPCP);
            wrfModelsAPCP.Add(resource.wrfTiffDirLinAPCP);
            wrfModelsAPCP.Add(resource.wrfTiffDirWsm6APCP);

            for (int i = 0; i < 4; i++)
            {
                FileInfo[] inputFiles = Directory.GetFiles(wrfModelsAPCP[i], "*.tif").Select(fn => new FileInfo(fn)).OrderBy(f => f.Name).ToArray();
                Array.Sort(inputFiles, ATG.atgMethods.CompareNatural);
                var splitedName = inputFiles[0].Name.Split('-');
                var modelName = splitedName[0];
                DateTime represetedTime = new DateTime(Convert.ToInt16(year), Convert.ToInt16(month), Convert.ToInt16(day), Convert.ToInt16(_run), 0, 0);
                if (Directory.Exists(Path.Combine(resource.wrfRawDB, modelName, year, month, day, _run, "3hr", "APCP")))
                    Directory.Delete(Path.Combine(resource.wrfRawDB, modelName, year, month, day, _run, "3hr", "APCP"), true);
                Directory.CreateDirectory(Path.Combine(resource.wrfRawDB, modelName, year, month, day, _run, "3hr", "APCP"));
                for (int j = 0; j < inputFiles.Length; j++)
                {
                    represetedTime = represetedTime.AddHours(3);
                    rasterCalculation.setNoData(inputFiles.ElementAt(j).FullName,
                        Path.Combine(resource.wrfRawDB, modelName, year, month, day, _run, "3hr", "APCP", "WRF0.11-RUN"
                        + year + month + day + _run + "-3hr-APCP-" + represetedTime.Year.ToString() + twoDigitNumber(represetedTime.Month)
                        + twoDigitNumber(represetedTime.Day) + twoDigitNumber(represetedTime.Hour) + ".tif"));
                }
            }
            return true;
        }
        public static bool snowSoildMoisture2RawDB(string _date, string _run)
        {
            string year = _date.Substring(0, 4);
            string month = _date.Substring(4, 2);
            string day = _date.Substring(6, 2);

            List<string> wrfModelsSNOW = new List<string>();
            List<string> wrfModelsSOIL = new List<string>();

            wrfModelsSNOW.Add(resource.wrfTiffDirEnsembleSNOW);
            wrfModelsSNOW.Add(resource.wrfTiffDirFerrierSNOW);
            wrfModelsSNOW.Add(resource.wrfTiffDirLinSNOW);
            wrfModelsSNOW.Add(resource.wrfTiffDirWsm6SNOW);

            wrfModelsSOIL.Add(resource.wrfTiffDirEnsembleSOIL);
            wrfModelsSOIL.Add(resource.wrfTiffDirFerrierSOIL);
            wrfModelsSOIL.Add(resource.wrfTiffDirLinSOIL);
            wrfModelsSOIL.Add(resource.wrfTiffDirWsm6SOIL);

            for(int i = 0; i < 4; i ++)
            {
                FileInfo[] files = new DirectoryInfo(wrfModelsSNOW[i]).GetFiles("*.tif").Select(fn => new FileInfo(fn.FullName)).OrderBy(f => f.Name).ToArray();
                Array.Sort(files, ATG.atgMethods.CompareNatural);
                var splitedName = files[0].Name.Split('-');
                var modelName = splitedName[0];
                if (Directory.Exists(Path.Combine(resource.wrfRawDB, modelName, year, month, day, _run, "3hr", "SNOW")))
                    Directory.Delete(Path.Combine(resource.wrfRawDB, modelName, year, month, day, _run, "3hr", "SNOW"), true);
                Directory.CreateDirectory(Path.Combine(resource.wrfRawDB, modelName, year, month, day, _run, "3hr", "SNOW"));

                foreach (var f in files)
                    File.Copy(f.FullName, Path.Combine(resource.wrfRawDB, modelName, year, month, day, _run, "3hr", "SNOW", f.Name));

                if (Directory.Exists(Path.Combine(resource.wrfRawDB, modelName, year, month, day, _run, "3hr", "SOIL")))
                    Directory.Delete(Path.Combine(resource.wrfRawDB, modelName, year, month, day, _run, "3hr", "SOIL"), true);
                Directory.CreateDirectory(Path.Combine(resource.wrfRawDB, modelName, year, month, day, _run, "3hr", "SOIL"));
                files = new DirectoryInfo(wrfModelsSOIL[i]).GetFiles("*.tif").Select(fn => new FileInfo(fn.FullName)).OrderBy(f => f.Name).ToArray();
                Array.Sort(files, ATG.atgMethods.CompareNatural);

                foreach (var f in files)
                    File.Copy(f.FullName, Path.Combine(resource.wrfRawDB, modelName, year, month, day, _run, "3hr", "SOIL", f.Name));
            }

            return true;
        }
        public static bool rain24hr(string _date, string _run)
        {
            string year = _date.Substring(0, 4);
            string month = _date.Substring(4, 2);
            string day = _date.Substring(6, 2);

            List<string> wrfModels = new List<string>();
            wrfModels.Add("ensemble");
            wrfModels.Add("ferrier");
            wrfModels.Add("lin");
            wrfModels.Add("wsm6");
            foreach(var model in wrfModels)
            {
                if (Directory.Exists(Path.Combine(resource.wrfRawDB, model, year, month, day, _run, "3hr", "RAIN-24hr")))
                    Directory.Delete(Path.Combine(resource.wrfRawDB, model, year, month, day, _run, "3hr", "RAIN-24hr"), true);
                Directory.CreateDirectory(Path.Combine(resource.wrfRawDB, model, year, month, day, _run, "3hr", "RAIN-24hr"));
            }

            for (int i = 0; i < 4; i ++)
            {
                FileInfo[] files = new DirectoryInfo(Path.Combine(resource.wrfRawDB, wrfModels[i], year, month, day, _run, "3hr","RAIN")).GetFiles("*.tif").Select(fn => new FileInfo(fn.FullName)).OrderBy(f => f.Name).ToArray();
                Array.Sort(files, ATG.atgMethods.CompareNatural);
                for (int j = 0; j <= (files.Length / 8) - 1; j ++)
                {
                    List<string> input = new List<string>();
                    string outputName = "";
                    for (int k = j * 8; k < 8 + (j * 8); k++)
                        input.Add(files[k].FullName);

                    outputName = Path.Combine(resource.wrfRawDB, wrfModels[i], year, month, day, _run, "3hr", "RAIN-24hr", "WRF0.11-"
                    + year + month + day + _run + "-RAIN-Sum24hr-" + Convert.ToString(j + 1) + "-"
                    + files[j * 8].Name.Substring(files[j * 8].Name.Length - 14, 10) + "~"
                    + files[j * 8 + 7].Name.Substring(files[j * 8 + 7].Name.Length - 14, 10) + ".tif");
                    rasterCalculation.sum(input, outputName);
                }
            }

            return true;
        }

        public static bool apcp24hr(string _date, string _run)
        {
            string year = _date.Substring(0, 4);
            string month = _date.Substring(4, 2);
            string day = _date.Substring(6, 2);

            List<string> wrfModels = new List<string>();
            wrfModels.Add("ensemble");
            wrfModels.Add("ferrier");
            wrfModels.Add("lin");
            wrfModels.Add("wsm6");
            foreach (var model in wrfModels)
            {
                if (Directory.Exists(Path.Combine(resource.wrfRawDB, model, year, month, day, _run, "3hr", "APCP-24hr")))
                    Directory.Delete(Path.Combine(resource.wrfRawDB, model, year, month, day, _run, "3hr", "APCP-24hr"), true);
                Directory.CreateDirectory(Path.Combine(resource.wrfRawDB, model, year, month, day, _run, "3hr", "APCP-24hr"));
            }

            for (int i = 0; i < 4; i++)
            {
                FileInfo[] files = new DirectoryInfo(Path.Combine(resource.wrfRawDB, wrfModels[i], year, month, day, _run, "3hr", "APCP")).GetFiles("*.tif").Select(fn => new FileInfo(fn.FullName)).OrderBy(f => f.Name).ToArray();
                Array.Sort(files, ATG.atgMethods.CompareNatural);
                for (int j = 0; j <= (files.Length / 8) - 1; j++)
                {
                    List<string> input = new List<string>();
                    string outputName = "";
                    for (int k = j * 8; k < 8 + (j * 8); k++)
                        input.Add(files[k].FullName);

                    outputName = Path.Combine(resource.wrfRawDB, wrfModels[i], year, month, day, _run, "3hr", "APCP-24hr", "WRF0.11-"
                    + year + month + day + _run + "-APCP-Sum24hr-" + Convert.ToString(j + 1) + "-"
                    + files[j * 8].Name.Substring(files[j * 8].Name.Length - 14, 10) + "~"
                    + files[j * 8 + 7].Name.Substring(files[j * 8 + 7].Name.Length - 14, 10) + ".tif");
                    rasterCalculation.sum(input, outputName);
                }
            }

            return true;
        }

        public static bool rain12hr(string _date, string _run)
        {
            string year = _date.Substring(0, 4);
            string month = _date.Substring(4, 2);
            string day = _date.Substring(6, 2);

            List<string> wrfModels = new List<string>();
            wrfModels.Add("ensemble");
            wrfModels.Add("ferrier");
            wrfModels.Add("lin");
            wrfModels.Add("wsm6");

            foreach (var model in wrfModels)
            {
                if (Directory.Exists(Path.Combine(resource.wrfRawDB, model, year, month, day, _run, "3hr", "RAIN-12hr")))
                    Directory.Delete(Path.Combine(resource.wrfRawDB, model, year, month, day, _run, "3hr", "RAIN-12hr"), true);
                Directory.CreateDirectory(Path.Combine(resource.wrfRawDB, model, year, month, day, _run, "3hr", "RAIN-12hr"));
            }

            for (int i = 0; i < 4; i++)
            {
                FileInfo[] files = new DirectoryInfo(Path.Combine(resource.wrfRawDB, wrfModels[i], year, month, day, _run, "3hr", "RAIN")).GetFiles("*.tif").Select(fn => new FileInfo(fn.FullName)).OrderBy(f => f.Name).ToArray();
                Array.Sort(files, ATG.atgMethods.CompareNatural);
                for (int j = 0; j <= (files.Length / 4) - 1; j++)
                {
                    List<string> input = new List<string>();
                    string outputName = "";
                    for (int k = j * 4; k < 4 + (j * 4); k++)
                        input.Add(files[k].FullName);

                    outputName = Path.Combine(resource.wrfRawDB, wrfModels[i], year, month, day, _run, "3hr", "RAIN-12hr", "WRF0.11-"
                    + year + month + day + _run + "-RAIN-Sum12hr-" + Convert.ToString(j + 1) + "-"
                    + files[j * 4].Name.Substring(files[j * 4].Name.Length - 14, 10) + "~"
                    + files[j * 4 + 3].Name.Substring(files[j * 4 + 3].Name.Length - 14, 10) + ".tif");
                    rasterCalculation.sum(input, outputName);
                }
            }

            return true;
        }

        public static bool apcp12hr(string _date, string _run)
        {
            string year = _date.Substring(0, 4);
            string month = _date.Substring(4, 2);
            string day = _date.Substring(6, 2);

            List<string> wrfModels = new List<string>();
            wrfModels.Add("ensemble");
            wrfModels.Add("ferrier");
            wrfModels.Add("lin");
            wrfModels.Add("wsm6");

            foreach (var model in wrfModels)
            {
                if (Directory.Exists(Path.Combine(resource.wrfRawDB, model, year, month, day, _run, "3hr", "APCP-12hr")))
                    Directory.Delete(Path.Combine(resource.wrfRawDB, model, year, month, day, _run, "3hr", "APCP-12hr"), true);
                Directory.CreateDirectory(Path.Combine(resource.wrfRawDB, model, year, month, day, _run, "3hr", "APCP-12hr"));
            }

            for (int i = 0; i < 4; i++)
            {
                FileInfo[] files = new DirectoryInfo(Path.Combine(resource.wrfRawDB, wrfModels[i], year, month, day, _run, "3hr", "APCP")).GetFiles("*.tif").Select(fn => new FileInfo(fn.FullName)).OrderBy(f => f.Name).ToArray();
                Array.Sort(files, ATG.atgMethods.CompareNatural);
                for (int j = 0; j <= (files.Length / 4) - 1; j++)
                {
                    List<string> input = new List<string>();
                    string outputName = "";
                    for (int k = j * 4; k < 4 + (j * 4); k++)
                        input.Add(files[k].FullName);

                    outputName = Path.Combine(resource.wrfRawDB, wrfModels[i], year, month, day, _run, "3hr", "APCP-12hr", "WRF0.11-"
                    + year + month + day + _run + "-APCP-Sum12hr-" + Convert.ToString(j + 1) + "-"
                    + files[j * 4].Name.Substring(files[j * 4].Name.Length - 14, 10) + "~"
                    + files[j * 4 + 3].Name.Substring(files[j * 4 + 3].Name.Length - 14, 10) + ".tif");
                    rasterCalculation.sum(input, outputName);
                }
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
                Path.Combine(resource.wrfRawDB,"ensemble", currnetDate.AddDays(-1).Year.ToString()
                , twoDigitNumber(currnetDate.AddDays(-1).Month)
                , twoDigitNumber(currnetDate.AddDays(-1).Day), _run, "3hr", variable), "*.tif").Select(fn => new FileInfo(fn)).OrderBy(f => f.Name).ToArray();
            Array.Sort(inputFiles, ATG.atgMethods.CompareNatural);
            int index = 0;
            if (Directory.Exists(Path.Combine(resource.wrfRawDB,"ensemble", currnetDate.AddDays(-1).Year.ToString()
                , twoDigitNumber(currnetDate.AddDays(-1).Month)
                , twoDigitNumber(currnetDate.AddDays(-1).Day), _run, "3hr", "Daily-" + variable)))
                Directory.Delete(Path.Combine(resource.wrfRawDB, "ensemble", currnetDate.AddDays(-1).Year.ToString()
                , twoDigitNumber(currnetDate.AddDays(-1).Month)
                , twoDigitNumber(currnetDate.AddDays(-1).Day), _run, "3hr", "Daily-" + variable), true);

            Directory.CreateDirectory(Path.Combine(resource.wrfRawDB, "ensemble",  currnetDate.AddDays(-1).Year.ToString()
                , twoDigitNumber(currnetDate.AddDays(-1).Month)
                , twoDigitNumber(currnetDate.AddDays(-1).Day), _run, "3hr", "Daily-" + variable));

            string outputAddress = Path.Combine(resource.wrfRawDB, "ensemble", currnetDate.AddDays(-1).Year.ToString()
                , twoDigitNumber(currnetDate.AddDays(-1).Month)
                , twoDigitNumber(currnetDate.AddDays(-1).Day), _run, "3hr", "Daily-" + variable);

            for (int i = 0; i < inputFiles.Length; i++)
            {
                if (extractDateFromFileName(inputFiles[i]) == currnetDate)
                {
                    index = i;
                    break;
                }
            }
            for (int i = 0; i < 3; i++)
            {
                List<string> sumFiles = new List<string>();
                for (int j = index; j < index + 8; j++)
                    sumFiles.Add(inputFiles[j].FullName);
                index += 8;
                rasterCalculation.sum(sumFiles, Path.Combine(outputAddress, Convert.ToString(i + 1)) + ".tif");
            }
            return true;
        }
    }
}
