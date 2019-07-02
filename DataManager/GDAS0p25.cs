using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataManager
{
    class GDAS0p25
    {

        static private List<string> variables = resource.GDAS0p25VariablesList.Split(',').ToList();
        static private List<string> levels = resource.GDAS0p25HeightList.Split(',').ToList();

        static string threeDigitNumber(int _num)
        {
            if (_num < 10)
                return "00" + Convert.ToString(_num);
            else if (_num < 100)
                return "0" + Convert.ToString(_num);
            else
                return Convert.ToString(_num);
        }

        static public Queue<string> urlGenerator(string _date, string _run)
        {
            Queue<string> urls = new Queue<string>();
            string url;
            for(int i = 1; i < 10; i ++)
            {
                url = @"http://nomads.ncep.noaa.gov/cgi-bin/filter_gdas_0p25.pl?file=gdas.t";
                url += _run;
                url += "z.pgrb2.0p25.f";
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
                url += @"&dir=%2Fgdas.";
                url += _date;
                urls.Enqueue(url);
            }
            return urls;
        }
        public static Queue<Tuple<string, string>> getFileNamesDownloaded(string _var)
        {
            Queue<Tuple<string, string>> fileNames = new Queue<Tuple<string, string>>();
            DirectoryInfo dirInfo = new DirectoryInfo(resource.GDAS0p25DownloadOutputDir);
            FileInfo[] fileInfo = dirInfo.GetFiles().Select(fn => new FileInfo(fn.FullName)).OrderBy(f => f.Name).ToArray();
            Array.Sort(fileInfo, ATG.atgMethods.CompareNatural);
            string outAdress = "";
            if (_var == "TEMP")
                outAdress = resource.GDAS0p25TiffDirTEMP;
            else if (_var == "APCP")
                outAdress = resource.GDAS0p25TiffDirAPCP;

            for (int i = 0; i < fileInfo.Length; i++)
                fileNames.Enqueue(new Tuple<string, string>(fileInfo[i].FullName, Path.Combine(outAdress, fileInfo[i].Name + ".tiff")));

            return fileNames;
        }
    }
}
