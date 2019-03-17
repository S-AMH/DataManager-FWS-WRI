using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataManager
{
    class updateHandlerWRF
    {
        public static bool uploadWRF(string date, string run, string variable = "RAIN")
        {
            if(variable == "APCP")
            {
                Process cmd = new Process();
                cmd.StartInfo.FileName = @"cmd.exe";
                cmd.StartInfo.Arguments = @"/C " + "python " + resource.uploadWRFAPCP + " " + date + run;
                cmd.Start();
                cmd.WaitForExit();
            }
            else
            {
                Process cmd = new Process();
                cmd.StartInfo.FileName = @"cmd.exe";
                cmd.StartInfo.Arguments = @"/C " + "python " + resource.uploadWRF + " " + date + run;
                cmd.Start();
                cmd.WaitForExit();
            }
            return true;
        }
        public static bool publishWRF(string variable = "RAIN")
        {
            if(variable == "APCP")
            {
                Process cmd = new Process();
                cmd.StartInfo.FileName = @"cmd.exe";
                cmd.StartInfo.Arguments = @"/C python " + resource.publishWRFAPCP;
                cmd.Start();
                cmd.WaitForExit();
            }
            else
            {
                Process cmd = new Process();
                cmd.StartInfo.FileName = @"cmd.exe";
                cmd.StartInfo.Arguments = @"/C python " + resource.publishWRF;
                cmd.Start();
                cmd.WaitForExit();
            }
            return true;
        }
        public static bool TestUploadWRF(string date, string run)
        {
            Process cmd = new Process();
            cmd.StartInfo.FileName = @"cmd.exe";
            cmd.StartInfo.Arguments = @"/C " + "python " + resource.testUploadWRF + " " + date + run;
            cmd.Start();
            cmd.WaitForExit();
            return true;
        }
        public static string twoDigitNumber(int _num)
        {
            if (_num < 10)
                return "0" + Convert.ToString(_num);
            else
                return Convert.ToString(_num);
        }
        public static bool updateDB()
        {
            Console.WriteLine("Updating WRF Database Process Started.");
            FileInfo[] files = new DirectoryInfo(resource.wrfTempDir).GetFiles("*.tif", SearchOption.AllDirectories).Select(fn => new FileInfo(fn.FullName)).OrderBy(f => f.Name).ToArray(); ;
            //if (files.Count() > 275)
            //{
            //    for (int i = 0; i < files.Count()-275; i++)
            //        File.Delete(files[i].FullName);
            //    files = new DirectoryInfo(resource.wrfTempDir).GetFiles("*.tif", SearchOption.AllDirectories).Select(fn => new FileInfo(fn.FullName)).OrderBy(f => f.Name).ToArray();
            //}
            //else
            //    return true;
            if (files.Count() != 275)
                return true;
            files = new DirectoryInfo(resource.wrfTiffDirEnsembleAPCP).GetFiles("*.tif").Select(fn => new FileInfo(fn.FullName)).OrderBy(f => f.Name).ToArray(); ;
            string[] tmp = files[0].Name.Split('-');
            string date = tmp[1].Substring(3, 10);
            string run = date.Substring(date.Length-2, 2);
            date = date.Substring(0, date.Length - 2);
            Console.WriteLine("WRF Date is: " + date + "\n \t Run is: " + run);
            Console.WriteLine("Convert APCP to RAIN : \n \t Status:  Started.");
            WRF0p11.convertAPCP2RAIN(date, run);
            Console.WriteLine("Convert APCP to RAIN : \n \t Status:  Finished.");

            Console.WriteLine("Writing Tempreture GeoTiff Data to Database: \n \t Status:  Started.");
            WRF0p11.temp2RawDB(date, run);
            Console.WriteLine("Writing Tempreture GeoTiff Data to Database: \n \t Status:  Finished.");

            Console.WriteLine("Writing APCP GeoTiff Data to Database: \n \t Status:  Started.");
            WRF0p11.apcp2RawDB(date, run);
            Console.WriteLine("Writing Tempreture GeoTiff Data to Database: \n \t Status:  Started.");

            Console.WriteLine("Writing Soil Moisture and Snow Data to Database: \n \t Status:  Started.");
            WRF0p11.snowSoildMoisture2RawDB(date, run);
            Console.WriteLine("Writing Soil Moisture and Snow Data to Database: \n \t Status:  Finished.");

            Console.WriteLine("Calculating 24 Hour Commulicative Rain Grids: \n \t Status: Started.");
            WRF0p11.rain24hr(date, run);
            Console.WriteLine("Calculating 24 Hour Commulicative Rain Grids: \n \t Status: Started.");

            Console.WriteLine("Calculating 12 Hour Commulicative Rain Grids: \n \t Status: Started.");
            WRF0p11.rain12hr(date, run);
            Console.WriteLine("Calculating 12 Hour Commulicative Rain Grids: \n \t Status: Finished.");

            Console.WriteLine("Calculating 3 Hour Commulicative Rain Grids: \n \t Status: Started.");
            try
            {
                WRF0p11.commulicativeRAIN(date, run);
            }
            catch
            {
                Console.WriteLine("Error: Cummulicative 3hr rasters failed...");
            }

            Console.WriteLine("Calculating 3 Hour Commulicative Rain Grids: \n \t Status: Finished.");

            Console.WriteLine("Calculating Daily Rain Grids: \n \t Status: Started.");
            try
            {
                WRF0p11.daily(date, run);
            }
            catch
            {
                Console.WriteLine("Error: Can not Creat WRF Daily Rain Griods...");
            }
            Console.WriteLine("Calculating Daily Rain Grids: \n \t Status: Finished.");

            Console.WriteLine("Zonal For Points Layers Operation: \n \t Status: Started.");
            SagaProcess.zonalForPointsWRF(date, run);
            Console.WriteLine("Zonal For Points Layers Operation: \n \t Status: Finished.");

            Console.WriteLine("Zonal For Polygon Layers Operation: \n \t Status: Started.");
            SagaProcess.zonalForPolygonsWRF(date, run);
            Console.WriteLine("Zonal For Polygon Layers Operation: \n \t Status: Finished.");

            Console.WriteLine("Updating WRF Database Process Finished.");

            Console.WriteLine("Publishing New Services: \n \t Status:  Finished.");
            Console.WriteLine("Updating WRF Process Log Files: \n \t Status: Started.");
            StreamWriter obj_reader = new StreamWriter(resource.uploadLogWRF, true);
            obj_reader.WriteLine(date + '-' + run);
            obj_reader.Close();
            Console.WriteLine("Updating WRF Process Log Files: \n \t Status: Finished.");


            Console.WriteLine("Calculating Sum 12 hr for APCP.");
            WRF0p11.apcp12hr(date, run);
            Console.WriteLine("Calculating Sum 24 hr for APCP.");
            WRF0p11.apcp24hr(date, run);
            Console.WriteLine("Calculating cummulicative 3hr rasters for APCP.");
            try
            {
                WRF0p11.commulicativeAPCP(date, run);
            }
            catch
            {
                Console.WriteLine("Error: Commulicative 3hr rasters for APCP faild...");
            }

            Console.WriteLine("Calculating Daily APCP Grids: \n \t Status: Started.");
            try
            {
                WRF0p11.daily(date, run, "APCP");
            }
            catch
            {
                Console.WriteLine("Erorr Creating WRF Daily APCP Grids.");
            }
            
            Console.WriteLine("Calculating Daily APCP Grids: \n \t Status: Finished.");

            Console.WriteLine("Zonal for points(APCP).");
            SagaProcess.zonalForPointsWRF(date, run, "APCP");
            Console.WriteLine("Zonal for Polygons(APCP)");
            SagaProcess.zonalForPolygonsWRF(date, run, "APCP");

            Console.WriteLine("Uploading WRF Model To SQL Server: \n \t Status: Started.");
            uploadWRF(date, run);
            Console.WriteLine("Uploading WRF Model To SQL Server: \n \t Status: Finished.");
            Console.WriteLine("Uploading WRF Model To SQL Server: \n \t Status: Started.");
            uploadWRF(date, run, "APCP");
            Console.WriteLine("Uploading GFS Model To SQL Server: \n \t Status: Finished.");

            Console.WriteLine("Publishing New Services: \n \t Status:  Started.");
            publishWRF();
            Console.WriteLine("Publishing New Services: \n \t Status:  Started.");
            publishWRF("APCP");



            FileInfo[] tiffFiles = new DirectoryInfo(resource.wrfTempDir).GetFiles("*.tif", SearchOption.AllDirectories).Select(fn => new FileInfo(fn.FullName)).OrderBy(f => f.Name).ToArray();

            foreach (var f in tiffFiles)
                File.Delete(f.FullName);

            FileInfo[] xmlFiles = new DirectoryInfo(resource.wrfTempDir).GetFiles("*.tif", SearchOption.AllDirectories).Select(fn => new FileInfo(fn.FullName)).OrderBy(f => f.Name).ToArray();

            foreach (var f in xmlFiles)
                File.Delete(f.FullName);
            Console.WriteLine("TEST UPLOAD WRF...");
            TestUploadWRF(date, run);
            string[] configNames = { "ensemble", "ferrier", "lin", "wsm6" };

            return true;
        }
    }
}
