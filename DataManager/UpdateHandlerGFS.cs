using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
using System.Threading;

namespace DataManager
{
    class UpdateHandlerGFS
    {
        public static bool uploadGFS(string date, string run, string variable = "RAIN")
        {
            if (variable == "APCP")
            {
                System.Diagnostics.Process cmd = new System.Diagnostics.Process();
                cmd.StartInfo.FileName = @"cmd.exe";
                cmd.StartInfo.Arguments = @"/C " + "python " + resource.uploadGfsApcp + " " + date + run;
                cmd.Start();
                cmd.WaitForExit();
                return true;
            }
            else
            {
                System.Diagnostics.Process cmd = new System.Diagnostics.Process();
                cmd.StartInfo.FileName = @"cmd.exe";
                cmd.StartInfo.Arguments = @"/C " + "python " + resource.uploadGFS + " " + date + run;
                cmd.Start();
                cmd.WaitForExit();
                return true;
            }
        }
        public static bool testUploadGFS(string date, string run)
        {
            System.Diagnostics.Process cmd = new System.Diagnostics.Process();
            cmd.StartInfo.FileName = @"cmd.exe";
            cmd.StartInfo.Arguments = @"/C " + "python " + resource.testUploadGFS + " " + date + run;
            cmd.Start();
            cmd.WaitForExit();
            return true;
        }
        public static bool publishGFS(string variable="RAIN")
        {
            if (variable == "APCP")
            {
                System.Diagnostics.Process cmd = new System.Diagnostics.Process();
                cmd.StartInfo.FileName = @"cmd.exe";
                cmd.StartInfo.Arguments = @"/C " + "python " + resource.publishGfsApcp;
                cmd.Start();
                cmd.WaitForExit();
                return true;
            }
            else
            {
                System.Diagnostics.Process cmd = new System.Diagnostics.Process();
                cmd.StartInfo.FileName = @"cmd.exe";
                cmd.StartInfo.Arguments = @"/C " + "python " + resource.publishGFS;
                cmd.Start();
                cmd.WaitForExit();
                return true;
            }
        }
        static Queue<string> readDBUpdate()
        {
            string[] runs = { "00", "06", "12", "18" };
            Queue<string> siteContent = new Queue<string>();
            Queue<string> output = new Queue<string>();
            StreamReader obj_read = new StreamReader(resource.DBUpdate);
            if (obj_read.EndOfStream)
                return new Queue<string>();
            while (obj_read.Peek() != -1)
                siteContent.Enqueue(obj_read.ReadLine());
            obj_read.Close();
            foreach (var content in siteContent)
                for (int i = 0; i < Convert.ToInt16(content.Substring(content.Length - 1, 1)); i++)
                    output.Enqueue(content.Substring(0, content.Length - 1) + "-" + runs[i]);
            return output;
        }
        static List<string> downloadLogs()
        {
            List<string> lastDownloaded = new List<string>();
            if (!File.Exists(resource.DownloadLog))
                File.Create(resource.DownloadLog).Close();
            StreamReader obj_reader = new StreamReader(resource.DownloadLog);
            while (obj_reader.Peek() != -1)
                lastDownloaded.Add(obj_reader.ReadLine());
            obj_reader.Close();
            return lastDownloaded;
        }
        static Queue<string> downloadQuery()
        {
            Queue<string> downloadQuery = new Queue<string>();
            List<string> updateDB = readDBUpdate().ToList();
            List<string> downloadLog = downloadLogs();
            foreach (var f in updateDB)
                if (!downloadLog.Contains(f))
                    downloadQuery.Enqueue(f);
            StreamWriter obj_writer = new StreamWriter(resource.downloadQuery);
            foreach (var f in downloadQuery)
                obj_writer.WriteLine(f);
            obj_writer.Close();
            return downloadQuery;
        }

        static public bool updateDB()
        {
            downloadQuery();
            StreamReader obj_reader = new StreamReader(resource.downloadQuery);
            Queue<string> downloadQueries = new Queue<string>();
            while (obj_reader.Peek() != -1)
                downloadQueries.Enqueue(obj_reader.ReadLine());
            obj_reader.Close();
            Console.WriteLine("Download Query contains:");
            foreach(var f in downloadQueries)
                Console.WriteLine(f);

            Console.WriteLine("Download Query Updated.");

            while(downloadQueries.Count > 0)
            {
                int downloadStatus = 0;
                int counter = 1;
                string[] queryDate = downloadQueries.Dequeue().Split('-');
                Console.WriteLine("Start process for :" + queryDate[0] + " " + queryDate[1]);
                Console.WriteLine("Downloading Files Started.");

                do
                {
                    Console.WriteLine("\t Try " + counter + "... .");
                    downloadStatus = Download.startDownload(queryDate[0], queryDate[1]);
                    counter++;
                }
                while (downloadStatus != 0);

                Console.WriteLine("Downloading Files Finished.");
                counter = 1;

                Console.WriteLine("Download Initial Files started.");
                do
                {
                    Console.WriteLine("\t Try " + counter + "... .");
                    downloadStatus = Download.startDownload(queryDate[0], queryDate[1], 1);
                    counter++;
                }
                while (downloadStatus != 0);
                Console.WriteLine("Downloading Initial Files Finished.");
                counter = 1;
                Console.WriteLine("Extracting Tempreture GeoTiffs from GRIB2 Files: \n \t Status:  Started.");
                Grib2Geotiff.convert2geotiff(GFS0p13.getFileNamesDownloaded("TEMP"), 2);
                

                Console.WriteLine("Extracting PRATE GeoTiffs from GRIB2 Files: \n \t Status: Started.");
                Grib2Geotiff.convert2geotiff(GFS0p13.getFileNamesDownloaded("PRATE"), 3);
                

                Console.WriteLine("Extracting SNOW GeoTiffs from GRIB2 Files: \n \t Status:  Started.");
                Grib2Geotiff.convert2geotiff(GFS0p13.getFileNamesDownloaded("SNOW", 1), 5);
                
                Console.WriteLine("Extracting Tempreture GeoTiffs from GRIB2 Files: \n \t Status:  Finished.");
                Console.WriteLine("Extracting PRATE GeoTiffs from GRIB2 Files: \n \t Status:  Finished.");
                Console.WriteLine("Extracting SNOW GeoTiffs from GRIB2 Files: \n \t Status:  Finished.");

                Console.WriteLine("Converting PRATE to APCP: \n \t Status:  Started.");
                GFS0p13.ConvertPRATE2APCP();
                Console.WriteLine("Converting PRATE to APCP: \n \t Status:  Finished.");

                Console.WriteLine("Convert APCP to RAIN : \n \t Status:  Started.");
                GFS0p13.convertAPCP2RAIN();
                Console.WriteLine("Converting APCP to RAIN: \n \t Status:  Finished.");

                Console.WriteLine("Converting Commulicative APCP to Single Time APCP Raster: \n \t Status:  Started.");
                GFS0p13.comcumulative2Single(queryDate[0], queryDate[1], 1, "APCP");
                

                Console.WriteLine("Converting Commulicative RAIN to Single Time RAIN Raster: \n \t Status:  Started.");
                GFS0p13.comcumulative2Single(queryDate[0], queryDate[1], 1, "RAIN");
                

                Console.WriteLine("Converting Commulicative APCP to Single Time APCP Raster: \n \t Status:  Started.");
                GFS0p13.comcumulative2Single(queryDate[0], queryDate[1], 3, "APCP");
                

                Console.WriteLine("Converting Commulicative RAIN to Single Time RAIN Raster: \n \t Status:  Started.");
                GFS0p13.comcumulative2Single(queryDate[0], queryDate[1], 3, "RAIN");
                
                Console.WriteLine("Converting Commulicative APCP to Single Time APCP Raster: \n \t Status:  Finished.");
                Console.WriteLine("Converting Commulicative RAIN to Single Time RAIN Raster: \n \t Status:  Finished.");
                Console.WriteLine("Converting Commulicative APCP to Single Time APCP Raster: \n \t Status:  Finished.");
                Console.WriteLine("Converting Commulicative RAIN to Single Time RAIN Raster: \n \t Status:  Finished.");

                Console.WriteLine("Writing Tempreture GeoTiff Data to Database: \n \t Status:  Started.");
                GFS0p13.temp2RawDB(queryDate[0], queryDate[1]);
                Console.WriteLine("Writing Tempreture GeoTiff Data to Database: \n \t Status:  Finished.");


                Console.WriteLine("Writing SOIL MOISTURE Data to Database: \n \t Status:  Started.");

                string outAdress = resource.GFS0p13TiffDirSOILW;
                if (Directory.Exists(outAdress))
                    Directory.Delete(outAdress, true);
                Directory.CreateDirectory(outAdress);
                DirectoryInfo dirinfo = new DirectoryInfo(resource.GFS0p13InitDownloadOutputDir);
                FileInfo[] files = dirinfo.GetFiles().Select(fn => new FileInfo(fn.FullName)).OrderBy(f => f.Name).ToArray();
                string tmp = Path.Combine(outAdress, files[0].Name + "-0-0.1.tiff");
                Tuple<string, string> tmp2 = new Tuple<string, string>(files[0].FullName, tmp);
                Queue<Tuple<string, string>> amir = new Queue<Tuple<string, string>>();
                amir.Enqueue(tmp2);
                Grib2Geotiff.convert2geotiff(amir, 1);
                tmp = Path.Combine(outAdress, files[0].Name + "-0.1-0.4.tiff");
                tmp2 = new Tuple<string, string>(files[0].FullName, tmp);
                amir.Clear();
                amir.Enqueue(tmp2);
                Grib2Geotiff.convert2geotiff(amir, 2);
                tmp = Path.Combine(outAdress, files[0].Name + "-0.4-1.tiff");
                tmp2 = new Tuple<string, string>(files[0].FullName, tmp);
                amir.Clear();
                amir.Enqueue(tmp2);
                Grib2Geotiff.convert2geotiff(amir, 3);
                tmp = Path.Combine(outAdress, files[0].Name + "-1-2.tiff");
                tmp2 = new Tuple<string, string>(files[0].FullName, tmp);
                amir.Clear();
                amir.Enqueue(tmp2);
                Grib2Geotiff.convert2geotiff(amir, 4);
                amir.Clear();
                Console.WriteLine("Writing SOIL MOISTURE Data to Database: \n \t Status:  Finished.");

                Console.WriteLine("Writing SNOW and Soil Moisture Data to Database: \n \t Status: Started.");
                GFS0p13.snowSoildMoisture2RawDB(queryDate[0], queryDate[1]);
                Console.WriteLine("Writing SNOW and Soil Moisture Data to Database: \n \t Status: Finished.");

                Console.WriteLine("Calculating 12 Hour Commulicative Rain Grids: \n \t Status: Started.");
                GFS0p13.rain12hr(queryDate[0], queryDate[1]);
                Console.WriteLine("Calculating 12 Hour Commulicative Rain Grids: \n \t Status: Finished.");

                Console.WriteLine("Calculating 24 Hour Commulicative Rain Grids: \n \t Status: Started.");
                GFS0p13.rain24hr(queryDate[0], queryDate[1]);
                Console.WriteLine("Calculating 24 Hour Commulicative Rain Grids: \n \t Status: Finished.");

                Console.WriteLine("Calculating 3 Hour Commulicative Rain Grids: \n \t Status: Started.");
                GFS0p13.commulicativeRAIN(queryDate[0], queryDate[1]);
                Console.WriteLine("Calculati 3 Hour Commulicative Rain Grids: \n \t Status: Finished.");

                Console.WriteLine("Calculating Daily Rain Grids: \n \t Status: Started.");
                GFS0p13.daily(queryDate[0], queryDate[1]);
                Console.WriteLine("Calculating Daily Rain Grids: \n \t Status: Finished.");

                Console.WriteLine("Zonal For Point Layers Operation: \n \t Status: Started.");
                SagaProcess.zonalForPoints(queryDate[0], queryDate[1]);
                

                Console.WriteLine("Zonal For Polygon Layers Operation: \n \t Status: Started.");
                SagaProcess.zonalForPolygons(queryDate[0], queryDate[1]);
                

                Console.WriteLine("Zonal For Point Layers Operation: \n \t Status: Finished.");
                Console.WriteLine("Zonal For Polygon Layers Operation: \n \t Status: Finished.");


                Console.WriteLine("Calculating 12 Hour Commulicative APCP Grids: \n \t Status: Started.");
                GFS0p13.APCP12hr(queryDate[0], queryDate[1]);
                Console.WriteLine("Calculating 12 Hour Commulicative APCP Grids: \n \t Status: Finished.");

                Console.WriteLine("Calculating 24 Hour Commulicative APCP Grids: \n \t Status: Started.");
                GFS0p13.APCP24hr(queryDate[0], queryDate[1]);
                Console.WriteLine("Calculating 24 Hour Commulicative APCP Grids: \n \t Status: Finished.");

                Console.WriteLine("Calculating 3 Hour Commulicative APCP Grids: \n \t Status: Started.");
                GFS0p13.commulicativeAPCP(queryDate[0], queryDate[1]);
                Console.WriteLine("Calculating 3 Hour Commulicative APCP Grids: \n \t Status: Finished.");

                Console.WriteLine("Calculating Daily APCP Grids: \n \t Status: Started.");
                GFS0p13.daily(queryDate[0], queryDate[1], "APCP");
                Console.WriteLine("Calculating Daily APCP Grids: \n \t Status: Finished.");

                Console.WriteLine("Zonal For Point Layers Operation: \n \t Status: Started.");
                SagaProcess.zonalForPoints(queryDate[0], queryDate[1], "APCP");
                Console.WriteLine("Zonal For Point Layers Operation: \n \t Status: Finished.");

                Console.WriteLine("Zonal For Polygon Layers Operation: \n \t Status: Started.");
                SagaProcess.zonalForPolygons(queryDate[0], queryDate[1], "APCP");
                Console.WriteLine("Zonal For Polygon Layers Operation: \n \t Status: Finished.");
                Console.WriteLine("Uploading APCP Data to SQL.");


                Console.WriteLine("Uploading GFS Model To SQL Server: \n \t Status: Started.");
                uploadGFS(queryDate[0], queryDate[1]);
                publishGFS();

                Console.WriteLine("Updating Download And Process Log Files: \n \t Status: Started.");
                StreamWriter obj_writer = new StreamWriter(resource.uploadLog, true);
                obj_writer.WriteLine(queryDate[0] + "-" + queryDate[1]);
                obj_writer.Close();
                Console.WriteLine("Updating Download And Process Log Files: \n \t Status: Finished.");
                uploadGFS(queryDate[0], queryDate[1], "APCP");

                Console.WriteLine("Publishing Changes to web...");
                publishGFS("APCP");



                obj_writer = new StreamWriter(resource.downloadQuery);
                foreach (var f in downloadQueries)
                    obj_writer.WriteLine(f);
                obj_writer.Close();
                obj_writer = new StreamWriter(resource.DownloadLog, true);
                obj_writer.WriteLine(queryDate[0] + "-" + queryDate[1]);
                obj_writer.Close();
                Console.WriteLine("Running TEST GFS UPLOAD...");
                testUploadGFS(queryDate[0], queryDate[1]);

            }

            return true;
        }
    }
}
