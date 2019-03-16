using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;
using System.Threading;
using System.IO;

namespace DataManager
{
    static class Download
    {
        static bool checkForDownloadError(int x)
        {
            if (x != 0)
                return false;
            else
                return true;
        }
        static string threeDigitNumber(int _num)
        {
            if (_num < 10)
                return "00" + Convert.ToString(_num);
            else if (_num < 100)
                return "0" + Convert.ToString(_num);
            else
                return Convert.ToString(_num);
        }
        static public int startDownload(string _date, string _run, int init = 0)
        {

            int maxConcurreny = 15;
            int counter = 0;
            List<int> downloadResults = new List<int>();
            Queue <string> gfs0p13 = GFS0p13.urlGenerator( _date, _run, init);

            if(Directory.Exists(resource.GFS0p13DownloadOutputDir) && init == 0)
                Directory.Delete(resource.GFS0p13DownloadOutputDir, true);
            if (Directory.Exists(resource.GFS0p13InitDownloadOutputDir) && init != 0)
                Directory.Delete(resource.GFS0p13InitDownloadOutputDir, true);

            Directory.CreateDirectory(resource.GFS0p13DownloadOutputDir);
            Directory.CreateDirectory(resource.GFS0p13InitDownloadOutputDir);

            using (SemaphoreSlim concurrencySemaphore = new SemaphoreSlim(maxConcurreny))
            {
                List<Task> tasks = new List<Task>();
                foreach (var dlreq in gfs0p13)
                {
                    concurrencySemaphore.Wait();
                    var t = Task.Factory.StartNew<int>(() =>
                    {
                        Process dl = new Process();
                        try
                        {
                            dl.StartInfo.FileName = resource.curlDir;
                            dl.StartInfo.UseShellExecute = false;
                            dl.StartInfo.CreateNoWindow = true;
                            dl.StartInfo.WindowStyle = ProcessWindowStyle.Hidden;
                            dl.StartInfo.Arguments = "\"" + dlreq + "\"";

                            if(init == 0)
                                dl.StartInfo.Arguments += " --retry 10 --connect-timeout 100 -m 1000 -o " + "\"" + resource.GFS0p13DownloadOutputDir + @"\" + "GFS0p13-" + _date + _run +
                                   "f" + threeDigitNumber(Convert.ToInt32(dlreq.Substring(80, 3).TrimEnd('.'))) + "\"";
                            else
                                dl.StartInfo.Arguments += " --retry 10 --connect-timeout 100 -m 1000 -o " + "\"" + resource.GFS0p13InitDownloadOutputDir + @"\" + "GFS0p13-" + _date + _run +
                                "f" + threeDigitNumber(Convert.ToInt32(dlreq.Substring(80, 3).TrimEnd('.'))) + "\"";
                            dl.Start();
                            dl.WaitForExit();
                        }
                        catch(Exception e)
                        {
                            Console.WriteLine("Following Error Occured: " + e.Message);
                        }
                        finally
                        {
                            concurrencySemaphore.Release();
                        }
                        downloadResults.Add(dl.ExitCode);
                        return dl.ExitCode;
                    });
                    tasks.Add(t);

                    counter++;
                    if (counter == 10)
                    {
                        counter = 0;
                        Thread.Sleep(50);
                    }
                }
                try
                {
                    Task.WaitAll(tasks.ToArray());
                }
                catch
                {
                    Console.WriteLine("Error Occured while Downloading... \n Please Check Network Connection ...");
                    //return -1;
                }
                
            }
            if (downloadResults.All(checkForDownloadError))
                return 0;
            else
                return -1;
        }

    }
}
