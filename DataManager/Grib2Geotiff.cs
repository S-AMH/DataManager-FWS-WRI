using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;
using System.Diagnostics;

namespace DataManager
{
    static class Grib2Geotiff
    {
        public static int convert2geotiff(Queue<Tuple<string, string>> _fileNames, int _band)
        {

            using (SemaphoreSlim concurrencySemaphore = new SemaphoreSlim(100))
            {

                List<Task> tasks = new List<Task>();
                foreach(var fileName in _fileNames)
                {
                    concurrencySemaphore.Wait();
                    var t = Task.Factory.StartNew<int>(() =>
                    {
                        Process convert = new Process();
                        try
                        {
                            convert.StartInfo.FileName = resource.gdal_translateDir;
                            convert.StartInfo.UseShellExecute = false;
                            convert.StartInfo.CreateNoWindow = true;
                            convert.StartInfo.WindowStyle = ProcessWindowStyle.Hidden;
                            convert.StartInfo.Arguments = "-ot Float32 -of GTiff ";
                            convert.StartInfo.Arguments += " -b " + Convert.ToString(_band) + " \"" + fileName.Item1 + "\"  \"" + fileName.Item2 + "\"";
                            convert.Start();
                            convert.WaitForExit();
                        }
                        finally
                        {
                            concurrencySemaphore.Release();
                        }
                        return convert.ExitCode;
                    });
                    tasks.Add(t);
                }
                Task.WaitAll(tasks.ToArray());
            }

            return 0x000;
        }
    }
}
