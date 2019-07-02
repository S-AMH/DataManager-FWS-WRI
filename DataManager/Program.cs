/*
 * 
 *  @Author: S.AmirMohammad Hasanli <amir.hasanli@ut.ac.ir>
 *  @Company: Water Research Institute <www.wri.ac.ir>
 * 
 */

using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using OSGeo.GDAL;
using OSGeo.OGR;
using OSGeo.OSR;
using Logger;
using System.IO;
using System.Diagnostics;
using System.Threading;

namespace DataManager
{
    class Program
    {
        static string twoDigitNumber(int _num)
        {
            if (_num < 10)
                return "0" + Convert.ToString(_num);
            else
                return Convert.ToString(_num);
        }
        static void Main(string[] args)
        {
            //Directory.SetCurrentDirectory(@"C:\Program Files\GDAL");
            GdalConfiguration.ConfigureGdal();
            GdalConfiguration.ConfigureOgr();
            Gdal.AllRegister();
            Ogr.RegisterAll();
            Gdal.PushErrorHandler("CPLQuietErrorHandle");
            Gdal.SetErrorHandler("CPLQuietErrorHandle");


            try
            {
                bool result = updateHandlerWRF.updateDB();
            }
            catch(Exception e)
            {
                Console.WriteLine("Error Executing WRF process...");
                Console.WriteLine("following Error occured: " + e.Message);
                Console.WriteLine(e.StackTrace);
            }
            try
            {
                UpdateHandlerGFS.updateDB();
            }
            catch(Exception e)
            {
                Console.WriteLine("Error Executing GFS0p13 process...");
                Console.WriteLine("following Error occured: " + e.Message);
                Console.WriteLine(e.StackTrace);
            }
            try
            {
                StreamReader r = new StreamReader(resource.IconFlag);
                string str = r.ReadLine();
                r.Close();
                r.Dispose();
                if (str == "1")
                    throw new Exception("Icon Is Currently Downloading... retry in few minutes.");
                else
                {
                    r = new StreamReader(resource.CurrentICONDateAndRun);
                    str = r.ReadLine();
                    r.Close();
                    r.Dispose();
                    Console.WriteLine(str);
                    Console.WriteLine(str.Substring(0, 8) + " : " + str.Substring(str.Length - 2, 2));
                    StreamWriter sw = new StreamWriter(resource.IconFlag);
                    sw.Write("1");
                    sw.Close();
                    sw.Dispose();
                    UpdateHandlerICON.updateDB(str.Substring(0, 8), str.Substring(str.Length - 2, 2));
                    sw = new StreamWriter(resource.IconFlag);
                    sw.Write("0");
                    sw.Close();
                    sw.Dispose();
                }
            }
            catch(Exception e)
            {
                Console.WriteLine("Error Executing ICON process...");
                Console.WriteLine("following Error occured: " + e.Message);
                Console.WriteLine(e.StackTrace);
            }


        }
    }
}
