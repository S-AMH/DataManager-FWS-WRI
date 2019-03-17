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
                updateHandlerWRF.updateDB();
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


        }
    }
}
