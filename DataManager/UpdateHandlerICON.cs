using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataManager
{
    class UpdateHandlerICON
    {
        public static void updateDB(string date, string run)
        {
            string[] logs = { @"C:\FWS\DB\tmpFiles\ICON-Log00.txt",
                @"C:\FWS\DB\tmpFiles\ICON-Log06.txt",
                @"C:\FWS\DB\tmpFiles\ICON-Log12.txt",
                @"C:\FWS\DB\tmpFiles\ICON-Log18.txt" };
            string[] runs = { "00", "06", "12", "18" };

            var countF = 0;
            countF += new DirectoryInfo(resource.ICON_Grb2).GetFiles("*.grib2", SearchOption.AllDirectories).Count();
            //Console.WriteLine(countF);
            //if (countF != 120)
            //{
            //    FileInfo[] files2 = new DirectoryInfo(resource.ICON_Grb2).GetFiles("*.*", SearchOption.AllDirectories);
            //    foreach (var f in files2)
            //        File.Delete(f.FullName);
            //    files2 = new DirectoryInfo(resource.ICON_Tiff).GetFiles("*.*", SearchOption.AllDirectories);
            //    foreach (var f in files2)
            //        File.Delete(f.FullName);
            //    return;
            //}
            countF += new DirectoryInfo(resource.ICON_Grb2).GetFiles("*.grib2", SearchOption.AllDirectories).Count();
            if(countF != 240)
            {
                FileInfo[] files2 = new DirectoryInfo(resource.ICON_Grb2).GetFiles("*.*", SearchOption.AllDirectories);
                foreach (var f in files2)
                    File.Delete(f.FullName);
                files2 = new DirectoryInfo(resource.ICON_Tiff).GetFiles("*.*", SearchOption.AllDirectories);
                foreach (var f in files2)
                    File.Delete(f.FullName);
                files2 = new DirectoryInfo(resource.ICON_DownloadedFiles).GetFiles("*.bzip2", SearchOption.AllDirectories);
                foreach (var f in files2)
                    File.Delete(f.FullName);
                files2 = new DirectoryInfo(resource.ICON_DownloadedFiles).GetFiles("*.grib2", SearchOption.AllDirectories);
                foreach (var f in files2)
                    File.Delete(f.FullName);

                return;
            }
            
            Grib2Geotiff.convert2geotiff(ICON.getFileNamesDownloaded("RAIN_GSP"), 1);
            Grib2Geotiff.convert2geotiff(ICON.getFileNamesDownloaded("RAIN_CON"), 1);
            Grib2Geotiff.convert2geotiff(ICON.getFileNamesDownloaded("TOT_PREC"), 1);
            ICON.createRainRasters();
            ICON.convertCummToSingle(date, run, "RAIN");
            ICON.convertCummToSingle(date, run, "APCP");
            ICON.cummAPCP(date, run);
            ICON.cummRain(date, run);
            ICON.dailyAPCP(date, run);
            ICON.dailyRain(date, run);
            SagaProcess.zonalForPointsICON(date, run);
            SagaProcess.zonalForPointsICON(date, run, "APCP");
            SagaProcess.zonalForPolygonsICON(date, run);
            SagaProcess.zonalForPolygonsICON(date, run, "APCP");
            ICON.testUploadICON(date, run);

            for(int i =0; i < 4; i ++)
                if(run == runs[i])
                {
                    StreamWriter wr = new StreamWriter(logs[i],true);
                    wr.WriteLine();
                    wr.Write(date.Substring(0, 4) + @"-" + date.Substring(4, 2) + @"-" + date.Substring(6, 2));
                    wr.Close();
                    wr.Dispose();
                }

            List<string> emails = new List<string>();
            StreamReader r = new StreamReader(resource.emailAddresses);
            while (r.Peek() != -1)
                emails.Add(r.ReadLine());
            r.Close();
            r = new StreamReader(resource.emailBody);
            string body = r.ReadToEnd();
            int year = Convert.ToInt32(date.Substring(0, 4));
            int month = Convert.ToInt32(date.Substring(4, 2));
            int day = Convert.ToInt32(date.Substring(6, 2));
            int hour = Convert.ToInt32(run);
            PersianCalendar pc = new PersianCalendar();
            DateTime date1 = new DateTime(year, month, day, hour, 0, 0, DateTimeKind.Utc);
            date1 = date1.ToLocalTime();

            string title = "ICON به روزرسانی سامانه هشدار سیل " + pc.GetYear(date1) + "/" + pc.GetMonth(date1) + "/" + pc.GetDayOfMonth(date1) + " ساعت " + pc.GetHour(date1) + ":" + pc.GetMinute(date1);

            body = body.Replace("#####", pc.GetYear(date1) + "/" + pc.GetMonth(date1) + "/" + pc.GetDayOfMonth(date1));
            body = body.Replace("$$", pc.GetHour(date1) + ":" + pc.GetMinute(date1));
            body = body.Replace("^^", "ICON");
            //body = body.Replace("**", "ICON");
            SendEmail.sendEmail(emails, title, body, true);

            FileInfo[] files = new DirectoryInfo(resource.ICON_Grb2).GetFiles("*.*",SearchOption.AllDirectories);
            foreach (var f in files)
                File.Delete(f.FullName);
            files = new DirectoryInfo(resource.ICON_Tiff).GetFiles("*.*", SearchOption.AllDirectories);
            foreach (var f in files)
                File.Delete(f.FullName);
            files = new DirectoryInfo(resource.ICON_DownloadedFiles).GetFiles("*.grib2", SearchOption.AllDirectories);
            foreach (var f in files)
                File.Delete(f.FullName);
        }
    }
}
