using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DataManager
{
    class DataDownload
    {
        double resoloution;
        DateTime time;
        string year;
        string month;
        string day;
        string run;
        
        public double Resoloution
        {
            get
            {
                return resoloution;
            }
            set
            {
                if (value == 0.25 || value == 0.5 || value == 0.13)
                    resoloution = value;
                else
                    throw new NotSupportedException("Error: Only 0.25, 0.5 and 0.13 Deg are supported.");
            }
        }
        public DateTime Time
        {
            get
            {
                return time;
            }
            set
            {
                time = value;
            }
        }
        public string Year
        {
            get
            {
                return year;
            }
            set
            {
                if (Convert.ToString(value).Length != 4)
                    throw new FormatException("Error: Year should be in YYYY format.");
                else
                    year = value;
            }
        }

        public string Month
        {
            get
            {
                return month;
            }
            set
            {
                if (Convert.ToString(value).Length != 2)
                    throw new FormatException("Error: Month should be in MM format.");
            }
        }
        public string Day
        {
            get
            {
                return day;
            }
            set
            {
                if (Convert.ToString(value).Length != 2)
                    throw new FormatException("Error: Day should be in DD format");
                else
                    day = value;
            }
        }
        public string Run
        {
            get
            {
                return run;
            }
            set
            {
                if (value != "00" || value != "06" || value != "12" || value != "18")
                    throw new FormatException("Erorr: Only 00,06,12,18 are acceptable as run parameter.");
                else
                    run = value;
            }
        }
        DataDownload()
        {
            resoloution = 0.0;
            time = new DateTime();
            year = "";
            month = "";
            day = "";
            run = "";
        }
       DataDownload(double _res, DateTime _time)
        {
            try
            {
                Resoloution = _res;
                Time = _time;
                run = _time.Hour.ToString(); 
            }
            catch(Exception e)
            {
                Console.WriteLine(e.Message);
                //TODO: Add log message.
            }
        }
    }
}
