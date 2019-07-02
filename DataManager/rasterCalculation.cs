using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using OSGeo.GDAL;
using OSGeo.OSR;
using OSGeo.OGR;
using System.IO;

namespace DataManager
{
    static class rasterCalculation
    {
        public static bool convertPrate2APCP(string input, string output, double? minValue = 0.0)
        {
            
            int secCycle = (Convert.ToInt16(input.Substring(input.Length - 7, 3)) % 6 ) * 3600; // 3600 * n * h
            if (secCycle == 0)
                secCycle = 6 * 3600;
            try
            {                                                                                                   
                using (var drv = Gdal.GetDriverByName("GTiff"))
                {
                    if (drv == null)
                        throw new Exception("Can't get GTiff driver.");
                    using (var ds = Gdal.Open(input, Access.GA_ReadOnly))
                    {
                        if (ds == null)
                            throw new Exception("Can't open GDAL dataset: " + input);
                        var options = new[] { "" };
                        using (var newDataset = drv.CreateCopy(output, ds, 0, options, null, "Sample_Data"))
                        {
                            if (newDataset == null)
                                throw new Exception("Can't create destination dataset: " + output);
                            ds.Dispose();
                            using (var band = newDataset.GetRasterBand(1))
                            {
                                double noData = -999000000;
                                band.SetNoDataValue(-999000000);
                                var sizeX = band.XSize;
                                var numLines = band.YSize;

                                for (var line = 0; line < numLines; line++)
                                {
                                    var scanline = new float[sizeX];
                                    var cplReturn = band.ReadRaster(0, line, sizeX-1, 1, scanline, sizeX, 1, 0, 0);
                                    if (cplReturn != CPLErr.CE_None)
                                        throw new Exception("band.ReadRaster failed: " + Gdal.GetLastErrorMsg());
                                    var outputline = new List<float>();
                                    foreach (var f in scanline)
                                    {
                                        var pixelValue = f;
                                        if ((float)f != (float)noData)
                                        {
                                            pixelValue = f * secCycle;
                                            if (minValue.HasValue)
                                                pixelValue = (float)Math.Max(pixelValue, minValue.GetValueOrDefault());
                                            if (pixelValue < 0)
                                                pixelValue = 0;
                                        }

                                        outputline.Add(pixelValue);
                                    }
                                    cplReturn = band.WriteRaster(0, line, sizeX-1, 1, outputline.ToArray(), sizeX, 1, 0, 0);
                                    if (cplReturn != CPLErr.CE_None)
                                        throw new Exception("band.WriteRaster failed: " + Gdal.GetLastErrorMsg());
                                    band.FlushCache();
                                    newDataset.FlushCache();
                                }
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                throw new Exception("Can't open GDAL dataset: " + input, e);
            }
            
                return true;
        }
        public static bool substraction(string first, string second, string output, double? minValue = 0.0)
        {
            
            try
            {
                using (var drv = Gdal.GetDriverByName("GTiff"))
                {
                    if (drv == null)
                        throw new Exception("Can't get GTiff driver.");

                    using (var firstInput = Gdal.Open(first, Access.GA_ReadOnly))
                    using (var secondInput = Gdal.Open(second, Access.GA_ReadOnly))
                    {
                        if (firstInput == null)
                            throw new Exception("Can't open GDAL dataset: " + first);
                        if (secondInput == null)
                            throw new Exception("Can't open GDAL dataset: " + second);
                        var options = new[] { "" };
                        using (var newDataset = drv.CreateCopy(output, firstInput, 0, options, null, "Sample_Sub"))
                        {
                            if (newDataset == null)
                                throw new Exception("Could not create destination dataset.");
                            using (var bandFirst = firstInput.GetRasterBand(1))
                            using (var bandSecond = secondInput.GetRasterBand(1))
                            using (var bandOut = newDataset.GetRasterBand(1))
                            {
                                double noData = -999000000;
                                bandOut.SetNoDataValue(-999000000);
                                var sizeX = bandOut.XSize;
                                var numLines = bandOut.YSize;
                                for(var i = 0; i < numLines; i ++)
                                {
                                    var FirstScanline = new float[sizeX];
                                    var SecondScanline = new float[sizeX];

                                    var cplReturn = bandFirst.ReadRaster(0, i, sizeX, 1, FirstScanline, sizeX, 1, 0, 0);
                                    if (cplReturn != CPLErr.CE_None)
                                        throw new Exception("band.ReadRaster failed: " + Gdal.GetLastErrorMsg());
                                    cplReturn = bandSecond.ReadRaster(0, i, sizeX, 1, SecondScanline, sizeX, 1, 0, 0);
                                    if (cplReturn != CPLErr.CE_None)
                                        throw new Exception("band.ReadRaster failed: " + Gdal.GetLastErrorMsg());
                                    var outputLine = new List<float>();
                                    for(var j = 0; j < sizeX; j ++)
                                    {
                                        double pixelValue;
                                        if (FirstScanline[j] != noData && SecondScanline[j] != noData)
                                        {
                                            pixelValue = FirstScanline[j] - SecondScanline[j];
                                            if (pixelValue < 0)
                                                pixelValue = 0;
                                            if (minValue.HasValue)
                                                pixelValue = Math.Max(pixelValue, minValue.GetValueOrDefault());
                                        }
                                        else
                                            pixelValue = noData;
                                        outputLine.Add((float)pixelValue);
                                    }
                                    cplReturn = bandOut.WriteRaster(0, i, sizeX, 1, outputLine.ToArray(), sizeX, 1, 0, 0);
                                    if (cplReturn != CPLErr.CE_None)
                                        throw new Exception("band.WriteRaster failed: " + Gdal.GetLastErrorMsg());
                                    bandOut.FlushCache();
                                    newDataset.FlushCache();
                                }
                            }
                        }
                    }
                }
            }
            catch(Exception e)
            {
                throw new Exception("Can't open GDAL datasets: " + first + " + " + second, e);
            }
            
            return true;
        }
        public static bool convertAPCP2RAIN(string inputApcp, string inputTemp, string output)
        {
            try
            {
                using (var dvr = Gdal.GetDriverByName("GTiff"))
                {
                    if (dvr == null)
                        throw new Exception("Can't Get GDAL GTiff driver.");

                    //using (var zeroGrid = Gdal.Open(resource.ZeroRaster13, Access.GA_ReadOnly))
                    using (var apcp = Gdal.Open(inputApcp, Access.GA_ReadOnly))
                    using (var temp = Gdal.Open(inputTemp, Access.GA_ReadOnly))
                    {
                        if (apcp == null)
                            throw new Exception("Can't open GDAL dataset: " + apcp);
                        if (temp == null)
                            throw new Exception("Can't open GDAL dataset: " + temp);
                        var options = new[] { "" };
                        using (var newDataset = dvr.CreateCopy(output, temp, 0, options, null, "Sample_Data"))
                        {
                            if (newDataset == null)
                                throw new Exception("Could not create destination dataset. ");
                            newDataset.GetRasterBand(1).Fill(0.0, 0.0);
                            using (var apcpBand = apcp.GetRasterBand(1))
                            using (var tempBand = temp.GetRasterBand(1))
                            using (var outBand = newDataset.GetRasterBand(1))
                            //using (var zeroBand = zeroGrid.GetRasterBand(1))
                            {
                                if (apcpBand == null)
                                    throw new Exception("Could not get apcp Band.");
                                if (tempBand == null)
                                    throw new Exception("Could not get tempreture Band.");
                                if (outBand == null)
                                    throw new Exception("Could not get output Band.");
                                //if (zeroBand == null)
                                    //throw new Exception("Could not get zero Raster band.");

                                double noData = -999000000;
                                outBand.SetNoDataValue(-999000000);
                                var sizeX = outBand.XSize;
                                var numLines = outBand.YSize;
                                for (int i = 0; i < numLines; i++)
                                {
                                    var tempScanLine = new float[sizeX];
                                    var apcpScanLine = new float[sizeX];
                                    //var zeroScanLine = new float[sizeX];
                                    var cplReturn = tempBand.ReadRaster(0, i, sizeX, 1, tempScanLine, sizeX, 1, 0, 0);
                                    if (cplReturn != CPLErr.CE_None)
                                        throw new Exception("band.ReadRaster failed: " + Gdal.GetLastErrorMsg());
                                    cplReturn = apcpBand.ReadRaster(0, i, sizeX, 1, apcpScanLine, sizeX, 1, 0, 0);
                                    if (cplReturn != CPLErr.CE_None)
                                        throw new Exception("band.ReadRaster failed: " + Gdal.GetLastErrorMsg());
                                    //cplReturn = zeroBand.ReadRaster(0, i, sizeX, 1, zeroScanLine, sizeX, 1, 0, 0);
                                    //if (cplReturn != CPLErr.CE_None)
                                        //throw new Exception("band.ReadRaster failed: " + Gdal.GetLastErrorMsg());

                                    var outputLine = new List<float>();
                                    for (var j = 0; j < sizeX; j++)
                                    {
                                        double pixelValue = 0.0;
                                        if (tempScanLine[j] != noData)
                                        {
                                            if (tempScanLine[j] < 0.0)
                                                pixelValue = 0.0;
                                            else
                                                pixelValue = apcpScanLine[j];
                                        }
                                        else
                                            pixelValue = noData;
                                        outputLine.Add((float)pixelValue);
                                    }
                                    cplReturn = outBand.WriteRaster(0, i, sizeX, 1, outputLine.ToArray(), sizeX, 1, 0, 0);
                                    if (cplReturn != CPLErr.CE_None)
                                        throw new Exception("band.WriteRaster failed: " + Gdal.GetLastErrorMsg());
                                    outBand.FlushCache();
                                    newDataset.FlushCache();
                                    //zeroGrid.FlushCache();
                                    //zeroGrid.Dispose();
                                }
                            }
                        }
                    }
                }
            }
            catch(Exception e)
            {
                throw new Exception(e.Message);
            }
            
        return true;
        }
        public static bool average(List<string> input, string output)
        {
            
            using (var dvr = Gdal.GetDriverByName("GTiff"))
            {
                if (dvr == null)
                    throw new Exception("Can not get GTiff driver. ");
                List<Dataset> inputRasters = new List<Dataset>();

                foreach (var f in input)
                    inputRasters.Add(Gdal.Open(f, Access.GA_ReadOnly));

                foreach (var f in inputRasters)
                    if (f == null)
                        throw new Exception("Could not open input Datasets.");
                string[] options = { "" };

                using (var newDataset = dvr.CreateCopy(output, inputRasters.ElementAt(0), 0, options, null, "Average_Data"))
                {
                    if (newDataset == null)
                        throw new Exception("Could not Create destination Dataset.");

                    List<Band> inputBands = new List<Band>();

                    foreach (var f in inputRasters)
                        inputBands.Add((f.GetRasterBand(1)));

                    foreach (var f in inputBands)
                        if (f == null)
                            throw new Exception("Could not get raster bands.");

                    using (var outBand = newDataset.GetRasterBand(1))
                    {
                        double noData = -999000000;
                        outBand.SetNoDataValue(-999000000);
                        var sizeX = outBand.XSize;
                        var numLines = outBand.YSize;

                        for (int i = 0; i < numLines; i++)
                        {
                            List<float[]> scanLines = new List<float[]>();

                            for (int j = 0; j < inputBands.Count; j++)
                                scanLines.Add(new float[sizeX]);

                            var outputLine = new List<float>();

                            for (int j = 0; j < inputBands.Count; j++)
                            {
                                var cpl = inputBands[j].ReadRaster(0, i, sizeX-1, 1, scanLines[j], sizeX, 1, 0, 0);
                                if (cpl != CPLErr.CE_None)
                                    throw new Exception("Could not read raster band values.");
                            }

                            for (int j = 0; j < sizeX; j++)
                            {
                                double pixelValue = 0.0;
                                double sum = 0.0;
                                for (int k = 0; k < inputBands.Count; k++)
                                    if (scanLines.ElementAt(k)[j] != noData)
                                        sum += scanLines.ElementAt(k)[j];

                                pixelValue = sum / inputBands.Count;
                                outputLine.Add((float)pixelValue);
                            }
                            var cplReturn = outBand.WriteRaster(0, i, sizeX-1, 1, outputLine.ToArray(), sizeX, 1, 0, 0);
                            if (cplReturn != CPLErr.CE_None)
                                throw new Exception("band.WriteRaster failed: " + Gdal.GetLastErrorMsg());
                            outBand.FlushCache();
                            newDataset.FlushCache();
                        }
                    }
                }
            }
            
            return true;
        }
        public static bool sum(List<string> input, string output)
        {
            
            using (var dvr = Gdal.GetDriverByName("GTiff"))
            {
                if (dvr == null)
                    throw new Exception("Can not get GTiff driver. ");
                List<Dataset> inputRasters = new List<Dataset>();

                foreach (var f in input)
                    inputRasters.Add(Gdal.Open(f, Access.GA_ReadOnly));

                foreach (var f in inputRasters)
                    if (f == null)
                        throw new Exception("Could not open input Datasets.");
                string[] options = { "" };

                using (var newDataset = dvr.CreateCopy(output, inputRasters.ElementAt(0), 0, options, null, "Average_Data"))
                {
                    if (newDataset == null)
                        throw new Exception("Could not Create destination Dataset.");

                    List<Band> inputBands = new List<Band>();

                    foreach (var f in inputRasters)
                        inputBands.Add((f.GetRasterBand(1)));

                    foreach (var f in inputBands)
                        if (f == null)
                            throw new Exception("Could not get raster bands.");

                    using (var outBand = newDataset.GetRasterBand(1))
                    {
                        double noData = -999999999;
                        outBand.SetNoDataValue(noData);
                        var sizeX = outBand.XSize;
                        var numLines = outBand.YSize;

                        for (int i = 0; i < numLines; i++)
                        {
                            List<float[]> scanLines = new List<float[]>();

                            for (int j = 0; j < inputBands.Count; j++)
                                scanLines.Add(new float[sizeX]);

                            var outputLine = new List<float>();

                            for (int j = 0; j < inputBands.Count; j++)
                            {
                                var cpl = inputBands[j].ReadRaster(0, i, sizeX, 1, scanLines[j], sizeX, 1, 0, 0);
                                if (cpl != CPLErr.CE_None)
                                    throw new Exception("Could not read raster band values.");
                            }

                            for (int j = 0; j < sizeX; j++)
                            {
                                double pixelValue = 0.0;
                                double sum = 0.0;
                                for (int k = 0; k < inputBands.Count; k++)
                                    if(scanLines.ElementAt(k)[j] > 0)
                                        sum += scanLines.ElementAt(k)[j];
                                pixelValue = sum;
                                outputLine.Add((float)pixelValue);
                            }
                            var cplReturn = outBand.WriteRaster(0, i, sizeX, 1, outputLine.ToArray(), sizeX, 1, 0, 0);
                            if (cplReturn != CPLErr.CE_None)
                                throw new Exception("band.WriteRaster failed: " + Gdal.GetLastErrorMsg());
                            outBand.FlushCache();
                            newDataset.FlushCache();
                        }
                    }
                }
            }
            
                return true;
        }
        public static bool setNoData(string input, string output)
        {
            
            using (var dvr = Gdal.GetDriverByName("GTiff"))
            {
                if (dvr == null)
                    throw new Exception("Error. Could not Load GeoTiff driver...");
                var inputRaster = Gdal.Open(input, Access.GA_ReadOnly);
                if (inputRaster == null)
                    throw new Exception("Error. Could not open input raster...");
                string[] options = { "" };
                using (var newDataset = dvr.CreateCopy(output, inputRaster, 0, options, null, "Sample_NoData"))
                {
                    if (newDataset == null)
                        throw new Exception("Error. Could not create destination dataset...");
                    using (var outBand = newDataset.GetRasterBand(1))
                        outBand.SetNoDataValue(-999000000);
                } 
            }
            
            return true;
        }
    }
}