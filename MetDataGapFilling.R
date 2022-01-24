#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
# Script to fill the gaps in meteorological data
# Written by `Pushpendra Raghav` 
# University of Alabama (ppushpendra@crimson.ua.edu)
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
if(!require(REddyProc)){
  install.packages("REddyProc")
  library(REddyProc)
}

data <- readRDS('~/Downloads/sample_data.rds')  # change here as required
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#****Gap Filling**** 
# (See paper by Markus Reichstein: "On the separation of net ecosystem exchange into assimilation and ecosystem respiration: review and improved algorithm
#https://onlinelibrary.wiley.com/doi/full/10.1111/j.1365-2486.2005.001002.x); https://rdrr.io/rforge/REddyProc/man/sMDSGapFill.html
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
data$DateTime <- as.POSIXct(data$DateTime, tz="Etc/GMT-6")  # time stamp in POSIX time format
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#--------------Case 1 (when you have ustar data)--------------------------------
#+++ Processing with ustar filtering before
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------

vars <- c('ET','GPP','RH','Rg', 'Tair', 'VPD', 'u', 'Ustar', 'NEE') # Variables for which you want to perform gap-filling
temp <- sEddyProc$new('Any name', data, vars)  # To preserve original data
temp$sSetLocationInfo(LatDeg=35.559815, LongDeg=-98.063128, TimeZoneHour=-6)  #Location of Site
# estimating the thresholds based on the ustar data
(uStarTh <- temp$sEstUstarThreshold()$uStarTh)
temp$sMDSGapFillAfterUstar('ET')
temp$sMDSGapFillAfterUstar('GPP')
temp$sMDSGapFillAfterUstar('RH')
temp$sMDSGapFillAfterUstar('Rg')
temp$sMDSGapFillAfterUstar('Tair')
temp$sMDSGapFillAfterUstar('VPD')
temp$sMDSGapFillAfterUstar('u')
temp$sMDSGapFillAfterUstar('NEE')
temp <- temp$sExportResults() #  <-------- DataFrame contains the gap-filled values and associated quality flags (Enjoy :)

#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------
#---Case 2 (when you do not ustar data and/or do not want ustar filtering for gap-filling)---
#+++ Processing without ustar filtering before
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------

vars <- c('ET','GPP','RH','Rg', 'Tair', 'VPD', 'u', 'NEE') # Variables for which you want to perform gap-filling
temp <- sEddyProc$new('Any name', data, vars)  # To preserve original data
# estimating the thresholds based on the ustar data
temp$sMDSGapFill('ET', FillAll=TRUE)
temp$sMDSGapFill('GPP', FillAll=TRUE)
temp$sMDSGapFill('RH', FillAll=TRUE)
temp$sMDSGapFill('Rg', FillAll=TRUE)
temp$sMDSGapFill('Tair', FillAll=TRUE)
temp$sMDSGapFill('VPD', FillAll=TRUE)
temp$sMDSGapFill('u', FillAll=TRUE)
temp$sMDSGapFill('NEE', FillAll=TRUE)
temp <- temp$sExportResults() #  <-------- DataFrame contains the gap-filled values and associated quality flags (Enjoy :)
#-------------------------------------------------------------------------------
#-------------------------------------------------------------------------------