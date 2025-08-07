#------------------------------------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------------------------------------------
load_or_install <- function(pkg) {
  if (!require(pkg, character.only = TRUE)) {
    install.packages(pkg, dependencies = TRUE)
    library(pkg, character.only = TRUE)
  }
}
# List of required packages
packages <- c(
  "archive",
  "readr",
  "dplyr",
  "fasttime",
  "foreach",
  "doParallel",
  "zip",
  "stringr",
  "REddyProc",
  "readxl"
)
suppressMessages(sapply(packages, load_or_install))
#------------------------------------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------------------------------------------
# Important Functions
get_utc_offset <- function(site,badm.path){
  
  badm.tmp.ls <-
    dir(badm.path)[which(grepl(site, dir(badm.path)))]
  if (length(badm.tmp.ls) == 0) {
    return(NULL)
  }
  data.in <- read_excel(
    paste0(badm.path, badm.tmp.ls),
    sheet = 1,
    col_names = T,
    na = "-9999"
  )
  
  data.in$GROUP_ID <- as.character(data.in$GROUP_ID)
  data.in$VARIABLE_GROUP <- as.character(data.in$VARIABLE_GROUP)
  data.in$DATAVALUE <- as.character(data.in$DATAVALUE)
  data.in$SITE_ID <- as.character(data.in$SITE_ID)
  
  data.out <- badm.extract(data.in = data.in, sel.grp = "GRP_UTC_OFFSET")
  
  data.out <- data.out[!duplicated(data.out$SITE_ID), ]
  
  return(data.out$UTC_OFFSET) 
}
###################################################################################################
badm.extract <- function(data.in, sel.grp) {
  
  if (length(which(data.in$VARIABLE_GROUP == sel.grp)) > 0) {
    
    data.in3 <- data.in[data.in$VARIABLE_GROUP == sel.grp, ]
    data.in3$VARIABLE <- factor(data.in3$VARIABLE)
    var.ls <- levels(data.in3$VARIABLE)
    
    data.in3$GROUP_ID <- as.numeric(data.in3$GROUP_ID)
    entry.ls <- tapply(data.in3$GROUP_ID, data.in3$GROUP_ID, mean)
    
    entry.ls.loc.i <- tapply(data.in3$SITE_ID,
                             data.in3$GROUP_ID,
                             function(x)
                               paste(x[1]))
    
    #entry.ls %in% data.out3$GROUP_ID
    
    data.out3 <- data.frame(
      GROUP_ID = tapply(data.in3$GROUP_ID,
                        data.in3$GROUP_ID,
                        function(x)
                          paste(x[1])),
      SITE_ID = tapply(data.in3$SITE_ID,
                       data.in3$GROUP_ID,
                       function(x)
                         paste(x[1])),
      stringsAsFactors = F
    )
    
    for (j in 1:length(var.ls)) {
      data.in3.tmp <-
        data.in3[data.in3$VARIABLE == paste(var.ls[j]), c("GROUP_ID", "DATAVALUE")]
      data.out3 <- merge.data.frame(data.out3,
                                    data.in3.tmp,
                                    by = "GROUP_ID", all = T)
      
      colnames(data.out3)[ncol(data.out3)] <- paste(var.ls[j])
    }
    
    data.out3 <- data.out3[order(data.out3$SITE_ID), ]
    
  } else{
    data.out3 <- NULL
    
  }
  
  return(data.out3)
}

select_columns <- function(df, prefix, preferred_contains = "PI", exclude_suffix = "QC") {
  cols <- colnames(df)
  exact_cols <- cols[startsWith(cols, prefix) & !endsWith(cols, exclude_suffix)]
  exact_cols <- exact_cols[
    nchar(exact_cols) <= nchar(prefix) | grepl(paste0(prefix, "_"), exact_cols)
  ]
  exact_preferred <- exact_cols[grepl(preferred_contains, exact_cols)]
  if (length(exact_preferred) > 0) return(exact_preferred)
  if (length(exact_cols) > 0) return(exact_cols)
  prefixed_cols <- cols[startsWith(cols, paste0(prefix, "_")) & !endsWith(cols, exclude_suffix)]
  preferred_cols <- prefixed_cols[grepl(preferred_contains, prefixed_cols)]
  if (length(preferred_cols) > 0) return(preferred_cols)
  return(prefixed_cols)
}
#------------------------------------------------------------------------------------------------------------------------
#------------------------------------------------------------------------------------------------------------------------
# Basic Settings
setwd("/mnt/wwn-0x5000c500e5ba5923/Research_Work/2025/AGU_Chapman/AmeriFlux")
badm.path <- "/mnt/wwn-0x5000c500e5ba5923/Research_Work/2025/AGU_Chapman/AmeriFlux/BADM"
site_info <- read_csv("/mnt/wwn-0x5000c500e5ba5923/Research_Work/2025/AGU_Chapman/All_AmeriFlux_Sites_info.csv",show_col_types = FALSE)
dir_1 <- "/mnt/wwn-0x5000c500e5ba5923/Research_Work/2025/AGU_Chapman/AmeriFlux/RAW_Data_AmeriFlux_Base"
zip_files <- list.files(dir_1, pattern = "\\.zip$", full.names = TRUE)
process_file <- function(zip_file){
  tryCatch({
    zip_contents <- archive::archive(zip_file)
    flx_file <- zip_contents$path[grepl("(BASE_HH|BASE_HR).*\\.csv$", zip_contents$path)][1]
    site_name <- str_split(flx_file, "_", simplify = TRUE)[1, 2]
    cat(paste0("----Processing site ", site_name, "----\n"))
    con <- archive::archive_read(zip_file, file = flx_file)
    df <- read_csv(con, skip = 2, show_col_types = FALSE)
    df[df == -9999] <- NA
    df$DateTime <- strptime(df$TIMESTAMP_START, format="%Y%m%d%H%M")
    df <- df %>% arrange(DateTime)
    rownames(df) <- NULL
    NETRAD_cols <- select_columns(df, "NETRAD")
    if (length(NETRAD_cols) == 0) {
      SW_IN_cols  <- select_columns(df, "SW_IN")
      SW_OUT_cols <- select_columns(df, "SW_OUT")
      LW_IN_cols  <- select_columns(df, "LW_IN")
      LW_OUT_cols <- select_columns(df, "LW_OUT")
      
      if (all(sapply(list(SW_IN_cols, SW_OUT_cols, LW_IN_cols, LW_OUT_cols), length) > 0)) {
        SW_IN  <- rowMeans(df[, SW_IN_cols], na.rm = TRUE)
        SW_OUT <- rowMeans(df[, SW_OUT_cols], na.rm = TRUE)
        LW_IN  <- rowMeans(df[, LW_IN_cols], na.rm = TRUE)
        LW_OUT <- rowMeans(df[, LW_OUT_cols], na.rm = TRUE)
        df$NETRAD <- SW_IN - SW_OUT + LW_IN - LW_OUT
      }
    } else {
      df$NETRAD <- rowMeans(df[, NETRAD_cols], na.rm = TRUE)
    }
    
    G_cols <- select_columns(df, "G")
    if (length(G_cols) == 0){
      df$G <- 0
    }
    le_cols <- select_columns(df, "LE")
    h_cols  <- select_columns(df, "H")
    gpp_cols  <- select_columns(df, "GPP")
    vpd_cols  <- select_columns(df, "VPD")
    ta_cols  <- select_columns(df, "TA")
    pa_cols  <- select_columns(df, "PA")
    SRad_cols  <- select_columns(df, "SW_IN")
    ws_cols  <- select_columns(df, "WS")
    ustar_cols  <- select_columns(df, "USTAR")
    fc_cols  <- select_columns(df, "FC")
    sc_cols  <- select_columns(df, "SC")
    swc_cols  <- select_columns(df, "SWC")
    
    df$LE <- rowMeans(df[, le_cols], na.rm = TRUE)
    df$H  <- rowMeans(df[, h_cols],  na.rm = TRUE)
    df$VPD  <- rowMeans(df[, vpd_cols],  na.rm = TRUE)
    df$Tair  <- rowMeans(df[, ta_cols],  na.rm = TRUE)
    df$Pa  <- rowMeans(df[, pa_cols],  na.rm = TRUE)
    df$Rg  <- rowMeans(df[, SRad_cols],  na.rm = TRUE)
    df$WS  <- rowMeans(df[, ws_cols],  na.rm = TRUE)
    df$Ustar  <- rowMeans(df[, ustar_cols],  na.rm = TRUE)

    if (length(swc_cols) == 0){
      df$SWC <- NA
    } else{
      df$SWC  <- rowMeans(df[, swc_cols],  na.rm = TRUE)
    }
    # NEE Partitioning using EddyProc
    nee_cols  <- select_columns(df, "NEE")
    if (length(nee_cols) == 0){
      df$FC  <- rowMeans(df[, fc_cols],  na.rm = TRUE)
      if (length(sc_cols) == 0){
        df$SC <- 0
      } else{
        df$SC  <- rowMeans(df[, sc_cols],  na.rm = TRUE)
      }
      df$NEE = df$FC + df$SC 
    } else{
      df$NEE  <- rowMeans(df[, nee_cols],  na.rm = TRUE)
    }
    utc_offset = get_utc_offset(site_name, badm.path)
    df$DateTime <- as.POSIXct(df$DateTime, tz=paste0("Etc/GMT",utc_offset)) 
    vars <- c('NEE', 'Rg', 'Tair', 'VPD', 'LE', 'H', 'NETRAD', 'G',  'Pa', 'WS', 'Ustar')
    site_lat <- site_info$LOCATION_LAT[site_info$SITE_ID==site_name]
    site_long <- site_info$LOCATION_LONG[site_info$SITE_ID==site_name]
    #---First ensure that DateTime is continuous with no gaps in between
    nStepsPerDay <- 24*60/as.numeric(difftime(df$DateTime[2], df$DateTime[1], units = 'mins'))
    if(nStepsPerDay == 48){
      time_start <- "00:00:00"
      time_end <- "23:30:00"
    } else {
      time_start <- "00:00:00"
      time_end <- "23:00:00"
    }
    date_start <- as.POSIXct(paste0(date(df$DateTime[1]), time_start))
    date_end <- as.POSIXct(paste0(date(df$DateTime[nrow(df)]), time_end))
    seq_DateTime <- seq(from=date_start, by=24*60/nStepsPerDay*60, to=date_end)
    temp <- data.frame(DateTime = seq_DateTime)
    df <- left_join(temp,df,by='DateTime')
    # Initalize R5 reference class sEddyProc for post-processing of eddy data with the variables needed later
    EProc  <- sEddyProc$new('Any name', df, vars) 
    EProc $sSetLocationInfo(LatDeg=site_lat, LongDeg=site_long, TimeZoneHour=as.numeric(utc_offset))
    (uStarTh <- EProc$sEstUstarThreshold()$uStarTh)
    EProc $sMDSGapFillAfterUstar('NEE')
    EProc $sMDSGapFill('Rg')
    EProc $sMDSGapFill('Tair')
    EProc $sMDSGapFill('VPD')
    EProc $sMDSGapFillAfterUstar('LE')
    EProc $sMDSGapFillAfterUstar('H')
    EProc $sMDSGapFill('NETRAD')
    EProc $sMDSGapFill('G')
    EProc $sMDSGapFill('Pa')
    EProc $sMDSGapFill('WS')
    EProc $sMDSGapFill('Ustar')
    
    # NEE Flux partitioning
    EProc$sApplyUStarScen( EProc$sMRFluxPartition )
    # Storing the results in a csv file
    FilledEddyData  <- EProc$sExportResults()
    final_df <- data.frame(
      DateTime = df$DateTime,
      LE_F_MDS = FilledEddyData$LE_uStar_f, LE_F_MDS_QC = FilledEddyData$LE_uStar_fqc,
      H_F_MDS = FilledEddyData$H_uStar_f, H_F_MDS_QC = FilledEddyData$H_uStar_fqc,
      NETRAD_F_MDS = FilledEddyData$NETRAD_f, NETRAD_F_MDS_QC = FilledEddyData$NETRAD_fqc,
      G_F_MDS = df$G_f, G_F_MDS_QC = FilledEddyData$G_fqc,
      VPD_F_MDS = FilledEddyData$VPD_f, VPD_F_MDS_QC = FilledEddyData$VPD_fqc,
      Tair_F_MDS = FilledEddyData$Tair_f, Tair_F_MDS_QC = FilledEddyData$Tair_fqc,
      Pa_F_MDS = FilledEddyData$Pa_f, Pa_F_MDS_QC = FilledEddyData$Pa_fqc,
      NEE_F_MDS = FilledEddyData$NEE_uStar_f, NEE_F_MDS_QC = FilledEddyData$NEE_uStar_fqc,
      Rg_F_MDS = FilledEddyData$Rg_f, Rg_F_MDS_QC = FilledEddyData$Rg_fqc,
      WS_F_MDS = FilledEddyData$WS_f, WS_F_MDS_QC = FilledEddyData$WS_fqc,
      Ustar_F_MDS = FilledEddyData$Ustar_f, Ustar_F_MDS_QC = FilledEddyData$Ustar_fqc,
      GPP = FilledEddyData$GPP_uStar_f, GPP_QC = FilledEddyData$GPP_uStar_fqc,
      SW_IN_POT = FilledEddyData$PotRad_uStar, RECO = FilledEddyData$Reco_uStar,
      SWC = df$SWC
    )
    if (length(gpp_cols) > 0){
      final_df$GPP_PI  <- rowMeans(df[, gpp_cols],  na.rm = TRUE)
    }
    # Save
    write.csv(final_df, paste0("AMF_BASE_EddyPro_Processed_Data/AMF_", site_name, "_BASE.csv"))
    return(NULL)
  }, error=function(error_message) {
    message(error_message)
    return(NULL)
  })
}
# Run in parallel
cl <- makeCluster(64)
registerDoParallel(cl)
out <- foreach(file = zip_files, .combine = rbind, .packages = c("archive", "readr", "stringr", "data.table", "REddyProc", "lubridate", "dplyr", "readxl")) %dopar% {
  process_file(file)
}
