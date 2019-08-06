############################################################################################################
#            Tag Filter for JSATS Receiver Files converted from CBR description of FAST algorithm
#                 Based on algorithm interpretation by Gabe Singer (GS) and Matt Pagel (MP)
#                               Contact/primary author: Matt Pagel @ UCDavis
# 
#                      Original version coded on 05/16/2017 by GS, Damien Caillaud (DC)
#                       Contributions made by: MP, GS, Colby Hause, DC, Arnold Ammann
#                                 Version 2.7.2. Updated 2019-08-06 by MP
############################################################################################################
#                          Special Note from http://www.twinsun.com/tz/tz-link.htm:
# Numeric time zone abbreviations typically count hours east of UTC, e.g., +09 for Japan and -10 for Hawaii.
#               However, the POSIX TZ environment variable uses the opposite convention.
#         For example, one might use TZ="JST-9" and TZ="HST10" for Japan and Hawaii, respectively.
############################################################################################################

# TODO 20180313: Document max drift between window clusters
# TODO 20180313: process unknown tags too. Figure out if their PRI is near-integer seconds, less than 1hr, 1m (tester/beacon)
# TODO 20180313: for RT files, ignore incoming file name...just read them all in to a big array pre-clean.
# TODO         : implement daily temperature flux <= 4 Kelvin out of 300ish = 1.333% variance in clock rate within a day
# See also TODOs in-line


# setwd("D:/tempssd2/FriantTagging/tankData")
memory.limit(44000)
install.load <- function(package.name)
{
  if (!require(package.name, character.only=TRUE, quietly=TRUE, warn.conflicts=TRUE)) install.packages(package.name, quiet=TRUE)
  library(package.name, character.only=TRUE, quietly=TRUE, warn.conflicts=TRUE)
}

install.only <- function(package) {
  if (!require(package, character.only=TRUE, quietly=TRUE, warn.conflicts=TRUE)) install.packages(package, quiet=TRUE)
}

install.load('tidyverse')
install.load('lubridate')
install.load('data.table')

DoCleanPrePre <- FALSE
DoCleanRT <- FALSE
DoCleanShoreRT <- FALSE
DoCleanERDDAP <- FALSE

DoCleanJST <- FALSE
DoCleanSUM <- FALSE
DoCleanLotek <- TRUE
DoCleanATS <- FALSE

RT_Dir <- "Z:/LimitedAccess/tek_realtime_sqs/data/preprocess/"
RT_File_PATTERN <- "jsats_2016901[1389]_TEK_JSATS_*|jsats_2017900[34]_JSATS_*|jsats_20169020_TEK_JSATS_17607[12]*"
SSRT_Dir <- "D:/tempssd2/SS/filtered/"
ERDDAP_SUBDIR <- ""

RAW_DATA_DIR <- "data/"
TEKNO_SUBDIR <- "Tekno/" # "" or "./" if not in a subdirectory of data directory
ATS_SUBDIR <- "ATS/" # or ""
LOTEK_SUBDIR <- "Lotek/cleaned_with_UCD_tags/"

vTAGFILENAME <- data.table(TagFilename=c("taglists/2019TEfishRice.csv","taglists/qPRIcsvColemanTank.csv", "taglists/2019PCkTags.csv"),PRI=c(5,5,5)) # Individual tag lists with path relative to project working directory. Default PRIs for files when otherwise not specified.

DoSaveIntermediate <- TRUE # (DoCleanJST || DoCleanSUM || DoCleanATS || DoCleanLotek)
DoFilterFromSavedCleanedData <- TRUE || !DoSaveIntermediate # if you're not saving the intermediate, you should do direct processing

# Algorithm constants.
FILTERTHRESH <- 4 # FAST spec: 4. Arnold: 2 for ATS&Tekno, 4 for Lotek
FLOPFACTOR_2.6 <- 0.006 # # FAST spec: 0.006. Arnold: .04*5 = 0.2; UCD per tag, but not per-window: 0.155-0.157
MULTIPATHWINDOW <- 0.2 # FAST spec: 0.156. Arnold: 0.2
COUNTERMAX <- 12 # FAST spec: 12

# add_contextmenu_winpath<-function() { # doesn't actually work properly in RStudio API
#   .rs.addJsonRpcHandler("convert_windows_path_to_R_style",makewinpath)
# }

makewinpath<-function() { # highlight a region in the file source window (up-left), then in console window (down-left) run this func to reverse \s to /s
  try({
    install.load("rstudioapi")
    adc <- rstudioapi::getSourceEditorContext()
    ps <- rstudioapi::primary_selection(adc)
    t <- ps$text
    t <- gsub("\\\\","/",t)
    rstudioapi::modifyRange(ps$`range`,t,adc$id)
    rstudioapi::setSelectionRanges(ps$`range`,adc$id)
  })
}

# if using read.csv rather than fread (data.table), you'll want to 
#   1. read a few entries from each column first
#   2. set the data type accordingly and 
#   3. re-set to character if it's picky
# dathead <- read.csv(i, header=T, nrows=10)
# classes<-sapply(dathead, class)
# classes[names(unlist(list(classes[which(classes %in% c("factor","numeric"))],classes[names(classes) %in% c("time","date","dtf")])))] <- "character"
# dat <- read.csv(i, header=T, colClasses=classes)
# names(dat) # SQSQueue,SQSMessageID,ReceiverID,DLOrder,DetectionDate,TagID,TxAmplitude,TxOffset,TxNBW
# names(dat) <- c("SQSQueue","SQSMessageID","RecSN","DLOrder","dtf","Hex","TxAmplitude","TxOffset","TxNBW")

# Set up custom code for going back and forth with data table and data frame read/write files
data.table.parse<-function (file = "", n = NULL, text = NULL, prompt = "?", keep.source = getOption("keep.source"), 
                            srcfile = NULL, encoding = "unknown") { # needed for dput data.tables (rather than data.frames)
  keep.source <- isTRUE(keep.source)
  if (!is.null(text)) {
    if (length(text) == 0L) return(expression())
    if (missing(srcfile)) {
      srcfile <- "<text>"
      if (keep.source)srcfile <- srcfilecopy(srcfile, text)
    }
    file <- stdin()
  }
  else {
    if (is.character(file)) {
      if (file == "") {
        file <- stdin()
        if (missing(srcfile)) srcfile <- "<stdin>"
      }
      else {
        filename <- file
        file <- file(filename, "r")
        if (missing(srcfile)) srcfile <- filename
        if (keep.source) {
          text <- readLines(file, warn = FALSE)
          if (!length(text)) text <- ""
          close(file)
          file <- stdin()
          srcfile <- srcfilecopy(filename, text, file.mtime(filename), isFile = TRUE)
        }
        else {
          text <- readLines(file, warn = FALSE)
          if (!length(text)) text <- ""
          else text <- gsub("(, .internal.selfref = <pointer: 0x[0-9A-Fa-f]+>)","",text,perl=TRUE)
          on.exit(close(file))
        }
      }
    }
  }
  .Internal(parse(file, n, text, prompt, srcfile, encoding))
}
data.table.get <- function(file, keep.source = FALSE)
  eval(data.table.parse(file = file, keep.source = keep.source))
dtget <- data.table.get # alias

# set up functions for calculating size of folders for progress bars
list.files.size <- function(path = getwd(), full.names=TRUE, nodotdot = TRUE, ignore.case = TRUE, include.dirs = FALSE, ...) { 
  filelist <- data.table(filename=list.files(path=path, full.names=full.names, no.. = nodotdot, ignore.case = ignore.case, include.dirs = include.dirs, ...))
  filelist[,`:=`(size=file.size(filename),ext=str_to_lower(tools::file_ext(filename)))][,tot:=sum.file.sizes(filelist)][,perc:=size/tot]
  return(filelist)
}
sum.file.sizes <- function(DT) {
  return(unlist(DT[,.(x=sum(size))],use.names=F)[1])
}

extractSNfromFN <-function(i) {
  # get it from the first number in the filename after the last \ / or ), spanning a dash if present
  SN <- as.numeric(gsub("([A-Z 0-9/()-]*[/\\()]){0,1}([A-Z_]{0,20}|[()]){0,5}([0-9]{1,6})(-([0-9]{0,4})[0-9]{0,20})?[A-Z._-][^)\t\n-]*$", "\\3\\5", i, ignore.case=TRUE))
  if (is.null(SN) || is.na(SN) || !is.numeric(SN) || SN==9000) { # maybe Lotek from Arnold
    SN <- gsub("([A-Z 0-9/\\()_-]*[/\\()]){0,1}([A-Z_]{0,20}|[()]){0,5}([0-9]{1,6})([_-]([0-9]{0,5})[0-9]{0,20})?[A-Z._-][^)\t\n-]*$", "\\5", i, ignore.case=TRUE)
    if (is.null(SN) || is.na(SN) || !grepl("^[0-9]*$",SN) || SN==9000) SN<-0
  }
  return(SN)
}

recheckTekno <- function() { # not hooked into any other function. Will not automatically run. # only looks for multiple checksums for a tagID
  dat<-data.table()
  for(i in list.files("./data_output/accepted",full.names=TRUE)){
    id<-as.logical(file.info(i)["isdir"])
    if (is.null(id) || is.na(id)) id <- TRUE
    if (id==TRUE) next
    # if it's a dput file, read it back in, but make sure there's no funky memory addresses that were saved by data.table
    if (endsWith(i,'.dput') || endsWith(i,'.txt')) datos <-as.data.table(dtget(i))
    if (endsWith(i,'.csv')) { # if it was written to disk with fwrite, use the faster fread, but make sure to set the datetime stamps
      datos <-fread(i)
      # datos[,dtf:=as.POSIXct(dtf, format = "%Y-%m-%d %H:%M:%OSZ", tz="UTC")] # %OS6Z doesn't seem to work correctly
    }
    if (dat[,.N] > 0) {
      dat<-rbindlist(list(dat, datos))
    } else {
      dat<-datos
    }
  }
  HC<-unique(dat,by=c("Hex", "valid", "fullHex"))[,.(Hex,valid,fullHex)]
  dtvalid<-HC[valid==1]
  invalid<-HC[valid==0]
  setkey(dtvalid, Hex)
  setkey(invalid, Hex)
  invalid[dtvalid,`:=`(tot=.N,realTag=realTag),by=.EACHI,on="Hex",nomatch=NA]
  setcolorder(invalid,c("Hex","fullHex","tot","realTag","valid"))
  uiv<-unique(invalid[,.(Hex,realTag,tot)],by="Hex")
  invalid[uiv,badTagIDs:=lapply(.SD,list),on=.(Hex),by=.EACHI]
  return(unique(invalid,by="Hex")[,.(Hex,realTag,tot,badTagIDs)])
}

lastXdetsPerTag <- function(fileName, num_dets = 10000) { # used for TagEffects tanks (lotek receivers primarily)
  xlmax<-1048576-1 # 1 header line
  fd<-fread(fileName)
  fd[,dtf:=as.POSIXct(dtf,format="%Y-%m-%d %H:%M:%S",tz="Etc/GMT+8")+FracSec]
  setkey(fd,RecSN,Hex,dtf)
  fd2<-fd[,tail(.SD, num_dets), by=.(RecSN,Hex)]
  fd2[,dtf:=as.character(strftime(dtf,tz="Etc/GMT+8",format="%Y-%m-%d %H:%M:%OS6"))]
  if (fd2[,.N] > xlmax) warning(paste0('number of lines in file ',filename,' (',fd2[,.N],') exceeds MS Excel limit of ',xlmax))
  fwrite(fd2, paste0(fileName,'_last',num_dets,'Dets.csv'))
}

find_ePRI <- function(obj) {
  N<-nrow(obj)
  tmp<-data.table(merge.data.frame(x=1:COUNTERMAX,obj))
  itr<-tmp[,ic:=round(twind/x,2)]
  ll <- itr[(ic >= nPRI*0.651 & ic<=nPRI*1.3)]
  setkey(ll,ic)
  rv<-ll[,dist:=abs(nPRI-ic)][,.(tot=.N),by=.(ic,dist)][order(-tot,dist,-ic)][1]
  retval<-data.table(rep(rv$ic,N))
  return(retval)
}

magicFilter2.6 <- function(dat, countermax, filterthresh){
  blankEntry <- data.table(hit=NA, initialHit=NA, isAccepted=FALSE, nbAcceptedHitsForThisInitialHit=0, ePRI=NA)
  setkey(dat,dtf)
  if (dat[,.N] == 0) return(blankEntry)
  dat[,temporary:=as.POSIXct(dtf, format = "%m/%d/%Y %H:%M:%OS", tz="Etc/GMT+8")] # dput file stores datestamp in this basic format
  if (is.na(dat[,.(temporary)][1])) dat[,dtf:=as.POSIXct(dtf, format = "%Y-%m-%dT%H:%M:%OSZ", tz="UTC")] # fwri file stores as UTC in this format ...was in this doc as %S.%OSZ
  else dat[,dtf:=temporary]
  dat[,temporary:=NULL][,winmax:=dtf+((nPRI*1.3*countermax)+1)]
  wind_range <- dat[,.(dtf,winmax,nPRI)]
  setkey(wind_range,dtf,winmax)
  fit <- dat[,.(dup=dtf,hit=dtf)]
  setkey(fit,dup,hit)
  fo_windows_full <- foverlaps(fit,wind_range,maxgap=0,type="within",nomatch=0,mult="all")[,`:=`(twind=hit-dtf,dup=NULL,winmax=NULL)][,c('hitsInWindow','WinID'):=list(.N,.GRP),by=dtf]
  setkey(fo_windows_full, WinID, hit)
  fo_windows <- fo_windows_full[hitsInWindow>=filterthresh]
  if (fo_windows[,.N] == 0) return(blankEntry)
  setkey(fo_windows,WinID)
  fo_windows[,ePRI:=find_ePRI(.SD),by=WinID,.SDcols=c("twind","nPRI")][,nP:=round(twind/ePRI,0)][,`:=`(maxdif=(nP+1)*FLOPFACTOR_2.6,windif=abs(nP*ePRI-twind))]
  windHits <- fo_windows[windif<=maxdif & nP<=countermax]
  if (windHits[,.N] == 0) return(blankEntry)
  setkey(windHits,WinID)
  windHits[,hitsInWindow:=.N,by=WinID][,isAccepted:=(hitsInWindow>=filterthresh)]
  windHits[hitsInWindow==1,hitsInWindow:=0]
  setkey(windHits, WinID, hit)
  earlyReject <- fo_windows_full[!windHits][hitsInWindow==1,hitsInWindow:=0][,`:=`(isAccepted=FALSE,ePRI=NA)][,.(hit,dtf,isAccepted,hitsInWindow,ePRI)]
  LT <- rbind(earlyReject,windHits[,.(hit,dtf,isAccepted,hitsInWindow,ePRI)])
  setkey(LT,dtf,hit)
  setnames(LT,c("dtf","hitsInWindow"),c("initialHit","nbAcceptedHitsForThisInitialHit"))
  return(LT)
}

dataFilter2.6 <- function(dat, filterthresh, countermax) {
  res <- dat[1==0] # copies structure
  iter <- 0
  setkey(dat,Hex)
  titl<-dat[!is.na(RecSN)][1][,RecSN]
  u <- as.list(unique(dat[,.N, by=Hex][N >= filterthresh])[,1])$Hex
  if (length(u)>0) {
    timerbar<-winProgressBar(title=titl, label="Tag", min=0, max=length(u), initial=0)
    for(i in u){
      setWinProgressBar(timerbar,iter,label=i)
      ans <- magicFilter2.6(dat[Hex==i], countermax=countermax, filterthresh)
      setkey(ans,nbAcceptedHitsForThisInitialHit,isAccepted)
      ans2 <- ans[(nbAcceptedHitsForThisInitialHit >= filterthresh)&(isAccepted)]
      if (ans2[,.N]>0) {
        keep<-as.data.table(unique(ans2[,hit]))
        setkey(dat,dtf)
        setkey(keep,x)
        res <- rbind(res, dat[keep][Hex==i]) # Hex==i added 2019-03-19MP as some Loteks had identical timestamps for two detections of two tags
      }
      iter <- iter+1
    } 
    close(timerbar)
  }
  return(res)
}

##filter Loop
filterData <- function(incomingData=NULL) {
  j <- 0
  loopFiles <- function() {
    for(i in list.files("./data_output/cleaned",full.names=TRUE)){
      id<-as.logical(file.info(i)["isdir"])
      if (is.null(id) || is.na(id)) id <- TRUE
      if (id==TRUE) next
      # if it's a dput file, read it back in, but make sure there's no funky memory addresses that were saved by data.table
      if (endsWith(i,'.dput') || endsWith(i,'.txt')) datos <-as.data.table(dtget(i))
      if (endsWith(i,'.fwri')) { # if it was written to disk with fwrite, use the faster fread, but make sure to set the datetime stamps
        datos <-as.data.table(fread(i))
        datos[,dtf:=as.POSIXct(dtf, format = "%Y-%m-%dT%H:%M:%OSZ", tz="UTC")] # %OS6Z doesn't work with as.POSIXct, just in reverse operation
      }
      proces(dat=datos)
    }
  }
  proces <- function(dat) {
    myResults <- dataFilter2.6(dat, filterthresh=FILTERTHRESH, countermax=COUNTERMAX)
    setkey(dat,dtf) # TODO 20180313: we should probably put TagID_Hex and RecSN in the key also
    setkey(myResults,dtf)
    rejecteds <- dat[!myResults]
    myResults[,tt:=strftime(dtf,tz="Etc/GMT+8",format="%Y-%m-%d %H:%M:%S")][,FracSec:=round(as.double(difftime(dtf,tt,tz="Etc/GMT+8",units="secs")),6)][,dtf:=as.character(tt)][,tt:=NULL]
    rejecteds[,tt:=strftime(dtf,tz="Etc/GMT+8",format="%Y-%m-%d %H:%M:%S")][,FracSec:=round(as.double(difftime(dtf,tt,tz="Etc/GMT+8",units="secs")),6)][,dtf:=as.character(tt)][,tt:=NULL]
    j <<- j+1
    if (myResults[,.N]>0) {
      recsn <- myResults[!is.na(RecSN)][1][,RecSN]
    } else {
      recsn <- rejecteds[!is.na(RecSN)][1][,RecSN]
    }
    fwrite(rejecteds, paste0("./data_output/rejected/", sprintf("%04d",j), "_", recsn, "_rejected.csv"))
    fwrite(myResults, paste0("./data_output/accepted/", sprintf("%04d",j), "_", recsn, "_accepted.csv"))
  }
  if (is.null(incomingData)) loopFiles()
  else proces(dat=incomingData)
}


readTags <- function(vTagFileNames=vTAGFILENAME, priColName=c('PRI_nominal','Period_Nom','nPRI','PRI_estimate','ePRI','Period','PRI'), 
                     TagColName=c('TagID_Hex','TagIDHex','TagID','TagCode_Hex','TagCode','CodeID','CodeHex','CodeID_Hex','CodeIDHex','Tag','Code','TagSN','HexCode','Tag ID (hex)','Hex'),
                     grpColName=c("Rel_Group","RelGroup","Rel_group","Release","Group","Origin","StudyID","Owner")) {
  ret <- data.frame(TagID_Hex=character(),nPRI=numeric(),rel_group=character())
  setDT(ret,key="TagID_Hex")
  for (i in 1:nrow(vTagFileNames)) {
    fileName <- as.character(vTagFileNames[i,TagFilename])
    if (!file.exists(fileName)) { next }
    pv <- as.numeric(vTagFileNames[i,PRI])
    tags <- fread(file=fileName, header=TRUE, stringsAsFactors=FALSE, blank.lines.skip=TRUE) # list of known Tag IDs
    heads <- names(tags)
    tcn <- TagColName[which(TagColName %in% heads)[1]] # prioritize the first in priority list
    pcn <- priColName[which(priColName %in% heads)[1]]
    gcn <- grpColName[which(grpColName %in% heads)[1]]
    if (is.null(pcn) || is.na(pcn) || length(pcn)<1 || pcn=="NA") pcn <- NULL
    if (is.null(gcn) || is.na(gcn) || length(gcn)<1 || gcn=="NA") gcn <- NULL
    tags <- tags[,c(tcn,pcn,gcn),with=F]
    if (is.null(pcn)) {
      tags[,nPRI:=pv]
      pcn <- "nPRI"
    }
    if (is.null(gcn)) {
      fileName <- as.character(basename(fileName))
      tags[,rel_group:=fileName]
      gcn <- "rel_group"
    }
    setnames(tags,c(tcn,pcn,gcn),c("TagID_Hex","nPRI","rel_group"))
    setkey(tags,TagID_Hex) # may not be needed anymore
    tags[,nPRI:=as.numeric(nPRI)]
    tags[is.na(nPRI) | nPRI==0, nPRI:=pv]
    tags <- unique(tags[nchar(TagID_Hex)>0], by="TagID_Hex")
    setkey(tags,TagID_Hex)
    ret <- rbindlist(list(ret, tags[!ret]),use.names=TRUE)
  }
  setDT(ret,key="TagID_Hex") # may not be needed anymore
  ret[,TagID_Hex:=substr(as.character(TagID_Hex),1,4)] # drop factors
  return(ret)
}

isDataLine <- function(x) { # non-header
  a <- as.character(x)
  if (is.null(a) || length(a)==0) return(FALSE) # NULL check
  if (nchar(a)==0) return(FALSE) # empty string check
  if (str_count(a,",")<5) return(FALSE) # are there too few fields to actually be data?
  return(!grepl("SiteName",a,fixed = TRUE)) # is it a header line
}

elimNPCandDS <- function(x) {# NPC = nonprinting characters; DS = double-space
  return(gsub("  ", "",gsub("[^[:alnum:] :.,|?&/\\\n-]", "",x)))
}

cleanLinesATS <- function(x) {
  return(elimNPCandDS(x))
}

catLatLon <- function(latitude, longitude) { # both numbers
  return(paste(as.character(latitude),as.character(longitude)))
}
# timestamps given in days since midnight first day of 1900, corrected for 1900 not being leapyear
# converting a number to a POSIXct timestamp seems to assume the numbers are number of seconds from midnight GMT, regardless of specified timezone
# so, first convert the number to a date text string without timezone, then read that in as if it's PST.
lotekDateconvert <- function(x,tz="Etc/GMT+8") {
  return (
    as.POSIXct(
      as.character(
        as.POSIXct(round(x*60*60*24), origin = "1899-12-30", tz = "GMT"),
        format="%Y-%m-%dT%H:%M:%OSZ", tz="GMT" 
      ),format="%Y-%m-%dT%H:%M:%OSZ", tz=tz
    ) )
}

# functionCall, tags, precleanDir, filePattern, wpbTitle,
# headerInFile, leadingBlanks, tz, dtFormat, nacols, foutPrefix,
# inferredHeader, Rec_dtf_Hex_strings, mergeFrac

# return values indicating leading blanks, tz, headers, rec_dtf_Hex
# Lotek = no headers. Sample Data: 43604.3223264,  0.93844, 47710, BA5E,   745 (DateTime, TimeFrac, TagID_Dec, TagID_Hex, SigStr)
# ATS = 10ish rows of crap, followed by
#   Internal, SiteName,,, DateTime, TagCode, Tilt, VBatt, Temp, Pressure, SigStr, Bit Period, Threshold
# ERDDAP CSV = 2 rows of headers, some of which are not always present.
#   time,latitude,longitude,TagCode,general_location,river_km,local_time,Study_ID
#   UTC,degrees_north,degrees_east,,,km,PST,
# ERDDAP CSVP (actual extension CSV) = 1 row of headers, some of which are not always present
#   time (UTC),latitude (degrees_north),longitude (degrees_east),TagCode,general_location,river_km (km),local_time (PST),Study_ID
# ERDDAP CSV0 (actual extension CSV) = 0 row of headers. Presence Would have to be inferred by values. Not currently supported.
CSVsubtype <- function(lfs, i, tags, precleanDir, filePattern = "(*.CSV)|(*.TXT)$", wpbTitle, tz, dtFormat, nacols, foutPrefix, Rec_dtf_Hex_strings, mergeFrac, checkType = "unknown") { # ATS, Lotek, ERDDAP
  fileName <- lfs[i,filename]
  # a bug in fread was encountered with a 522MB file when attempting to only read the first 20 lines with the following
  # fc <- fread(file = fileName, nrows = 20, blank.lines.skip = TRUE, fill=TRUE, check.names=TRUE, verbose=TRUE, showProgress=interactive())
  # switch to readr to get those lines in
  fc <- data.table::fread(paste(readr::read_lines(fileName,n_max=20),collapse ='\n'), sep=',', blank.lines.skip=TRUE, fill=TRUE)
  fhead <- names(fc)
  classes <- as.vector(sapply(fc,class))
  ATSprecolname <- startsWith(fhead[1],'Site Name:')
  ATScolnames <- c('Internal', 'SiteName', 'SN1','SN2','DateTime', 'TagCode', 'Tilt', 'VBatt', 'Temp', 'Pressure', 'SigStr', 'Bit Period', 'Threshold', "Detection") # leaving off two blank unheadered columns
  EC_colnames <- c('time','latitude','longitude','TagCode','general_location','river_km','local_time','Study_ID')
  ECPcolnames <- c('time (UTC)','latitude (degrees_north)','longitude (degrees_east)','TagCode','general_location','river_km (km)','local_time (PST)','Study_ID')
  ECfcolnames <- c('time_utc','latitude','longitude','TagCode','general_location','river_km','local_time','Study_ID')
  LOTcolnames <- c('RecSN','datetime', 'FracSec', 'Dec', 'Hex', 'SigStr') # no header in Lotek file, but these are the names to assign to the columns
  SS_colnames <- c("RecSN","DetOrder","DetectionDate","microsecs","TagID","Amp","FreqShift","NBW","Pressure","WaterTemp","CRC")
  acn = which(ATScolnames %in% fhead)
  ccn = which(EC_colnames %in% fhead)
  pcn = which(ECPcolnames %in% fhead)
  ctbool <- TRUE
  # browser()
  if (checkType=="ATS" || checkType=="unknown") {
    if (ATSprecolname || (length(acn) > 1 && ATScolnames[acn[1]] != "TagCode")) {
      lfs[filename==fileName,c('typeMatch','header','fun','leadingBlanks','dtFormat','hif','rdhs'):=
                          list(TRUE,list(ATScolnames),list(cleanATScsv),0,dtFormat,TRUE,list(Rec_dtf_Hex_strings))] # as.logical(checkType=="ATS")
      ctbool <- FALSE
    } else {
      lfs[filename==fileName,typeMatch:=FALSE]
    }
  }
  if (checkType=="SS" || (checkType=="unknown" && ctbool)) {
    if (length(fhead)==11 && all(startsWith(fhead,"V")) && (identical(classes,c("integer","integer","character","numeric","character","integer","integer","integer","integer","numeric","character")) ||
                                                            identical(classes,c("integer","integer","character","numeric","integer","integer","integer","integer","integer","numeric","character")))) {
      lfs[filename==fileName,c('typeMatch','header','fun','leadingBlanks','dtFormat','hif','rdhs'):=
                          list(TRUE,list(SS_colnames),list(cleanInnerWrap),0,dtFormat,FALSE,list(Rec_dtf_Hex_strings))]
      ctbool <- FALSE
    } else {
      lfs[filename==fileName,typeMatch:=FALSE]
    }
  }
  if (checkType=="Lotek" || (checkType=="unknown" && ctbool)) {
#    if (length(fhead)==5 && class(unlist(fc[,1]))=="numeric" && class(unlist(fc[,3]))=="numeric") { # numeric, numeric, numeric, character/numeric, numeric
    if (length(fhead)==5 && all(startsWith(fhead,"V")) && classes[1]=="numeric" && classes[2]=="numeric" && classes[3]=="integer" && classes[5]=="integer") { # 4 is char, but could be interpretted as numeric
      Rec_dtf_Hex_strings[1] <- NA
      lfs[filename==fileName,c('typeMatch','header','leadingBlanks','fun','dtFormat','hif','rdhs'):=
                          list(TRUE,list(LOTcolnames[-1]),0,list(cleanInnerWrap),dtFormat,FALSE,list(Rec_dtf_Hex_strings))]
      ctbool <- FALSE
    } else if (length(fhead)==6 && all(startsWith(fhead,"V")) && classes[1]=="character" && classes[2]=="numeric" && classes[3]=="numeric" && classes[4]=="integer" && classes[6]=="integer") { # 5 is char, but could be interpretted as numeric
      Rec_dtf_Hex_strings[1] <- "RecSN"
      lfs[filename==fileName,c('typeMatch','header','leadingBlanks','fun','dtFormat','hif','rdhs'):=
                          list(TRUE,list(LOTcolnames),0,list(cleanInnerWrap),dtFormat,FALSE,list(Rec_dtf_Hex_strings))]
      ctbool <- FALSE
    } else {
      lfs[filename==fileName,typeMatch:=FALSE]
    }
  }
  if (checkType=="ERDDAP" || checkType=="ERDAP" || checkType=="ERRDAP" || (checkType=="unknown" && ctbool)) {
    if (length(ccn) > 1 || length(pcn) > 1) {
      if (length(ccn)>length(pcn)) {
        magicWhich <- ccn
        headerlines<- 2
      } else {
        magicWhich <- pcn
        headerlines<- 1
      }
      hd <- ECfcolnames[magicWhich]
      time_types <- c("time_utc","local_time")
      loc_types <- c("general_location","river_km")
      ltsel <- loc_types[which(loc_types %in% hd)]
      if (length(ltsel)==0) ltsel<-c(catLatLon) 
      rdhs1 <- c(ltsel[1],time_types[which(time_types %in% hd)][1],"TagCode")
      dt1 <- if (rdhs1[2]=="time_utc") "%Y-%m-%dT%H:%M:%SZ" else "%Y-%m-%d %H:%M:%S"
      lfs[filename==fileName,c("typeMatch","header","leadingBlanks","rdhs","hif","dtFormat"):=list(TRUE,list(hd),headerlines,list(rdhs1),FALSE,dt1)]
      lfs[filename==fileName,fun:=list(list(cleanInnerWrap))]
      ctbool <- FALSE
    } else {
      if (all(startsWith(fhead,"V")) && length(ccn)==0 && length(pcn)==0 && # no headers
          !(length(classes)==5 && classes[1]=="numeric" && classes[2]=="numeric" && classes[3]=="integer" && classes[5]=="integer") && 
          !(length(classes)==6 && classes[1]=="character" && classes[2]=="numeric" && classes[3]=="numeric" && classes[4]=="integer" && classes[6]=="integer") # exclude Lotek styles
      ) { # TODO: adjust TagCode column (4) to allow for integer type as derived from top 20 rows
        colMatrix <- data.table(pat = list(c("character","numeric","numeric","character","character","numeric","character","character"), # all
                                           c("numeric","numeric","character","character","numeric","character","character"), # missing UTC
                                           c("character","numeric","numeric","character","character","character","character"), # missing rkm
                                           c("character","numeric","numeric","character","character","numeric","character"), # missing localtime or studyid
                                           c("character","numeric","numeric","character","numeric","character","character"), # missing genloc
                                           
                                           c("character","character","character","numeric","character","character"), # missing lat/long
                                           c("numeric","numeric","character","character","character","character"), # missing utc, rkm
                                           c("numeric","numeric","character","numeric","character","character"), # missing utc, genloc
                                           c("numeric","numeric","character","character","numeric","character"), # missing utc, studyid (has localtime)
                                           c("character","numeric","numeric","character","character","numeric"), # missing localtime, studyID
                                           c("character","numeric","numeric","character","character","character"), # missing rkm, either lcltime or studyid (or genloc)
                                           c("character","numeric","numeric","character","numeric","character"), # missing genloc, either lcltime or studyid
                                           
                                           c("character","character","character","character","character"), # missing lat/long, rkm
                                           c("character","character","character","numeric","character"), # missing lat/long, either lcltime or studyid
                                           c("character","character","numeric","character","character"), # missing lat/long, genloc; alternately missing UTC, lat/long
                                           c("character","numeric","numeric","character","character"), # missing rkm, two of localtime, studyid, genloc
                                           c("character","numeric","numeric","character","numeric"), # missing genloc, localtime, studyid
                                           c("numeric","numeric","character","numeric","character"), # missing utc, genloc, studyID (has localtime)
                                           c("numeric","numeric","character","character","character"), # missing utc, rkm, studyID or genloc (has localtime)
                                           
                                           c("character","character","character","character"), # missing lat/long, rkm, either lcltime or studyID
                                           c("character","character","character","numeric"), # missing lat/long, lcltime, studyID
                                           c("character","character","numeric","character"), # missing lat/long, genloc, either lcltime or studyID
                                           c("numeric","numeric","character","character"), # missing utc, genloc, rkm, studyid (has localtime)
                                           
                                           c("character","character","character"), # time, tag, genloc  OR  tag, genloc, lcltime
                                           c("character","numeric","character"), # tag, rkm, localtime
                                           c("character","character","numeric")), # utc, tag, rkm
                                
                                field=list(c('time_utc','latitude','longitude','TagCode','general_location','river_km','local_time','Study_ID'), # all
                                           c('latitude','longitude','TagCode','general_location','river_km','local_time','Study_ID'),
                                           c('time_utc','latitude','longitude','TagCode','general_location','local_time','Study_ID'),
                                           c('time_utc','latitude','longitude','TagCode','general_location','river_km','PST_or_study_ID'),
                                           c('time_utc','latitude','longitude','TagCode','river_km','local_time','Study_ID'),
                                           
                                           c('time_utc','TagCode','general_location','river_km','local_time','Study_ID'),
                                           c('latitude','longitude','TagCode','general_location','local_time','Study_ID'),
                                           c('latitude','longitude','TagCode','river_km','local_time','Study_ID'),
                                           c('latitude','longitude','TagCode','general_location','river_km','local_time'),
                                           c('time_utc','latitude','longitude','TagCode','general_location','river_km'),
                                           c('time_utc','latitude','longitude','TagCode','general_location_or_PST','PST_or_Study_ID'),
                                           c('time_utc','latitude','longitude','TagCode','river_km','PST_or_Study_ID'),
                                           
                                           c('time_utc','TagCode','general_location','local_time','Study_ID'),
                                           c('time_utc','TagCode','general_location','river_km','PST_or_Study_ID'),
                                           if (!is.na(fast_strptime(as.character(fc[1,V1]),"%Y-%m-%dT%H:%M:%SZ"))) { # either UTC, TagCode, rkm, pst, studyID or TagCode, genloc, rkm, localtime, studyID
                                             c('time_utc','TagCode','river_km','local_time','Study_ID') # UTRLS
                                           } else {
                                             c('TagCode','general_location','river_km','local_time','Study_ID') #TGRLS
                                           },
                                           c('time_utc','latitude','longitude','TagCode','general_location_or_PST_or_Study_ID'),
                                           c('time_utc','latitude','longitude','TagCode','river_km'),
                                           c('latitude','longitude','TagCode','river_km','local_time'),
                                           c('latitude','longitude','TagCode','general_location_or_PST','PST_or_Study_ID'),
                                           
                                           c('time_utc','TagCode','general_location','PST_or_Study_ID'),
                                           c('time_utc','TagCode','general_location','river_km'),
                                           c('time_utc','TagCode','river_km','PST_or_Study_ID'),
                                           c('latitude','longitude','TagCode','local_time'),
                                           
                                           if (!is.na(fast_strptime(as.character(fc[1,V1]),"%Y-%m-%dT%H:%M:%SZ"))) { # either time, tag, genloc  OR  tag, genloc, lcltime
                                             c('time_utc','TagCode','general_location')
                                           } else {
                                             c('TagCode','general_location','local_time')
                                           },
                                           c('TagCode','river_km','local_time'),
                                           c('time_utc','TagCode','river_km')),
                                
                                rdhs =list(c('general_location','time_utc','TagCode'),
                                           c('general_location','local_time','TagCode'),
                                           c('general_location','time_utc','TagCode'),
                                           c('general_location','time_utc','TagCode'),
                                           c('river_km','time_utc','TagCode'),
                                           
                                           c('general_location','time_utc','TagCode'), # length 6 block
                                           c('general_location','local_time','TagCode'),
                                           c('river_km','local_time','TagCode'),
                                           c('general_location','local_time','TagCode'),
                                           c('general_location','time_utc','TagCode'),
                                           if (!is.na(fast_strptime(as.character(fc[1,V5]),"%Y-%m-%d %H:%M:%S")) && length(classes)==6) { # location from general_location_or_PST or lat+long
                                             c(list(catLatLon), 'time_utc', 'TagCode') # function call to catLatLon
                                           } else {
                                             c('general_location_or_PST','time_utc','TagCode')
                                           },
                                           c('river_km','time_utc','TagCode'),
                                           
                                           c('general_location','time_utc','TagCode'), # length 5 block
                                           c('general_location','time_utc','TagCode'),
                                           if (!is.na(fast_strptime(as.character(fc[1,V1]),"%Y-%m-%dT%H:%M:%SZ"))) {
                                             c('river_km','time_utc','TagCode')
                                           } else {
                                             c('general_location','local_time','TagCode')
                                           },
                                           if (length(classes)==5) {
                                             c(list(catLatLon),'time_utc','TagCode') # location from lat+long OR general_location_or_PST_or_Study_ID
                                           } else { # just avoiding catLatLon whenever possible.
                                             c('bs','time_utc','TagCode')
                                           },
                                           c('river_km','time_utc','TagCode'),
                                           c('river_km','local_time','TagCode'),
                                           if (length(fhead)==5 && !is.na(fast_strptime(as.character(fc[1,V4]),"%Y-%m-%d %H:%M:%S"))) { # localtime from general_location_or_PST or PST_or_Study_ID; location from general_location_or_PST or lat+long
                                             c(list(catLatLon),'general_location_or_PST','TagCode')
                                           } else {
                                             c('general_location_or_PST','PST_or_Study_ID','TagCode')
                                           },
                                           
                                           c('general_location','time_utc','TagCode'),
                                           c('general_location','time_utc','TagCode'),
                                           c('river_km','time_utc','TagCode'),
                                           if (length(classes)==4) {
                                             c(catLatLon,'local_time','TagCode') # location from lat+long
                                           } else { # avoiding function insertion
                                             c('bs','local_time','TagCode')
                                           },
                                           
                                           if (is.na(fast_strptime(as.character(fc[1,V1]),"%Y-%m-%dT%H:%M:%SZ"))) {
                                             c('general_location','local_time','TagCode')
                                           } else {
                                             c('general_location','time_utc','TagCode')
                                           },
                                           c('river_km','local_time','TagCode'),
                                           c('river_km','time_utc','TagCode')))

        ColMatrix[,TagIndex:=sapply(field, function(x) { which("TagCode"==x)})
                  ][,pat2:=mapply(function(x,y) c(x[seq(1,length.out=y-1)],"integer",x[seq(y+1,length.out=length(x)-y)]),ColMatrix$pat,ColMatrix$TagIndex)]
        centry <- colMatrix[sapply(pat,identical,classes) | sapply(pat2,identical,classes)]
        if (centry[,.N] > 0) {
          rdhs1<-unlist(centry[1][,rdhs])
          if (rdhs1[2]=="time_utc") {
            dtF<-"%Y-%m-%dT%H:%M:%SZ"
            tzz <- "GMT"
          } else { 
            dtF <- "%Y-%m-%d %H:%M:%S"
            tzz <- "Etc/GMT+8"
          }
          lfs[filename==fileName,c('typeMatch','header','rdhs','leadingBlanks','dtFormat','hif','fun'):=list(TRUE,centry[,field][1],list(rdhs1),0,dtF,FALSE,list(cleanInnerWrap))]
          ctbool <- FALSE
        }
      } else {
        lfs[filename==fileName,typeMatch:=FALSE]
      }
    }
  }
  if (ctbool) {
    print(paste("file",fileName,"didn't match",sQuote(checkType),"format"))
    # file didn't match anything
  } else {
    print(paste("file",fileName,"matched",sQuote(checkType),"format"))
  }
  if (lfs[filename==fileName & typeMatch==TRUE, .N] > 0) {
    lfsmatch <- lfs[filename==fileName & typeMatch==TRUE][1]
    # print(lfsmatch[,fun][[1]])
    funct <- unlist(lfsmatch[,fun][[1]])()
    # browser()
    funct(i=fileName, tags, lfsmatch$hif, lfsmatch$leadingBlanks, tz, lfsmatch$dtFormat, nacols, foutPrefix,
                 unlist(lfsmatch$header), unlist(lfsmatch$rdhs), mergeFrac)
  }
  return # (lfs)
}

cleanWrapper <- function(functionCall, tags, precleanDir, filePattern, wpbTitle=NULL, technology="ATS") { # for customized code (ATS)
  lfs<-list.files.size(precleanDir, pattern=filePattern, full.names=TRUE, include.dirs=FALSE)
  # lf<-list.files(precleanDir, pattern=filePattern, full.names=TRUE, include.dirs = FALSE)
  tf<-length(lfs[,filename]) # total files
  if (tf==0) return(F)
  tfs<-lfs[,tot][1] # total file sizes
  if (tfs==0) return(F)
  pb<-winProgressBar(title=wpbTitle, label="Setting up file cleaning...", min=0, max=tfs, initial=0)
  rt<-0 # running size total
  for(i in 1:tf){
    ts <- lfs[i,size]
    if (ts==0) next
    fn <- lfs[i,filename]
    rt <- rt+ts
    id <- as.logical(file.info(fn)["isdir"])
    if (is.null(id) || is.na(id)) id <- TRUE
    if (id==TRUE) next
    lab<-paste0(basename(fn),"\n",i,"/",tf," (",((10000*rt)%/%tfs)/100,"% by size)\n")
    setWinProgressBar(pb,rt,label=lab)
    if (lfs[i,ext]=="csv" || lfs[i,ext]=="txt") {
      CSVsubtype(lfs, i, tags, precleanDir, filePattern = "(*.CSV)|(*.TXT)$", wpbTitle, tz, dtFormat, nacols, foutPrefix, Rec_dtf_Hex_strings, mergeFrac, checkType = technology)
    } else {
      functionCall(fn, tags)
    }
  }
  close(pb)
  # return(T)
}

cleanOuterWrapper <- function(functionCall, tags, precleanDir, filePattern, wpbTitle,
                              headerInFile, leadingBlanks, tz, dtFormat, nacols, foutPrefix,
                              inferredHeader, Rec_dtf_Hex_strings, mergeFrac, technology) {
  # if (grepl(pattern="LOTEK",precleanDir,ignore.case=TRUE)) {
  #   filePattern = paste0("(*.CSV)|",filePattern)
  # }
  # print(filePattern)
  # print(precleanDir)
  # print(wpbTitle)
  lfs<-list.files.size(precleanDir, pattern=filePattern, full.names=TRUE, include.dirs=FALSE)
  # print(lfs)
  tf<-length(lfs[,filename]) # total files
  if (tf==0) return(F)
  tfs<-lfs[,tot][1] # total file sizes
  if (tfs==0) return(F)
  pb<-winProgressBar(title=wpbTitle, label="Setting up file cleaning...", min=0, max=tfs, initial=0)
  rt<-0 # running size total
  for(i in 1:tf){
    ts <- lfs[i,size]
    if (ts==0) next
    fileName <- lfs[i,filename]
    rt <- rt+ts
    id <- as.logical(file.info(fileName)["isdir"])
    if (is.null(id) || is.na(id)) id <- TRUE
    if (id==TRUE) next
    lab<-paste0(basename(fileName),"\n",i,"/",tf," (",((10000*rt)%/%tfs)/100,"% by size)\n")
    setWinProgressBar(pb,rt,label=lab)
    # str(functionCall)
    # print(tags)
    # print(paste("file:",fileName,"    header?:",headerInFile,"    header/blank lines:",leadingBlanks,"    timezone:",tz))
    # print(paste("date format",dtFormat,"    naCols:",toString(nacols),"    output file prefix:",foutPrefix,"    header:",toString(inferredHeader),"    rdh strings:",toString(Rec_dtf_Hex_strings),"    mergefrac:",mergeFrac))
    if (lfs[i,ext]=="csv" || lfs[i,ext]=="txt") {
      CSVsubtype(lfs, i, tags, precleanDir, filePattern = "(*.CSV)|(*.TXT)$", wpbTitle, tz, dtFormat, nacols, foutPrefix, Rec_dtf_Hex_strings, mergeFrac, checkType = technology)
    } else {
      functionCall(i=fileName, tags, headerInFile, leadingBlanks, tz, dtFormat, nacols, foutPrefix,
                   inferredHeader, Rec_dtf_Hex_strings, mergeFrac)
    }
  }
  close(pb)
  # return(T)
}

cleanInnerWrap <-function(...) {
  itercount <- 0
  function(i, tags, headerInFile=T, leadingBlanks=0, tz="GMT", dtFormat="%Y-%m-%dT%H:%M:%OS", nacols=NULL, foutPrefix=".txt", inferredHeader=NULL, Rec_dtf_Hex_strings=c("RecSN","DateTime","TagID_Hex"), mergeFrac=NULL) {
    if (!headerInFile) {
      dathead <- fread(i, header=F, nrows=10, stringsAsFactors=F, skip=leadingBlanks, fill=T, na.strings=c("NA","NULL","Null","null","nan","-nan","N/A",""))
      classes<-sapply(dathead, class)
      classes[names(unlist(list(classes[which(classes %in% c("factor","numeric"))],classes[names(classes) %in% c("time","date","dtf")])))] <- "character"
      dat <- fread(i, header=F, skip=leadingBlanks, fill=T, na.strings=c("NA","NULL","Null","null","nan","-nan","N/A","","-"), colClasses=classes)
      setnames(dat,inferredHeader)
    } else {
      dat <- fread(i, header=T, skip=leadingBlanks, fill=T, na.strings=c("NA","NULL","Null","null","nan","-nan","N/A","","-")) # fread (unlike read.csv) should read dates as character automatically
    }
    if (length(nacols)>0) {
      dat <- na.omit(dat,cols=nacols)
    }
    if (nrow(dat)==0) return(F)
    if (is.na(Rec_dtf_Hex_strings[1]))  {
      SN <- extractSNfromFN(i)
      dat[,RecSN:=SN]
      Rec_dtf_Hex_strings[1]<-"RecSN"
    }
    setnames(dat,Rec_dtf_Hex_strings,c("RecSN","dtf","Hex")) 
    if (dtFormat=="EPOCH") {
      dat[,dtf:=as.character(lotekDateconvert(as.numeric(dtf),tz),format="%Y-%m-%d %H:%M:%S",tz=tz)]
      dtFormat="%Y-%m-%d %H:%M:%OS"
    }
	suppressWarnings(dat[,Dec:=NULL])
    colnamelist<-as.data.table(names(dat))
    if (colnamelist[V1=="valid",.N] > 0) {
      if (dat[valid==0,.N] > 0) {
        dat<-dat[valid!=0]
      }
    }
    # combine the DT and FracSec columns into a single time column
    if (length(mergeFrac)>0) {
      dat[,iznumb:=ifelse(is.na(
        tryCatch(suppressWarnings(as.numeric(eval(as.name(mergeFrac)))))
      ),FALSE,TRUE)
      ][iznumb==T         ,gt1:=(as.numeric(eval(as.name(mergeFrac)))>=1)
        ][gt1==T,fracLead0:=sprintf("%7.6f",as.numeric(eval(as.name(mergeFrac)))/1000000)
          ][iznumb==T & gt1==F,fracLead0:=sprintf("%7.6f",as.numeric(eval(as.name(mergeFrac))))
            ][iznumb==T         ,fracDot:=substring(fracLead0,2)
              ][!is.na(fracDot)   ,newDateTime:=paste0(as.character(dtf),fracDot)
                ][ is.na(fracDot)   ,newDateTime:=as.character(dtf)
                   ]
      dat[,c("iznumb","gt1","fracLead0","dtf"):=NULL]
      setnames(dat, old="newDateTime", new="dtf")
      dat[,c(mergeFrac,"fracDot"):=NULL]
    }
    dat[,fullHex:=Hex]
    dat[nchar(Hex)==9,Hex:=substr(Hex,4,7)]
    setkey(dat, Hex)
    if (nrow(dat)==0) return(F)
    # convert time string into POSIXct timestamp
    dat[,dtf:=as.POSIXct(dtf, format = dtFormat, tz=tz)]
    setkey(tags,TagID_Hex)
    dat2<-dat[tags,nomatch=0] # bring in the nominal PRI (nPRI)
    if (nrow(dat2)==0) {
      print(paste0("no matching tag hits in raw file: ",i))
      print(paste0("First tags file (",as.character(tags[,rel_group][1]),"). All tags regardless of file:"))
      print(as.vector(unique(tags[,TagID_Hex]),mode="character"))
      print("Data file tags")
      print(as.vector(unique(dat[,Hex]),mode="character"))
      print("Were the tags files read in successfully for this study?")
      return(F)
    }
    setkey(dat2, RecSN, Hex, dtf)
    dat2[,tlag:=shift(.SD,n=1L,fill=NA,type="lag"), by=.(Hex,RecSN),.SDcols="dtf"]
    if (nrow(dat2)==0) return(F) # should this be <filterthresh?
    suppressWarnings(dat2[,c("SQSQueue","SQSMessageID","DLOrder","TxAmplitude","TxOffset","TxNBW", "Dec", "SigStr", "fullHex", "CRC", "valid", "TagAmp", "NBW"):=NULL]) # kill warnings of non-existing columns
    # setkey(dat2,RecSN,Hex,dtf) # should still be set
    # calculate tdiff, then remove multipath
    dat4 <- dat2[,tdiff:=difftime(dtf,tlag)][tdiff>MULTIPATHWINDOW | tdiff==0 | is.na(tdiff)]
    if (nrow(dat4)==0) return(F)
    rm(dat2)
    # dat4[dtf==tlag,tlag:=NA] # if we want to set the first lag dif to NA rather than 0 for the first detection of a tag
    # setkey(dat4,RecSN,Hex,dtf) # inherited from dat2
    setkey(dat, RecSN, Hex, dtf)
    keepcols <- unlist(list(names(dat),"nPRI","rel_group")) # use initial datafile columns plus the nPRI column from the taglist file
    dat5 <- unique(dat[dat4,keepcols,with=FALSE]) # too slow? try dat[dat4] then dat5[,(colnames(dat5)-keepcols):=NULL,with=FALSE]
    setkey(dat5,RecSN,Hex,dtf)
    rm(dat4)
    # "Crazy" in Damien's original appears to have just been a check to see if matches previous tag, if not discard result of subtraction
    # Shouldn't be needed for data.table.
    dat5[is.null(RecSN) | is.na(RecSN) | RecSN==9000 | RecSN=="" || RecSN==0,RecSN:=extractSNfromFN(i)]
    SNs<-unique(dat5[,RecSN])
    for(sn in SNs) { # don't trust the initial file to have only a single receiver in it
      itercount <<- itercount + 1
      # fwri format stores timestamps as UTC-based (yyyy-mm-ddTHH:MM:SS.microsZ)
      if (DoSaveIntermediate) fwrite(dat5[RecSN==sn], file = paste0("./data_output/cleaned/", foutPrefix, sprintf("%04d",itercount), "_", sn,  "_cleaned.fwri"))
      if (!DoFilterFromSavedCleanedData) filterData(dat5[RecSN=sn])
    }
    rm(dat5)
  }
}

cleanATSxls <- function() { # just converts an excel file to a "csv" (with a bunch of header lines)
  install.load('readxl')
  function(i, tags) {
    newfn <- paste0(i,".xtmp")
    if (!file.exists(newfn)) {
      dat <- read_excel(i)
      fwrite(dat,file=newfn)  # make sure to check for timezone shifts!
    }
  }
}

cleanATScsv <- function() { # have to figure out how to dovetail this with the other, more sane, files.
  itercount <- 0
  install.load('readr')
  install.load('stringr')
  functionCall<-cleanInnerWrap()
  function(i, tags) { # run the inner function of the enclosure
    SN<-NA
    headr <- read_lines(i,n_max=10) # find serial number somewhere in the top 10 lines
    for (rw in 1:length(headr)) {
      if (startsWith(headr[rw],"Serial Number")) {
        SN<-as.numeric(gsub("Serial Number: ([0-9]{1,8})[^\n\t]*", "\\1", headr[rw]))
        break
      }
    }
    if (is.null(SN) || is.na(SN) || SN==9000 || SN=="") SN <- extractSNfromFN(i) # 9000 is a placeholder in some files, so take from filename instead.
    rl<-read_lines(i)
    gs<-lapply(rl,cleanLinesATS)
    p<-paste(Filter(isDataLine,gs),sep="\n",collapse="\n") # Possibly gs[lapply(gs,isDataLine)] would work too
    dat<-fread(p,blank.lines.skip=TRUE,strip.white=TRUE,na.strings=c("NA","NULL","Null","null","nan","-nan","N/A","","-")) # ,skip="SiteName",
    headers <- c("Internal", "SiteName", "SiteName2", "SiteName3", "dtf", "Hex", "Tilt", "VBatt", "Temp", "Pres", "SigStr",
                 "BitPeriod", "Thresh","Detection")                        # make vector of new headers
    setnames(dat,headers)
    dat[,RecSN:=SN] # apply the value from the header to all the rows in the file
    newfn <- paste0(i,".ctmp")
    fwrite(dat,newfn)
    headerInFile <- TRUE
    leadingBlanks <- 0
    tz <- "Etc/GMT+8"
    dtFormat <- "%m/%d/%Y %H:%M:%OS"
    nacols <- NULL # if this column is null, the row gets cut from the data set. c("Detection","Pres")
    foutPrefix <- "ATS"
    inferredHeader <- NULL
    Rec_dtf_Hex_strings <- c("RecSN","dtf","Hex")
    mergeFrac <- NULL
    functionCall(i=newfn, tags=tags, headerInFile=headerInFile, leadingBlanks=leadingBlanks, tz=tz, dtFormat=dtFormat, foutPrefix=foutPrefix, inferredHeader=inferredHeader, Rec_dtf_Hex_strings=Rec_dtf_Hex_strings, mergeFrac=mergeFrac)
    file.remove(newfn)
    itercount <<- itercount + 1
  }
}

# Load Taglist
# tags<- read.csv(TAGFILENAME, header=T, colClasses="character") # single tag list file. Superseeded.
tags<-readTags(vTAGFILENAME)

# Handle all the well-structured files
# Realtime file formats
if (DoCleanPrePre) {
  headerInFile <- FALSE
  leadingBlanks <- 0
  tz <- "GMT"
  dtFormat <- "%Y-%m-%d %H:%M:%OS"
  nacols <- NULL
  foutPrefix <- "RT_npp"
  inferredHeader <- c("SQSQueue","SQSMessageID","RecSN","DLOrder","DateTime","microsecs","Hex","TxAmplitude","TxOffset","TxNBW","TxCRC")
  Rec_dtf_Hex_strings <- c("RecSN", "DateTime", "Hex")
  mergeFrac <- "microsecs"
  functionCall <- cleanInnerWrap()
  cleanOuterWrapper(functionCall, tags=tags, precleanDir = RT_Dir, filePattern = "*.CSV$", wpbTitle = "Cleaning RT files before preprocessing",
                    headerInFile=headerInFile, leadingBlanks=leadingBlanks, tz=tz, dtFormat=dtFormat, 
                    nacols=nacols, foutPrefix=foutPrefix, inferredHeader=inferredHeader, 
                    Rec_dtf_Hex_strings=Rec_dtf_Hex_strings, mergeFrac=mergeFrac, technology="DC")
}

if (DoCleanRT) { # preprocessed (with DBCnx.py) DataCom detection files
  headerInFile <- TRUE
  leadingBlanks <- 0
  tz <- "GMT"
  dtFormat <- "%Y-%m-%d %H:%M:%OS"
  nacols <- NULL
  foutPrefix <- "RT"
  inferredHeader <- NULL
  Rec_dtf_Hex_strings <- c("ReceiverID","DetectionDate","TagID")
  mergeFrac <- NULL
  funName <- cleanInnerWrap()
  cleanOuterWrapper(funName, tags=tags, precleanDir = RT_Dir, filePattern = RT_File_PATTERN, wpbTitle = "Cleaning Preprocessed Realtime Data",
                    headerInFile=headerInFile, leadingBlanks=leadingBlanks, tz=tz, dtFormat=dtFormat, 
                    nacols=nacols, foutPrefix=foutPrefix, inferredHeader=inferredHeader, 
                    Rec_dtf_Hex_strings=Rec_dtf_Hex_strings, mergeFrac=mergeFrac, technology="DC")
}

if (DoCleanShoreRT){ # ShoreStation files created with
  headerInFile <- FALSE
  leadingBlanks <- 0
  tz <- "GMT"
  dtFormat <- "%Y-%m-%d %H:%M:%OS"
  nacols <- NULL
  foutPrefix <- "SSRT"
  inferredHeader <- c("RecSN","DetOrder","DetectionDate","microsecs","TagID","Amp","FreqShift","NBW","Pressure","WaterTemp","CRC")
  Rec_dtf_Hex_strings <- c("RecSN","DetectionDate","TagID")
  mergeFrac <- "microsecs"
  funName <- cleanInnerWrap()
  cleanOuterWrapper(funName, tags=tags, precleanDir = SSRT_Dir, filePattern = "*.csv$", wpbTitle = "Cleaning Shore Station Data",
                    headerInFile=headerInFile, leadingBlanks=leadingBlanks, tz=tz, dtFormat=dtFormat, 
                    nacols=nacols, foutPrefix=foutPrefix, inferredHeader=inferredHeader, 
                    Rec_dtf_Hex_strings=Rec_dtf_Hex_strings, mergeFrac=mergeFrac, technology="SS")
}

# Tekno autonomous file formats
if (DoCleanJST) {
  headerInFile <- FALSE
  leadingBlanks <- 0
  tz <- "Etc/GMT+8"
  dtFormat <- "%m/%d/%Y %H:%M:%OS"
  nacols <- NULL
  foutPrefix <- "JT"
  inferredHeader <- c("Filename", "RecSN", "DT", "FracSec", "Hex", "CRC", "valid", "TagAmp", "NBW")
  Rec_dtf_Hex_strings <- c("RecSN", "DT", "Hex")
  mergeFrac <- "FracSec"
  funName <- cleanInnerWrap()
  cleanOuterWrapper(funName, tags=tags, precleanDir = paste0(RAW_DATA_DIR,TEKNO_SUBDIR), filePattern = "*.JST$", wpbTitle = "Cleaning Tekno JST files",
                    headerInFile=headerInFile, leadingBlanks=leadingBlanks, tz=tz, dtFormat=dtFormat, 
                    nacols=nacols, foutPrefix=foutPrefix, inferredHeader=inferredHeader, 
                    Rec_dtf_Hex_strings=Rec_dtf_Hex_strings, mergeFrac=mergeFrac, technology="Tekno")
}

if (DoCleanSUM) {
  headerInFile <- TRUE
  leadingBlanks <- 8
  tz <- "Etc/GMT+8"
  dtFormat <- "%m/%d/%Y %H:%M:%OS"
  nacols <- c("Detection")
  foutPrefix <- "SUM"
  inferredHeader <- NULL
  Rec_dtf_Hex_strings <- c("Serial Number","Date Time","TagCode")
  mergeFrac <- NULL
  funName <- cleanInnerWrap()
  cleanOuterWrapper(funName, tags=tags, precleanDir = paste0(RAW_DATA_DIR,TEKNO_SUBDIR), filePattern = "*.SUM$", wpbTitle = "Cleaning SUM Files",
                    headerInFile=headerInFile, leadingBlanks=leadingBlanks, tz=tz, dtFormat=dtFormat, 
                    nacols=nacols, foutPrefix=foutPrefix, inferredHeader=inferredHeader, 
                    Rec_dtf_Hex_strings=Rec_dtf_Hex_strings, mergeFrac=mergeFrac, technology="Tekno")
}

# Lotek Autonomous. Files are already pre-filtered for tags from their "JST" file format
if (DoCleanLotek) {
  headerInFile <- FALSE
  leadingBlanks <- 0
  tz <- "Etc/GMT+8"
  dtFormat <- "EPOCH"
  nacols <- NULL
  foutPrefix <- "LT"
  inferredHeader <- c("datetime", "FracSec", "Dec", "Hex", "SigStr")
  Rec_dtf_Hex_strings <- c(NA, "datetime", "Hex")
  mergeFrac <- "FracSec"
  funName <- cleanInnerWrap()
  cleanOuterWrapper(funName, tags=tags, precleanDir = paste0(RAW_DATA_DIR,LOTEK_SUBDIR), filePattern = "(*.LO_CSV)|(*.TXT)|(*.CSV)$", wpbTitle = "Cleaning LoTek LO_CSV and TXT files",
                    headerInFile=headerInFile, leadingBlanks=leadingBlanks, tz=tz, dtFormat=dtFormat, 
                    nacols=nacols, foutPrefix=foutPrefix, inferredHeader=inferredHeader, 
                    Rec_dtf_Hex_strings=Rec_dtf_Hex_strings, mergeFrac=mergeFrac, technology="Lotek")
}

if (DoCleanERDDAP) {
  headerInFile <- TRUE
  leadingBlanks <- 0 # potentially 2 line header
  tz <- "GMT"
  dtFormat <- "%Y-%m-%dT%H:%M:%SZ"
  nacols <- NULL
  foutPrefix <- "ERD"
  inferredHeader <- NULL
  Rec_dtf_Hex_strings <- c("general_location","time (UTC)","TagCode")
  mergeFrac <- NULL
  funName <- cleanInnerWrap()
  cleanOuterWrapper(funName, tags=tags, precleanDir = paste0(RAW_DATA_DIR,ERDDAP_SUBDIR), filePattern = "(*.CSV)|(*.CSVP)|(*.CSV0)|(*.ER_CSV)$", wpbTitle = "Cleaning ERDDAP Files",
                    headerInFile=headerInFile, leadingBlanks=leadingBlanks, tz=tz, dtFormat=dtFormat, 
                    nacols=nacols, foutPrefix=foutPrefix, inferredHeader=inferredHeader, 
                    Rec_dtf_Hex_strings=Rec_dtf_Hex_strings, mergeFrac=mergeFrac, technology="ERDDAP")
}

# Last, do the files with less structure (needing customized code)
# ATS autonomous
if (DoCleanATS) {
  funName <- cleanATSxls() # converts Excel files to a CSV-style file
  cleanWrapper(funName, tags, precleanDir = paste0(RAW_DATA_DIR,ATS_SUBDIR), filePattern = "*.XLS[X]?$", wpbTitle = "Converting ATS XLS(x) files")
  funName <- cleanATScsv() # clean ATS CSVs
  cleanWrapper(funName, tags, precleanDir = paste0(RAW_DATA_DIR,ATS_SUBDIR), filePattern = "(*.XTMP)|(*.CSV)|(*.ATS_CSV)$", wpbTitle = "Cleaning ATS CSV files")
}

###Do the filtering loop
if (DoFilterFromSavedCleanedData) {
  filterData()
}
