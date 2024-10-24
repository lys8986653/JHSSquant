### 日期: 20210330
### 作者: HuanfengDong
### 该文件是函数头文件，储存各种通用函数（本版本为简化版）
### =================================================================================== ### 
### 发布日期: 2023-10-19 更新内容: 1、简化GetTradingDay函数；2、新增GetTradingDayStart，便于获取月初、月中、季初日期；3、odbcConnect全部改为OdbcSqlConnect


# 新odbc连接方法 不需要设置windows odbc
OdbcSqlConnect <- function(DBN="FutResearch",UID="",PWD="",Server = "192.168.8.38"){
  # =================================================================================== #
  # 函数功能：根据数据库名，用户名、密码与服务器，直接建立odbc访问连接
  #
  # 输入变量说明：DBN为数据库，UID,Pwd为账户密码，Sever为服务器IP地址
  # 
  # 输出变量说明：建立的数据库访问连 
  #
  # 示例：OdbcSqlConnect(ODBC, User, Pwd, SQL.Address)
  # =================================================================================== #
  
  return(odbcDriverConnect(paste0("Driver={SQL Server};Server=",Server,";Database=",DBN,";UID=",UID,";Pwd=",PWD,";")))
}


## SecuCode补全6
SecuCodeComplete <- function(SecuCode){
  SecuCode <- as.character(SecuCode)
  SecuCode <- format(as.numeric(SecuCode),scientific = F)
  SecuCode.Complete <- str_pad(SecuCode,width = 6,pad = "0",side = "left")
  SecuCode.Complete <- str_replace_all(SecuCode.Complete,"[ ]",'0')
  return(SecuCode.Complete)
}

## SecuCode转SecuAbbr
SecuCode2SecuAbbr <- function(F.Ary.SecuCode){
  F.Ary.SecuCode <- SecuCodeComplete(F.Ary.SecuCode)
  F.Ary.SecuCode.Str <- paste(F.Ary.SecuCode,collapse = "','")
  F.Ary.SecuCode.Str <- paste("'",F.Ary.SecuCode.Str,"'",sep='')
  SQLSTR <- ("SELECT 
      SecuCode
      ,[SecuAbbr]
             FROM [JYDB].[dbo].[SecuMain]
             where SecuCode in (F.Ary.SecuCode.Str) and SecuCategory = 1 and SecuMarket in (83,90) and ListedState in (1,3,5)")
  SQLSTR <- sub('F.Ary.SecuCode.Str', F.Ary.SecuCode.Str, SQLSTR, fixed=TRUE)
  F.DT.Secu <- as.data.table(GetMainDataGeneral(SQLSTR))
  F.DT.Secu[, SecuCode:= SecuCodeComplete(SecuCode)]
  F.DT.Secu[, SecuAbbr:=as.character(SecuAbbr)]
  F.Ary.SecuAbbr <- F.DT.Secu[match(F.Ary.SecuCode, SecuCode)]$SecuAbbr
  return(F.Ary.SecuAbbr)
}

## 日期转换
Int2Date <- function(IntDate){
  return(as.Date(paste(substr(IntDate,1,4),substr(IntDate,5,6),substr(IntDate,7,8),sep='-')))
}

## 时间转换
Int2ITime <- function(IntTime){
  IntTime <- str_pad(as.character(IntTime),width = 6, side = 'left', pad='0')
  return(as.ITime(paste(substr(IntTime,1,2),substr(IntTime,3,4),substr(IntTime,5,6),sep=':')))
}

## 指数InnerCode转中文名
IndustryInnerCode2CName <- function(InnerCode){
 SQLSTR <- ("
            SELECT
      [IndexCode]
      ,[IndustryStandard]
      ,CII.[IndustryCode]
,FirstIndustryCode
,FirstIndustryName
      ,[IndexState]
  FROM [JYDB].[dbo].[LC_CorrIndexIndustry] as CII,
  [JYDB].[dbo].[CT_IndustryType] as IT
  where  CII.IndustryCode = IT.IndustryNum and  IndexCode in (15285,15286,15287,15288,15289,15290,15291,15292
                         ,15293,15294,15295,15296,15297,15298,15299,15300,15301,15302
             ,15303,15304,15305,15306,15307,15308,15309,15310,15311,15312
             ,15313) 
             ")
  Data.CorrIndexIndustry <- GetSQLDataNew(SQLSTR, ODBC.Server, SQL.User, SQL.Pwd) # 勘误zhipuwang 原为ODBC.StockResearch
  Data.CorrIndexIndustry[, FirstIndustryName := as.character(FirstIndustryName)]
  Industry_InnerCode <-c(39144,46,3145,4978,15285,15286,15287,15288,15289,15290,15291,15292
                         ,15293,15294,15295,15296,15297,15298,15299,15300,15301,15302
                         ,15303,15304,15305,15306,15307,15308,15309,15310,15311,15312
                         ,15313)
  Industry_name <- c('ZZ1000','SZ50','HS300','ZZ500',Data.CorrIndexIndustry[ IndexCode %in% Industry_InnerCode]$FirstIndustryName)
  Index <- match(InnerCode, Industry_InnerCode)
  return(Industry_name[Index])
  
}

## 行业中文名转InnerCode
IndustryName2InnerCode <- function(Ind.Name){
  SQLSTR <- ("
            SELECT
             [IndexCode]
             ,[IndustryStandard]
             ,CII.[IndustryCode]
             ,FirstIndustryCode
             ,FirstIndustryName
             ,[IndexState]
             FROM [JYDB].[dbo].[LC_CorrIndexIndustry] as CII,
             [JYDB].[dbo].[CT_IndustryType] as IT
             where  CII.IndustryCode = IT.IndustryNum and  IndexCode in (15285,15286,15287,15288,15289,15290,15291,15292
             ,15293,15294,15295,15296,15297,15298,15299,15300,15301,15302
             ,15303,15304,15305,15306,15307,15308,15309,15310,15311,15312
             ,15313) 
             ")
  Data.CorrIndexIndustry <- GetSQLDataNew(SQLSTR, ODBC.StockResearch, SQL.User, SQL.Pwd)
  Data.CorrIndexIndustry[, FirstIndustryName := as.character(FirstIndustryName)]
  Industry_InnerCode <-c(39144,46,3145,4978,15285,15286,15287,15288,15289,15290,15291,15292
                         ,15293,15294,15295,15296,15297,15298,15299,15300,15301,15302
                         ,15303,15304,15305,15306,15307,15308,15309,15310,15311,15312
                         ,15313)
  Industry_name <- c('ZZ1000','SZ50','HS300','ZZ500',Data.CorrIndexIndustry[ IndexCode %in% Industry_InnerCode]$FirstIndustryName)
  Index <- match(Ind.Name, Industry_name)
  return(Industry_InnerCode[Index])
  
}

## 一级中信行业代码转为中文名
FirstIndustryCode2CName <- function(IndCode){
  ZXStandard <- 37 # 中信行业标准  
  SQLSTR <- ("
            SELECT 
      [Standard]
      ,[IndustryNum]
      ,[Classification]
      ,[IndustryCode]
      ,[IndustryName]
      ,[SectorCode]
      ,[FirstIndustryCode]
      ,[FirstIndustryName]
      ,[SecondIndustryCode]
      ,[SecondIndustryName]
      ,[ThirdIndustryCode]
      ,[ThirdIndustryName]
      ,[FourthIndustryCode]
      ,[FourthIndustryName]
  FROM [JYDB].[dbo].[CT_IndustryType] 
             ")
  Data.IndustryType <- GetSQLDataNew(SQLSTR, ODBC.StockResearch, SQL.User, SQL.Pwd)
  Data.IndustryType <- as.data.table(Data.IndustryType)
  Data.IndustryType <- Data.IndustryType[Standard == ZXStandard]
  Data.IndustryType[,IndustryName:=as.character(IndustryName)]
  Data.IndustryType[,FirstIndustryName:=as.character(FirstIndustryName)]
  Data.IndustryType[,SecondIndustryName:=as.character(SecondIndustryName)]
  Data.IndustryType[,ThirdIndustryName:=as.character(ThirdIndustryName)]
  Data.IndustryType[,FourthIndustryName:=as.character(FourthIndustryName)]
  Data.IndustryType[,IndustryCode:=as.numeric(as.character(IndustryCode))]
  Data.IndustryType[,FirstIndustryCode:=as.numeric(as.character(FirstIndustryCode))]
  Data.IndustryType[,SecondIndustryCode:=as.numeric(as.character(SecondIndustryCode))]
  Data.IndustryType[,ThirdIndustryCode:=as.numeric(as.character(ThirdIndustryCode))]
  Data.IndustryType[,FourthIndustryCode:=as.numeric(as.character(FourthIndustryCode))]
  Industry_name <- Data.IndustryType[match(IndCode,IndustryCode)]$IndustryName
  return(Industry_name)
}

## 一级中信行业中文名转为代码
FirstIndustryCName2Code <- function(IndName){
  ZXStandard <- 37 # 中信行业标准  
  SQLSTR <- ("
             SELECT 
             [Standard]
             ,[IndustryNum]
             ,[Classification]
             ,[IndustryCode]
             ,[IndustryName]
             ,[SectorCode]
             ,[FirstIndustryCode]
             ,[FirstIndustryName]
             ,[SecondIndustryCode]
             ,[SecondIndustryName]
             ,[ThirdIndustryCode]
             ,[ThirdIndustryName]
             ,[FourthIndustryCode]
             ,[FourthIndustryName]
             FROM [JYDB].[dbo].[CT_IndustryType] 
             ")
  Data.IndustryType <- GetSQLDataNew(SQLSTR, ODBC.StockResearch, SQL.User, SQL.Pwd)
  Data.IndustryType <- as.data.table(Data.IndustryType)
  Data.IndustryType <- Data.IndustryType[Standard == ZXStandard]
  Data.IndustryType[,IndustryName:=as.character(IndustryName)]
  Data.IndustryType[,FirstIndustryName:=as.character(FirstIndustryName)]
  Data.IndustryType[,SecondIndustryName:=as.character(SecondIndustryName)]
  Data.IndustryType[,ThirdIndustryName:=as.character(ThirdIndustryName)]
  Data.IndustryType[,FourthIndustryName:=as.character(FourthIndustryName)]
  Data.IndustryType[,IndustryCode:=as.numeric(as.character(IndustryCode))]
  Data.IndustryType[,FirstIndustryCode:=as.numeric(as.character(FirstIndustryCode))]
  Data.IndustryType[,SecondIndustryCode:=as.numeric(as.character(SecondIndustryCode))]
  Data.IndustryType[,ThirdIndustryCode:=as.numeric(as.character(ThirdIndustryCode))]
  Data.IndustryType[,FourthIndustryCode:=as.numeric(as.character(FourthIndustryCode))]
  Industry_code <- Data.IndustryType[match(IndName,IndustryName)]$IndustryCode
  return(Industry_code)
  
}



## 给个股行情数据进行行业分,合并行业分类
Combine.ExgIndustry <- function(Data.DailyQuote, Data.ExgIndustry, StandardTag = 37, IndustryLevel = 1){
  #交易
  Ary.TradingDay <- unique(Data.DailyQuote$TradingDay)
  #选择细分等级
  Data.ExgIndustry.Sub <- Data.ExgIndustry[Standard == StandardTag ]#中信行业分类
  #转Factor到Numeric不能直接转，要先转character
  Data.ExgIndustry.Sub$SecuCode <- as.character(Data.ExgIndustry.Sub$SecuCode)
  Data.ExgIndustry.Sub$SecuCode <- as.numeric(Data.ExgIndustry.Sub$SecuCode)
  Data.ExgIndustry.Sub$FirstIndustryCode <- as.character(Data.ExgIndustry.Sub$FirstIndustryCode)
  Data.ExgIndustry.Sub$SecondIndustryCode <- as.character(Data.ExgIndustry.Sub$SecondIndustryCode)
  Data.ExgIndustry.Sub$ThirdIndustryCode <- as.character(Data.ExgIndustry.Sub$ThirdIndustryCode)
  Data.ExgIndustry.Sub$FourthIndustryCode <- as.character(Data.ExgIndustry.Sub$FourthIndustryCode)
  Data.ExgIndustry.Sub$FirstIndustryCode <- as.numeric(Data.ExgIndustry.Sub$FirstIndustryCode)
  Data.ExgIndustry.Sub$SecondIndustryCode <- as.numeric(Data.ExgIndustry.Sub$SecondIndustryCode)
  Data.ExgIndustry.Sub$ThirdIndustryCode <- as.numeric(Data.ExgIndustry.Sub$ThirdIndustryCode)
  Data.ExgIndustry.Sub$FourthIndustryCode <- as.numeric(Data.ExgIndustry.Sub$FourthIndustryCode)
  
  Data.ExgIndustry.Sub <- switch(IndustryLevel
                                 ,Data.ExgIndustry.Sub[,.(TradingDay,SecuCode,CancelDate,FirstIndustryCode,FirstIndustryName)]#一级分
                                 , Data.ExgIndustry.Sub[,.(TradingDay,SecuCode,CancelDate,SecondIndustryCode,SecondIndustryName)]#二级分类
                                 , Data.ExgIndustry.Sub[,.(TradingDay,SecuCode,CancelDate,ThirdIndustryCode,ThirdIndustryName)]#三级分类
                                 , Data.ExgIndustry.Sub[,.(TradingDay,SecuCode,CancelDate,FourthIndustryCode,FourthIndustryName)]#四级分类
  )
  
  names(Data.ExgIndustry.Sub) <- c('TradingDay','SecuCode','CancelDate','IndustryCode','IndustryName')
  setkey(Data.ExgIndustry.Sub,TradingDay,SecuCode)
  
  
  StartDate <- min(Data.DailyQuote$TradingDay)
  Data.ExgIndustry.Sub <- Data.ExgIndustry.Sub[CancelDate > StartDate  | is.na(CancelDate)]
  Data.ExgIndustry.Sub[TradingDay < StartDate, TradingDay:=StartDate]
  Data.ExgIndustry.Sub[,TradingDay:=GetLastTradingDay(TradingDay, Ary.TradingDay)]#Data.ExgIndustry中有节假日数,要变为节假日前第一个交易日
  Data.ExgIndustry.Sub <- Data.ExgIndustry.Sub[!is.na(TradingDay)]
  Data.DailyQuote <- merge(Data.DailyQuote, Data.ExgIndustry.Sub, by= c('TradingDay','SecuCode'),all=T)
  #填充数据
  Data.DailyQuote[,IndustryCode:= na.locf(IndustryCode,na.rm = F),by=SecuCode]
  Data.DailyQuote[,IndustryName:= na.locf(IndustryName,na.rm = F),by=SecuCode]
  Data.DailyQuote <- Data.DailyQuote[!is.na(ClosePrice)]
  setnames(Data.DailyQuote,'IndustryCode',switch(IndustryLevel
                                                 ,'FirstIndustryCode'
                                                 ,'SecondIndustryCode'
                                                 ,'ThirdIndustryCode'
                                                 ,'FourthIndustryCode'
                                                 ))
  setnames(Data.DailyQuote,'IndustryName',switch(IndustryLevel
                                                 ,'FirstIndustryName'
                                                 ,'SecondIndustryName'
                                                 ,'ThirdIndustryName'
                                                 ,'FourthIndustryName'
  ))
  return(Data.DailyQuote)
}



## 获取当季数据
GetReportData <- function(F.DT.Data, TargetVariable, Former = 0){
  # =================================================================================== #
  # 函数功能：获取当季原始数
  #
  # 输入变量说明：F.DT.Data是data.table，必须包含TradingDay, SecuCode, EndDate, TargetVariable
  #
  # 输出变量说明：输出F.DT.Data包含TradingDay, SecuCode，EndDate, OriginalTargetVariable,  CaculatedTargetVariable
  #
  # 注意：如果缺少某一季度数据，则算出为NA。原则上来讲，同一天发布多个季报和季报修正的，
  # 应当只保留最新一期的季报。如果有新的季报修正，那么在它还是最新的季报情况下，给予显示，否则无视
  # =================================================================================== #
  # F.DT.Data <- copy(Data.BalanceSheetAll)
  # TargetVariable <- 'CashEquivalents'
  F.DT.Data <- F.DT.Data[,c("TradingDay","SecuCode","EndDate",TargetVariable), with=F]
  setnames(F.DT.Data, TargetVariable, "TargetVariable")
  F.DT.Data[, TargetVariable := na.fill(TargetVariable, 0)]
  F.DT.Data <- F.DT.Data[!is.na(TradingDay)]
  setkey(F.DT.Data, TradingDay, SecuCode)
  ### 得到需要的数据主表
  {
    ## 把同一天发多个季报的只留最近一期季
    F.DT.Data.Last <- copy(F.DT.Data)
    setorder(F.DT.Data.Last, -EndDate)
    F.DT.Data.Last[,Rowid:=rowidv(.SD,cols=c('TradingDay','SecuCode'))]
    F.DT.Data.Last <- F.DT.Data.Last[Rowid==1]
    F.DT.Data.Last[,Rowid:=NULL]
    setkey(F.DT.Data.Last, TradingDay, SecuCode)
    ## 只保留最新季报信
    F.DT.Data.Last[, MaxEndDate := RollMaxNoLeast(EndDate, Inf), by=SecuCode]
    F.DT.Data.Last <- F.DT.Data.Last[EndDate == MaxEndDate]
    F.DT.Data.Last[, MaxEndDate := NULL]
    cat("TargetNrow: ", nrow(F.DT.Data.Last),"\n")
  }
  if(Former > 0){
    F.DT.Data.Last[!is.na(EndDate), EndDate.Former := EndDate]
    ## 向前推EndDate
    for(i in 1:Former){
      F.DT.Data.Last[!is.na(EndDate), EndDate.Former:= as.Date(GetFormerEndDate(EndDate.Former))]
    }
    F.DT.Data.Last <- merge(F.DT.Data.Last, F.DT.Data[, .(TradingDay.Former = TradingDay, SecuCode, EndDate.Former = EndDate , TargetVariable.Former = TargetVariable)]
                            , by = c('SecuCode','EndDate.Former')
                            , all.x = T)
    F.DT.Data.Last[, MinTradingDay := pmin(TradingDay, TradingDay.Former, na.rm = T)] # 最小值用于排
    F.DT.Data.Last[, TradingDay := pmax(TradingDay, TradingDay.Former, na.rm = T)] # 对于在本季报发布后再更新上一期季报的，设置TradingDay.Former为NA，为未来信息
    setkey(F.DT.Data.Last, TradingDay, SecuCode)
    ## 对于多个历史季报，选取最近的一
    ## 去重，同日期发布两份报告，主要来自历史；以及取最新的
    cat("TargetNrow: ", nrow(F.DT.Data.Last),"\n")
    setorder(F.DT.Data.Last, -EndDate, -MinTradingDay)
    F.DT.Data.Last[,Rowid:=rowidv(.SD,cols=c('TradingDay','SecuCode'))]
    F.DT.Data.Last <- F.DT.Data.Last[Rowid==1]
    F.DT.Data.Last[,Rowid:=NULL]
    setkey(F.DT.Data.Last, TradingDay, SecuCode)
    cat("TargetNrow: ", nrow(F.DT.Data.Last),"\n")
    F.DT.Data.Last[, MaxEndDate := RollMaxNoLeast(EndDate, Inf), by=SecuCode]
    F.DT.Data.Last <- F.DT.Data.Last[EndDate == MaxEndDate]
    F.DT.Data.Last[, MaxEndDate := NULL]
    cat("TargetNrow: ", nrow(F.DT.Data.Last),"\n")
    setkey(F.DT.Data.Last, TradingDay, SecuCode)
    ## 把TradingDay.Former为NA,代表不知何时发布的信,Variable设为NA
    F.DT.Data.Last[is.na(TradingDay.Former), TargetVariable.Former := NA]
    F.DT.Data.Last[, TradingDay.Former := NULL]
    F.DT.Data.Last <- F.DT.Data.Last[,.(TradingDay, SecuCode, EndDate = EndDate.Former, TargetVariable = TargetVariable.Former,MinTradingDay)]
  }
  cat("FinalNrow: ", nrow(F.DT.Data.Last), "\n")
  ## 现值减上季值为单季值，如果EndDate3月则为一季报不用
  F.DT.Data.Last[, OriginalTargetVariable := TargetVariable]
  F.DT.Data.Last[, TargetVariable := TargetVariable]
  setnames(F.DT.Data.Last, 'TargetVariable', TargetVariable)
  setnames(F.DT.Data.Last, 'OriginalTargetVariable', paste0('Original', TargetVariable))
  F.DT.Data.Last[, LastTargetVariable1 := NULL]
  F.DT.Data.Last[, LastEndDate := NULL]
  F.DT.Data.Last <- F.DT.Data.Last[,c("TradingDay", "SecuCode", "EndDate",TargetVariable), with=F]
  return(F.DT.Data.Last)
}


## 报告型数据转交易日数
ReportData2TradingDayData <- function(Ary.TradingDay, F.DT.Data, TargetVariable, Ary.SecuCode = NULL){
  # F.Data.TradingDay <- GetTradingDay(ODBC.StockResearch, SQL.User, SQL.Pwd, EndDate = Sys.Date())
  load(paste0(Dir.Data, 'TradingDay.Rdata'))
  F.Data.TradingDay <- Data.TradingDay
  F.DT.Data[, TradingDay:=GetLastTradingDay(TradingDay, F.Data.TradingDay)]
  setorder(F.DT.Data, -EndDate)
  F.DT.Data[,Rowid:=rowidv(.SD,cols=c('TradingDay','SecuCode'))]
  F.DT.Data <- F.DT.Data[Rowid==1]
  F.DT.Data[,Rowid:=NULL]
  setkey(F.DT.Data, TradingDay, SecuCode)
  F.DT.Data <- F.DT.Data[!is.na(TradingDay)]
  setnames(F.DT.Data, TargetVariable, 'TargetVariable')
  F.DT.Data[, TargetVariable := na.fill(TargetVariable, -11111111.222)]
  if(is.null(Ary.SecuCode)){
    DT.Data <- data.table(TradingDay = Ary.TradingDay, Tag = 1, Order = 1:length(Ary.TradingDay))
    DT.Data <- merge(DT.Data, F.DT.Data[,c('TradingDay', 'TargetVariable'), with=F], by = 'TradingDay', all=T)
    setkey(DT.Data, TradingDay, SecuCode)
    DT.Data[, TargetVariable := na.locf(TargetVariable, na.rm = F)]
    DT.Data[TargetVariable == -11111111.222, TargetVariable := NA ]
    DT.Data <- DT.Data[Tag==1]
    setorder(DT.Data, Order)
  }else{
    
    DT.Data <- data.table(TradingDay = Ary.TradingDay, SecuCode = Ary.SecuCode, Tag = 1, Order = 1:length(Ary.TradingDay))
    DT.Data <- merge(DT.Data, F.DT.Data[,c('TradingDay','SecuCode', 'TargetVariable'), with=F], by = c('TradingDay','SecuCode'), all=T)
    setkey(DT.Data, TradingDay, SecuCode)
    DT.Data[, TargetVariable := na.locf(TargetVariable, na.rm = F), by ='SecuCode']
    DT.Data[TargetVariable == -11111111.222, TargetVariable := NA ]
    DT.Data <- DT.Data[Tag==1]
    setorder(DT.Data, Order)
    
  }
  return(DT.Data$TargetVariable)
}




#通用获取数据
GetSQLData <- function(SQLString
                               , ODBC
                               , User 
                               , Pwd
                               , as.is = F
){
  DBChannel <- odbcConnect(ODBC, User, Pwd) 
  require(data.table)
  Table <- sqlQuery(DBChannel,SQLString, as.is) 
  Table <- as.data.table(Table)
  close(DBChannel)
  return(Table)
}
#通用获取数据 不需要配置odbc管理
GetSQLDataNew <- function(SQLString
                       , Dbn
                       , User 
                       , Pwd
                       , as.is = F
){
  DBChannel <- OdbcSqlConnect(DBN=Dbn, UID=User, PWD= Pwd)
  require(data.table)
  Table <- sqlQuery(DBChannel,SQLString, as.is) 
  Table <- as.data.table(Table)
  close(DBChannel)
  return(Table)
}

#获取交易日数据 无需导入交易日版本
GetTradingDay <- function(ODBC, User, Pwd, Type="Daily",  
                          BegDate = '2000-01-01' , EndDate = Sys.Date()){
  
  dbConn <- OdbcSqlConnect(DBN=ODBC,UID=User,PWD=Pwd)
  # odbcConnect(dsn=ODBC, uid=User, pwd= Pwd)
  # while (dbConn < 0) {
  #   tcl("after", 1000, expression(dbConn <- odbcConnect(dsn=ODBC, uid=User, pwd= Pwd) ))    
  # }
  
  sqlStr = "Select TradingDate,
                 IfTradingDay, SecuMarket, IfWeekEnd, IfMonthEnd, IfQuarterEnd, IfYearEnd
                 From JYDB.dbo.QT_TradingDayNew
                 where SecuMarket = 83 and IfTradingDay = 1
                 order by TradingDate"
  
  tradData = sqlQuery(dbConn, sqlStr, as.is=rep(T,7))
  tradData$TradingDate <- as.Date(tradData$TradingDate)
  close(dbConn)
  
  # Trading days at month end
  if (Type == "Daily"){
    AllDates = unique(tradData$TradingDate)    
    
  }else if (Type == "Weekly"){
    AllDates = unique(tradData$TradingDate[tradData$IfWeekEnd ==1])   
    
  }else if (Type == "Monthly"){
    AllDates = unique(tradData$TradingDate[tradData$IfMonthEnd ==1])     
    
  }else if (Type == "Quarterly"){
    AllDates = unique(tradData$TradingDate[tradData$IfQuarterEnd ==1]) 
    
  }else if (Type == "Yearly"){
    AllDates = unique(tradData$TradingDate[tradData$IfYearEnd ==1])  
    
  }else{
    cat("Error: Type is wrong value. It must be one of Daily, Weekly, Monthly or Yearly\n")
    return
  }
  
  AllDates = AllDates[AllDates >= BegDate & AllDates <= EndDate]
  
  return(AllDates)
  
}


#获取交易日数据 无需导入交易日版本
GetTradingDayStart <- function(ODBC, User, Pwd, Type="Daily",  
                               BegDate = '2010-01-01' , EndDate = Sys.Date()){
  
  # 更新为周初、月初、季出、年初
  dbConn <- OdbcSqlConnect(DBN=ODBC,UID=User,PWD=Pwd)
  
  sqlStr = "Select TradingDate,
                 IfTradingDay, SecuMarket, IfWeekEnd, IfMonthEnd, IfQuarterEnd, IfYearEnd
                 From JYDB.dbo.QT_TradingDayNew
                 where SecuMarket = 83 and IfTradingDay = 1
                 order by TradingDate"
  
  tradData = sqlQuery(dbConn, sqlStr, as.is=rep(T,7))
  tradData$TradingDate <- as.Date(tradData$TradingDate)
  close(dbConn)
  
  # Trading days at month begin
  if (Type == "Daily"){
    # AllDates = unique(tradData$TradingDate)     
    iIndex <- which(!is.na(tradData$TradingDate)) 
    
  }else if (Type == "Weekly"){
    iIndex <- which(tradData$IfWeekEnd ==1) + 1
    
  }else if (Type == "Monthly"){
    iIndex <- which(tradData$IfMonthEnd ==1) + 1
    
  }else if (Type == "MonthlyMid"){
    iIndex <- c()
    icurrentDate <- ymd(paste(year(BegDate),month(BegDate),'15',sep='-'))
    while( icurrentDate <= EndDate){
      iIndex <- c(iIndex,
                  which(tradData$TradingDate == min(tradData$TradingDate[tradData$TradingDate >= icurrentDate])))
      icurrentDate <- icurrentDate + months(1)
    }
    
  }else if (Type == "Quarterly"){
    iIndex <- which(tradData$IfQuarterEnd ==1) + 1
    
  }else if (Type == "Yearly"){
    iIndex <- which(tradData$IfYearEnd ==1) + 1
    
  }else{
    cat("Error: Type is wrong value. It must be one of Daily, Weekly, Monthly, MonthlyMid or Yearly\n")
    return
  }
  
  iIndex <- iIndex[iIndex <= nrow(tradData)]
  AllDates = sort(unique(tradData$TradingDate[iIndex]))     
  
  AllDates = AllDates[AllDates >= BegDate & AllDates <= EndDate]
  
  return(AllDates)
  
}

# 获取两个日期间相差的交易日天
GetTradingDayDiff <- function(Ary.BegDate, Ary.EndDate, Ary.TradingDay){
  Ary.BegDate <- GetFirstTradingDay(Ary.BegDate, Ary.TradingDay)
  Ary.EndDate <- GetFirstTradingDay(Ary.EndDate, Ary.TradingDay)
  Index.BegDate <- match(Ary.BegDate, Ary.TradingDay)
  Index.EndDate <- match(Ary.EndDate, Ary.TradingDay)
  return(Index.EndDate-Index.BegDate)
}
#获取TradingDay里的每月第一个交易日
GetFirstTradingDayofMonth <- function(TradingDay){
  MonthChangeTag <- month(TradingDay)-shift(month(TradingDay),fill=0)
  FirstTradingDayofMonth <- TradingDay[MonthChangeTag!=0]#
  return(FirstTradingDayofMonth)
}
#获取TradingDay里的每周第一个交易日
GetFirstTradingDayofWeek <- function(TradingDay){
  WeekChangeTag <- wday(TradingDay)-shift(wday(TradingDay),fill=0)
  FirstTradingDayofWeek <- TradingDay[WeekChangeTag!=1]
  return(FirstTradingDayofWeek)
}
#获取当前交易日的前n或后n个交易日
GetTradingDayLeadLag <- function(OriginTradingDay, Data.TradingDay,LeadLag=1){
  OriginTradingDay <- GetFirstTradingDay(OriginTradingDay,Data.TradingDay)
  Index <- match(OriginTradingDay,Data.TradingDay)
  Index <- Index-LeadLag
  Index[Index<=0] <- 1
  return(Data.TradingDay[Index])
}
#获取当前日期后的第一个交易日
GetFirstTradingDay <- function(Ary.Date,Ary.TradingDay){
  DT.TradingDay <- data.table(Date=Ary.TradingDay, TradingDay  =Ary.TradingDay)
  DT.Date <- data.table(Date = seq.Date(from =as.Date(DT.TradingDay$Date[1])
                                        ,to = as.Date(DT.TradingDay$Date[nrow(DT.TradingDay)])
                                        , 'day'))
  setkey(DT.Date, Date)
  setkey(DT.TradingDay, Date)
  DT.Date <- DT.TradingDay[DT.Date,nomatch=NA]
  DT.Date[,TradingDay := na.locf(TradingDay, na.rm=F,fromLast = T)]
  return(DT.Date$TradingDay[match(Ary.Date, DT.Date$Date)])
}
#获取当前日期前的第一个交易日
GetLastTradingDay <- function(Ary.Date,Ary.TradingDay){
  DT.TradingDay <- data.table(Date=Ary.TradingDay, TradingDay  =Ary.TradingDay)
  DT.Date <- data.table(Date = seq.Date(from =as.Date(DT.TradingDay$Date[1])
                                        ,to = as.Date(DT.TradingDay$Date[nrow(DT.TradingDay)])
                                        , 'day'))
  setkey(DT.Date, Date)
  setkey(DT.TradingDay, Date)
  DT.Date <- DT.TradingDay[DT.Date,nomatch=NA]
  DT.Date[,TradingDay := na.locf(TradingDay, na.rm = F, fromLast = F)]
  return(DT.Date$TradingDay[match(Ary.Date, DT.Date$Date)])
}
#获取下一个报告期截止
GetNextEndDate <- function(TradingDay){
  EndDate.N <- TradingDay
  Index <- which(month(TradingDay)>=1 & month(TradingDay)<=3)
  if(length(Index)>0){
    EndDate.N[Index] <- as.Date(paste(year(TradingDay[Index]),'-06-30',sep =''))
  }
  Index <- which(month(TradingDay)>=4 & month(TradingDay)<=6)
  if(length(Index)>0){
    EndDate.N[Index] <- as.Date(paste(year(TradingDay[Index]),'-9-30',sep =''))
  }
  Index <- which(month(TradingDay)>=7 & month(TradingDay)<=9)
  if(length(Index)>0){
    EndDate.N[Index] <- as.Date(paste(year(TradingDay[Index]),'-12-31',sep =''))
  }
  Index <- which(month(TradingDay)>=10 & month(TradingDay)<=12)
  if(length(Index)>0){
    EndDate.N[Index] <- as.Date(paste(year(TradingDay[Index])+1,'-03-31',sep =''))
  }
  return(EndDate.N)
}
#获取当前报告期截止日
GetCurrentEndDate <- function(TradingDay){
  EndDate.C <- TradingDay
  Index <- which(month(TradingDay)>=1 & month(TradingDay)<=3)
  if(length(Index)>0){
    EndDate.C[Index] <- as.Date(paste(year(TradingDay[Index]),'-03-31',sep =''))
  }
  Index <- which(month(TradingDay)>=4 & month(TradingDay)<=6)
  if(length(Index)>0){
    EndDate.C[Index] <- as.Date(paste(year(TradingDay[Index]),'-06-30',sep =''))
  }
  Index <- which(month(TradingDay)>=7 & month(TradingDay)<=9)
  if(length(Index)>0){
    EndDate.C[Index] <- as.Date(paste(year(TradingDay[Index]),'-9-30',sep =''))
  }
  Index <- which(month(TradingDay)>=10 & month(TradingDay)<=12)
  if(length(Index)>0){
    EndDate.C[Index] <- as.Date(paste(year(TradingDay[Index]),'-12-31',sep =''))
  }
  return(EndDate.C)
}
#获取上一个报告期截止
GetFormerEndDate <- function(TradingDay){
  EndDate.F <- TradingDay
  Index <- which(month(TradingDay)>=1 & month(TradingDay)<=3)
  EndDate.F[Index] <- as.Date(paste(year(TradingDay[Index])-1,'-12-31',sep =''))
  Index <- which(month(TradingDay)>=4 & month(TradingDay)<=6)
  EndDate.F[Index] <- as.Date(paste(year(TradingDay[Index]),'-03-31',sep =''))
  Index <- which(month(TradingDay)>=7 & month(TradingDay)<=9)
  EndDate.F[Index] <- as.Date(paste(year(TradingDay[Index]),'-06-30',sep =''))
  Index <- which(month(TradingDay)>=10 & month(TradingDay)<=12)
  EndDate.F[Index] <- as.Date(paste(year(TradingDay[Index]),'-9-30',sep =''))
  return(EndDate.F)
}

#获取上上一个报告期截止
GetFormerOfFormerEndDate <- function(TradingDay){
  EndDate.F <- TradingDay
  Index <- which(month(TradingDay)>=1 & month(TradingDay)<=3)
  EndDate.F[Index] <- as.Date(paste(year(TradingDay[Index])-1,'-9-30',sep =''))
  Index <- which(month(TradingDay)>=4 & month(TradingDay)<=6)
  EndDate.F[Index] <- as.Date(paste(year(TradingDay[Index])-1,'-12-31',sep =''))
  Index <- which(month(TradingDay)>=7 & month(TradingDay)<=9)
  EndDate.F[Index] <- as.Date(paste(year(TradingDay[Index]),'-03-31',sep =''))
  Index <- which(month(TradingDay)>=10 & month(TradingDay)<=12)
  EndDate.F[Index] <- as.Date(paste(year(TradingDay[Index]),'-06-30',sep =''))
  return(EndDate.F)
}
#获取收益报告
GetProfitSummary <- function(x, DeleteZero = F){
  Return <- c(0,diff(x)/x[1:(length(x)-1)])
  if(DeleteZero){
    Return <- Return[Return!=0]
  }
  SharpeRatio <- mean(Return)/sd(Return) * sqrt(250)
  AnnualReturn <- sum(Return) / (length(Return)) * 250
  MaxDrawdown <- max((RollMaxNoLeast(x,Inf)-x)/RollMaxNoLeast(x,Inf))
 
  Summary <- data.table(AnnualReturn = AnnualReturn
                        , SharpeRatio = SharpeRatio
                        , MaxDrawdown = MaxDrawdown)
  return(Summary)
}
#获取收益报告，输入为data.table TradingDay, CumProReturn
GetProfitSummary2 <- function(DT.Return, DeleteZero = F){
  colnames(DT.Return) <- c('TradingDay', 'CumProdReturn')
  DT.Return[,Return:=c(0,diff(CumProdReturn)/CumProdReturn[1:(length(CumProdReturn)-1)])]
  
  if(DeleteZero){
    DT.Return <- DT.Return[Return!=0]
  }
  
  Summary <- DT.Return[,.(SharpeRatio = mean(Return)/sd(Return) * sqrt(250),
                          AnnualReturn = sum(Return) / (length(Return)) * 250,
                          MaxDrawdown = max((RollMaxNoLeast(CumProdReturn,Inf)-CumProdReturn)/RollMaxNoLeast(CumProdReturn,Inf)),
                          CalMar = sum(Return) / (length(Return)) * 250 / maxdrawdown(log(CumProdReturn))$maxdrawdown,
                          From = TradingDay[maxdrawdown(log(CumProdReturn))$from],
                          To = TradingDay[maxdrawdown(log(CumProdReturn))$to]
  )]
  
  return(Summary)
}
## 滚动最大回
RollMaxDrawdownNoLeast <- function(x, n){
  MDD <- (RollMaxNoLeastCpp(x, n) - x)/RollMaxNoLeastCpp(x, n)
  MDDn <- RollMaxNoLeastCpp(MDD, n)
  return(MDDn)
}
# 获取个股区间Return
RangeReturn <- function(DT.DailyQuote,Data.TradingDay, Ary.SecuCode, Ary.BegDate, Ary.EndDate){
  if(!is.element('Return', names(DT.DailyQuote))){
    DT.DailyQuote[,Return := ClosePrice/PrevClosePrice-1]
  }
  DT.RangeReturn <- data.table()
  Ary.DayCount <- c()
  Ary.SecuCode.Unique <- unique(Ary.SecuCode)
  c <- 0# 分SecuCode进行子集中查找，减少对地址的调取、更改次
  for(s in Ary.SecuCode.Unique){
    DT.DailyQuote.Sub <- DT.DailyQuote[SecuCode == s]
    setkey(DT.DailyQuote.Sub, TradingDay)
    Ary.SecuCode.Sub <-  Ary.SecuCode[which(Ary.SecuCode ==s )]
    Ary.BegDate.Sub <-  Ary.BegDate[which(Ary.SecuCode == s)]
    Ary.EndDate.Sub <-  Ary.EndDate[which(Ary.SecuCode == s)]
    DT.RangeReturn.Sub <- data.table()
    Ary.DayCount.Sub <- c()
    for(i in 1:length(Ary.SecuCode.Sub)){
      New <- data.table(TradingDay = Data.TradingDay[Data.TradingDay >= Ary.BegDate.Sub[i] 
                                                     & Data.TradingDay <= Ary.EndDate.Sub[i]]
      )
      New <- DT.DailyQuote.Sub[list(New),.(TradingDay, SecuCode, Return)]
      if(nrow(New[is.na(SecuCode)])>0)
      {
        next
      }
      DT.RangeReturn.Sub <- rbind(DT.RangeReturn.Sub, New)
      Ary.DayCount.Sub <- c(Ary.DayCount.Sub, c(1:nrow(New)))
    }
    DT.RangeReturn <- rbind(DT.RangeReturn, DT.RangeReturn.Sub)
    Ary.DayCount <- c(Ary.DayCount, Ary.DayCount.Sub)
  }
  DT.RangeReturn <- DT.DailyQuote[list(DT.RangeReturn),.(TradingDay, SecuCode, Return)]
  DT.RangeReturn[, DayCount := Ary.DayCount]
  Ary.Group <- Ary.DayCount
  Ary.Group <- c(0,diff(Ary.Group))
  Ary.Group[Ary.Group == 1] <- NA
  Ary.Group[Ary.Group < 1] <- 1
  Ary.Group <- na.fill(Ary.Group,0)
  Ary.Group <- cumsum(Ary.Group)
  DT.RangeReturn[, Group := Ary.Group]
  return(DT.RangeReturn)
}
RangeReturn2 <- function(DT.DailyQuote,Data.TradingDay, Ary.SecuCode, Ary.BegDate, Ary.EndDate){
  if(!is.element('Return', names(DT.DailyQuote))){
    DT.DailyQuote[,Return := ClosePrice/PrevClosePrice-1]
  }
  DT.RangeReturn <- data.table()
  Ary.DayCount <- c()
  Ary.SecuCode.Unique <- unique(Ary.SecuCode)
  c <- 0# 分SecuCode进行子集中查找，减少对地址的调取、更改次
  DT.RangeReturn <- list()
  for(i in 1:length(Ary.SecuCode)){
    TradingDay <- Data.TradingDay[Data.TradingDay >= Ary.BegDate[i]
                                  & Data.TradingDay <= Ary.EndDate[i]]
    DT.RangeReturn <- c(DT.RangeReturn
                          , list(data.table(SecuCode= Ary.SecuCode[i]
                                 , TradingDay = TradingDay
                                 , DayCount = 1:length(TradingDay)
                                 , Group = i)))
  }
  DT.RangeReturn <- rbind.BigData(DT.RangeReturn)
  DT.RangeReturn <- merge(DT.RangeReturn, Data.DailyQuote[,.(TradingDay, SecuCode, Return)], by=c('TradingDay','SecuCode'),all.x=T)
  return(DT.RangeReturn)
}
## 获取时间长数据，需要foreach+doParallel
GetDateRange <- function(F.Ary.Data, F.Ary.BegDate, F.Ary.EndDate, Data.TradingDay){
  require(doParallel)
  F.DT.Data <- data.table(Data = F.Ary.Data
                          , BegDate = F.Ary.BegDate
                          , EndDate = F.Ary.EndDate)
  if(nrow(F.DT.Data[BegDate > EndDate]) > 0){
    cat("存在BegDate > EndDate!")
  }
  cl <- makeCluster(detectCores() - 1)
  registerDoParallel(cl) ## 注册并行
  F.DT.Data <- foreach(i = 1:nrow(F.DT.Data),.packages = "data.table") %dopar% {
    Ary.TradingDay <- Data.TradingDay[Data.TradingDay >=  F.DT.Data$BegDate[i]
                                      & Data.TradingDay <=  F.DT.Data$EndDate[i]]
    data.table(Data = F.DT.Data$Data[i]
               , TradingDay = Ary.TradingDay
               , Group = i
               , Rank = 1:length(Ary.TradingDay))
  }
  stopImplicitCluster() ## 结束并行
  F.DT.Data <- rbindlist(F.DT.Data)
  return(F.DT.Data)
}
## 获取时间长数据，需要foreach+doParallel
GetDateRange2 <- function(DT.Data, BegDate, EndDate, Data.TradingDay){
  require(doParallel)
  F.DT.Data <- copy(DT.Data)
  F.DT.Data[, Group := 1:.N]
  setnames(F.DT.Data, BegDate, 'F.BegDate')
  setnames(F.DT.Data, EndDate, 'F.EndDate')
  if(nrow(F.DT.Data[F.BegDate > F.EndDate]) > 0){
    stop("存在BegDate > EndDate!")
  }
  cl <- makeCluster(detectCores() - 1)
  registerDoParallel(cl) ## 注册并行
  F.Result <- foreach(i = 1:nrow(F.DT.Data),.packages = "data.table") %dopar% {
    Ary.TradingDay <- Data.TradingDay[Data.TradingDay >=  F.DT.Data$F.BegDate[i]
                                      & Data.TradingDay <=  F.DT.Data$F.EndDate[i]]
    data.table(TradingDay = Ary.TradingDay
               , Group = i
               , Rank = 1:length(Ary.TradingDay))
  }
  stopCluster(cl)
  stopImplicitCluster() ## 结束并行
  F.Result <- rbindlist(F.Result)
  F.DT.Data <- merge(F.DT.Data, F.Result, by = 'Group', all.x=T)
  setnames(F.DT.Data, 'F.BegDate', BegDate)
  setnames(F.DT.Data, 'F.EndDate', EndDate)
  return(F.DT.Data)
}
#开根号
sqrtn <- function(x, n){
  return(abs(x)^(1/n)*sign(x))
}
#滚动最大值NoLeast
RollMaxNoLeast <- function(x,Length=21){
  # =================================================================================== #
  # 函数功能：获取一个窗口内的最大值, 对于输入数组x的任意数据点，Length为当前数据向前能取到的最大长度与设定Length的最小值，
  #           返回的数组大小和输入的x一致
  #
  # 输入变量说明：x是一个numeric数组, 为要计算的基础数据
  #               Length是一个integer, 为窗口的大小
  #
  # 输出变量说明：输出为一个与x等长的数组
  #
  # 例：
  # =================================================================================== #
  size <- length(x)
  Result <- array(0, size)
  i <- 1
  while(i <= size){
    Result[i] <- max(x[max((i-Length+1),1):i],na.rm=T)
    i <- i + 1
  }
  return (Result)
}
#滚动最小值NoLeast
RollMinNoLeast <- function(x,Length=21){
  size <- length(x)
  Result <- array(0, size)
  i <- 1
  while(i <= size){
    Result[i] <- min(x[max((i-Length+1),1):i],na.rm=T)
    i <- i + 1
  }
  return (Result)
}
#滚动指数加权平均
RollEWMA <- function(x, HalfLife, Length){
  Size <- length(x)
  HLW <- HalfLifeWeight(HalfLife, Length)
  Result <- array(0, Size-Length+1)
  i <- 1
  while(i + Length - 1 <= Size){
    Result[i] <- weighted.mean(x[i:(i + Length - 1)], HLW, na.rm=T)
    i <- i + 1
  }
  return (Result)
}
#滚动稳定小波滤波
RollWavelet <- function(x,Length,Method = 'modwt'){
  Size <- length(x)
  Result <- array(0, Size-Length+1)
  i <- 1
  while(i + Length - 1 <= Size){
    wt <- modwt(x[i:(i + Length - 1)], n.levels = 10 ,fast = T, boundary = "reflection")
    wt@W$W1[] <- 0
    wt@W$W2[] <- 0
    wt@W$W3[] <- 0
    wt@W$W4[] <- 0
    # wt@W$W5[] <- 0
    # wt@W$W6[] <- 0
    # wt@W$W7[] <- 0
    # wt@W$W8[] <- 0
    # wt@W$W9[] <- 0
    # wt@W$W10[] <- 0
    iwt <- imodwt(wt)
    Result[i] <- iwt[Length]
    i <- i + 1
  }
  return (Result)
}
#滚动幂加权平
RollPower <- function(x, Alpha, Length){
  Size <- length(x)
  PW <- PowerWeight(Alpha, Length)
  Result <- array(0, Size-Length+1)
  i <- 1
  while(i + Length - 1 <= Size){
    Result[i] <- weighted.mean(x[i:(i + Length - 1)], PW)
    i <- i + 1
  }
  return (Result)
}

#获取单因子分, 默认升序
GetRankScoreSingle <- function(Factor, Decreasing=FALSE){
  Rank <- sort(Factor, decreasing = Decreasing)
  Rank[] <- 1:length(Factor)
  Result <- Rank
  return (Result)
}
#通用画图Date
DFplot <- function(DF
                   ,Xlabel = 'X'
                   ,Ylabel = 'Y'
                   ,Title = '时序'
                   ,Graph = NULL
                   , TogetherGraph = NULL
                   , Ybreaks=NULL
                   , Xbreaks="8 week"
                   , Color = NULL
                   , Leftlabel = T
                   , Print = T
                   , First_size = NULL){
  Melt <- melt(DF, id.vars = 'TradingDay', measure.vars= 2:ncol(DF))
  Melt <- data.table(Melt)
  if(!is.null(Graph)){
    if(is.null(TogetherGraph)){
      Melt$Graph <- ifelse(is.element(Melt$variable, Graph), as.character(Melt$variable),'Main')
      
      p <- (ggplot(Melt, aes(TradingDay, value, colour=variable))
            + geom_path(size=0.4)
            + labs(x=Xlabel, y=Ylabel,title = Title)#x轴y轴Title
            + theme(plot.title = element_text(hjust = 0.5))#Title居中
            # + scale_y_continuous(breaks=seq(0, 2, 0.1))#自定义y轴间
            # + scale_x_continuous(breaks=seq(0, 2, 0.1))#数值X
            + scale_x_date(date_breaks=Xbreaks,date_labels = "%Y_%m_%d")#Date值x
            + facet_wrap(~Graph,ncol = 1,nrow=length(Graph)+1,scales = 'free_y')
            + theme(axis.text.x = element_text(angle=270)))#竖放x轴标
        
      
      if(Leftlabel){
        p <- p + theme(legend.justification=c(0.01,0.99), legend.position=c(0.01,0.99))
      }
      if(!is.null(Ybreaks))
      {
        p <- p +scale_y_continuous(breaks=Ybreaks)
      }
    }else{
      TogetherGraph <- TogetherGraph
      NotTogetherGraph <- Graph[!(Graph %in% TogetherGraph)]
      Melt$Graph <- ifelse(is.element(Melt$variable, Graph), as.character(Melt$variable),'Main')
      Melt$Graph <- ifelse(is.element(Melt$variable, TogetherGraph), paste(TogetherGraph, collapse = ','),as.character(Melt$Graph))
      p <- (ggplot(Melt, aes(TradingDay, value, colour=variable))
            + geom_path(size=0.4)
            + labs(x=Xlabel, y=Ylabel,title = Title)#x轴y轴Title
            + theme(plot.title = element_text(hjust = 0.5))#Title居中
            # + scale_y_continuous(breaks=seq(0, 2, 0.1))#自定义y轴间距Get
            # + scale_x_continuous(breaks=seq(0, 2, 0.1))#数值X
            + scale_x_date(date_breaks=Xbreaks,date_labels = "%Y_%m_%d")#Date值x
            
            + facet_wrap(~Graph,ncol = 1,nrow=length(Graph)+1,scales = 'free_y')
            + theme(axis.text.x = element_text(angle=270)))#竖放x轴标
      if(Leftlabel){
        p <- p + theme(legend.justification=c(0.01,0.99), legend.position=c(0.01,0.99))
      }
      if(!is.null(Ybreaks))
      {
        p <- p + scale_y_continuous(breaks=Ybreaks)
      }
    }
    
  }else{
    Melt$size <- 0.1
    if (!is.null(First_size)){
      Melt[variable==names(DF)[2], size:=0.6]
    }
    p <- (ggplot(Melt, aes(TradingDay, value, colour=variable, size=size))
          + scale_size(range=c(1, 2), guide=FALSE)
          + geom_path() # size=0.4
          + labs(x=Xlabel, y=Ylabel,title = Title)#x轴y轴Title
          + theme(plot.title = element_text(hjust = 0.5))#Title居中
          # + scale_y_continuous(breaks=seq(0, 2, 0.1))#自定义y轴间
          # + scale_x_continuous(breaks=seq(0, 2, 0.1))#数值X
          + scale_x_date(date_breaks=Xbreaks,date_labels = "%Y_%m_%d")#Date值x
          # + facet_wrap(~Graph,ncol = 1,nrow=2,scales = 'free_y')
          + theme(axis.text.x = element_text(angle=270)))#竖放x轴标
    if(Leftlabel){
      p <- p + theme(legend.justification=c(0.01,0.99), legend.position=c(0.01,0.99))
    }
    if(!is.null(Ybreaks))
    {
      p <- p +scale_y_continuous(breaks=Ybreaks)
    }
    if(!is.null(Color)){
      p <- p + scale_color_manual(values = Color)
    }
  }
  if(Print){
    print(p)
  }
  return(p)
}



#通用画图TimeDate
DFplot.Times <- function(DF
                         ,Xlabel = 'X'
                         ,Ylabel = 'Y'
                         ,Title = '时序'
                         ,Graph = NULL
                         , TogetherGraph = NULL
                         , Ybreaks=NULL
                         , Xbreaks="5 min"
                         , Color = NULL
                         , Leftlabel = T
                         , Print = T){
  Melt <- melt(DF, id.vars = 'TradingDay', measure.vars= 2:ncol(DF))
  if(!is.null(Graph)){
    if(is.null(TogetherGraph)){
      Melt$Graph <- ifelse(!is.na(match(Melt$variable, Graph)), Melt$variable,'2')
      
      p <- (ggplot(Melt, aes(TradingDay, value, colour=variable))
            + geom_path()
            + labs(x=Xlabel, y=Ylabel,title = Title)#x轴y轴Title
            + theme(plot.title = element_text(hjust = 0.5))#Title居中
            # + scale_y_continuous(breaks=seq(0, 2, 0.1))#自定义y轴间
            # + scale_x_continuous(breaks=seq(0, 2, 0.1))#数值X
            + scale_x_datetime(date_breaks=Xbreaks,date_labels = "%H:%M:%S")#Datetime值x
            + facet_wrap(~Graph,ncol = 1,nrow=length(Graph)+1,scales = 'free_y')
            + theme(axis.text.x = element_text(angle=270)))#竖放x轴标
      if(Leftlabel){
        p <- p + theme(legend.justification=c(0.01,0.99), legend.position=c(0.01,0.99))
      }
      if(!is.null(Ybreaks))
      {
        p <- p +scale_y_continuous(breaks=Ybreaks)
      }
    }else{
      TogetherGraph <- TogetherGraph
      NotTogetherGraph <- Graph[!(Graph %in% TogetherGraph)]
      Melt$Graph <- ifelse(!is.na(match(Melt$variable, Graph)), Melt$variable,'2')
      Melt$Graph <- ifelse(!is.na(match(Melt$variable, TogetherGraph)),'TogetherGraph', Melt$Graph)
      p <- (ggplot(Melt, aes(TradingDay, value, colour=variable))
            + geom_path()
            + labs(x=Xlabel, y=Ylabel,title = Title)#x轴y轴Title
            + theme(plot.title = element_text(hjust = 0.5))#Title居中
            # + scale_y_continuous(breaks=seq(0, 2, 0.1))#自定义y轴间
            # + scale_x_continuous(breaks=seq(0, 2, 0.1))#数值X
            + scale_x_datetime(date_breaks=Xbreaks,date_labels = "%H:%M:%S")#Datetime值x
            
            + facet_wrap(~Graph,ncol = 1,nrow=length(Graph)+1,scales = 'free_y')
            + theme(axis.text.x = element_text(angle=270)))#竖放x轴标
      if(Leftlabel){
        p <- p + theme(legend.justification=c(0.01,0.99), legend.position=c(0.01,0.99))
      }
      if(!is.null(Ybreaks))
      {
        p <- p +scale_y_continuous(breaks=Ybreaks)
      }
    }
    
  }else{
    p <- (ggplot(Melt, aes(TradingDay, value, colour=variable))
          + geom_path()
          + labs(x=Xlabel, y=Ylabel,title = Title)#x轴y轴Title
          + theme(plot.title = element_text(hjust = 0.5))#Title居中
          # + scale_y_continuous(breaks=seq(0, 2, 0.1))#自定义y轴间
          # + scale_x_continuous(breaks=seq(0, 2, 0.1))#数值X
          + scale_x_datetime(date_breaks=Xbreaks,date_labels = "%H:%M:%S")#Datetime值x
          # + facet_wrap(~Graph,ncol = 1,nrow=2,scales = 'free_y')
          + theme(axis.text.x = element_text(angle=270)))#竖放x轴标
    if(Leftlabel){
      p <- p + theme(legend.justification=c(0.01,0.99), legend.position=c(0.01,0.99))
    }
    if(!is.null(Ybreaks))
    {
      p <- p +scale_y_continuous(breaks=Ybreaks)
    }
    if(!is.null(Color)){
      p <- p + scale_color_manual(values = Color)
    }
  }
  if(Print){
    print(p)
  }
  return(p)
}

#画双Y
fun.plotyy <- function(x,y,y1,TYPE = c("l","l")){
  par(mar=c(5,5,4,5)+0.1,new=F)
  plot(x,y,type = TYPE[1],xlab = "Date")
  par(new=T)
  plot(y1,xlab="",ylab="",type = TYPE[2],col='red',axes=F,lty=3, lwd=1.5)
  axis(4, col.ticks = "red",col.axis="red")
  #mtext("Sharpe",side=4,line=3,col="red")
}
MultiAdjustRowNumber <- function(DataList, StartDay, EndDay = NULL){
  for(DF in DataList){
    DF <- AdjustRowNumber(DF, StartDay, EndDay = EndDay)
  }
  return(DataList)
  
}
AdjustRowNumber <- function(DF, StartDay, EndDay=NULL){
  Index.Start <- which(DF$TradingDay >= StartDay)
  DF <- DF[Index.Start,]
  if (!is.null(EndDay))
  {
    Index.End <- which(DF$TradingDay <= EndDay)
    DF <- DF[Index.End,]
  }
  return(DF)
}
#半衰指数权重
HalfLifeWeight <- function(HalfLife, Length){
  Lambda <- exp(log(1/2)/HalfLife)
  Result <- array((1-Lambda), dim = c(Length, 1))
  i <- 1
  while (i < Length){
    Result[i] <- Result[i] * (Lambda ^ (Length - i))
    i <- i + 1
  }
  Result <- Result/sum(Result)
  return(Result)
}
#权重半衰
HalfLife <- function(Weight){
  Weight.Cumsum <- cumsum(Weight)
  Result <- length(Weight.Cumsum[Weight.Cumsum>0.5])
  return(Result)
}


# 是否停复
IfSuspend <- function(F.Ary.SecuCode, F.Ary.TradingDay){
  if(length(F.Ary.SecuCode)!=length(F.Ary.TradingDay)){
    stop('日期和股票代码数组长度不一致！')
  }
  load(paste0(Dir.Data, 'SuspendResumption.Rdata'))
  F.Ary.SecuCode <- as.numeric(F.Ary.SecuCode)
  
  F.DT.Data <- data.table(SecuCode = F.Ary.SecuCode, TradingDay=F.Ary.TradingDay,Group=1:length(F.Ary.TradingDay))
  F.DT.SuspendResumption <- copy(Data.SuspendResumption)


  F.DT.Data <- full_join(F.DT.Data, F.DT.SuspendResumption,by='SecuCode')
  F.DT.Data <- as.data.table(F.DT.Data)
  F.DT.Data <- F.DT.Data[TradingDay >= SuspendDate | is.na(SuspendDate)]
  setorder(F.DT.Data,-SuspendDate)
  F.DT.Data[,Rowid := rowidv(.SD,cols=c('Group'))]
  F.DT.Data <- F.DT.Data[Rowid == 1]
  F.DT.Data[,Rowid := NULL]
  F.DT.Data[,IfSuspend := 0]
  F.DT.Data[TradingDay < ResumptionDate | ResumptionDate<SuspendDate, IfSuspend := 1]
  setorder(F.DT.Data, Group)
  return(F.DT.Data$IfSuspend)
}
# 获取复牌
GetResumptionDate <- function(F.Ary.SecuCode, F.Ary.TradingDay){
  if(length(F.Ary.SecuCode)!=length(F.Ary.TradingDay)){
    stop('日期和股票代码数组长度不一致！')
  }
  SQLSTR <- ("
             SELECT SecuCode      
             ,[SuspendDate]
             ,[SuspendTime]
             ,[ResumptionDate]
             ,[ResumptionTime]
             FROM [JYDB].[dbo].[LC_SuspendResumption] as SR, [JYDB].[dbo].[SecuMain] as SM
             where SR.InnerCode = SM.InnerCode and SecuCategory = 1 and SecuMarket  in (83,90) and ListedState in (1,3,5)
             
             order by SuspendDate  ,SecuCode
             ")
  Data.SuspendResumption <- GetSQLDataNew(SQLSTR, ODBC.StockResearch, SQL.User, SQL.Pwd)  
  if(nrow(Data.SuspendResumption)>0){
    Data.SuspendResumption <- as.data.table(Data.SuspendResumption)
    Data.SuspendResumption[, SuspendDate := as.Date(SuspendDate)+1]
    Data.SuspendResumption[, SuspendTime := as.character(SuspendTime)]
    Data.SuspendResumption[, SuspendTime := na.fill(SuspendTime, '00:00:00')]
    Data.SuspendResumption[, SuspendTime := as.POSIXct(paste(SuspendDate,SuspendTime,sep=' '),tz='GMT')]
    Data.SuspendResumption[, ResumptionDate := as.Date(ResumptionDate)+1]
    Data.SuspendResumption[, ResumptionTime := as.character(ResumptionTime)]
    Data.SuspendResumption[is.na(ResumptionTime), ResumptionTime := '00:00:00']
    Data.SuspendResumption[ResumptionTime == '', ResumptionTime :=  '00:00:00']
    Data.SuspendResumption[, ResumptionTime := as.POSIXct(paste(ResumptionDate,ResumptionTime,sep=' '),tz='GMT')]
    setkey(Data.SuspendResumption,SuspendDate, SecuCode)
    # 去重
    setorder(Data.SuspendResumption, -SuspendTime)
    Data.SuspendResumption[, Rowid := rowidv(.SD, cols = c('ResumptionDate', 'SecuCode'))]
    Data.SuspendResumption <- Data.SuspendResumption[Rowid == 1]
    Data.SuspendResumption[, Rowid := NULL]
    setkey(Data.SuspendResumption, ResumptionDate, SecuCode)
  }
  F.Ary.SecuCode <- as.numeric(F.Ary.SecuCode)
  F.DT.Data <- data.table(SecuCode = F.Ary.SecuCode, TradingDay=F.Ary.TradingDay,Group=1:length(F.Ary.TradingDay))
  F.DT.SuspendResumption <- copy(Data.SuspendResumption)
  
  
  F.DT.Data <- full_join(F.DT.Data, F.DT.SuspendResumption,by='SecuCode')
  F.DT.Data <- as.data.table(F.DT.Data)
  F.DT.Data <- F.DT.Data[TradingDay >= SuspendDate | is.na(SuspendDate)]
  setorder(F.DT.Data,-SuspendDate)
  F.DT.Data[,Rowid := rowidv(.SD,cols=c('Group'))]
  F.DT.Data <- F.DT.Data[Rowid == 1]
  F.DT.Data[,Rowid := NULL]

  F.DT.Data[!(TradingDay < ResumptionDate | ResumptionDate<SuspendDate), ResumptionDate := TradingDay]
  F.DT.Data[ResumptionDate<SuspendDate, ResumptionDate := NA]
  setkey(F.DT.Data, TradingDay, SecuCode)
  return(F.DT.Data[,.(TradingDay,SecuCode,ResumptionDate)])
}
#统计最大回
fun.mdd <- function(Return) 
{
  require(tseries)
  if(Return[1] < 0.5) #return
  {
    y <- cumprod(Return + 1) 
    z <- cummax(y)
    x <- y/z
    mdd <- maxdrawdown(x)
    return(mdd)    
  } else{
    y <- Return #net value
    z <- cummax(y)
    x <- y/z
    mdd <- maxdrawdown(x)
    return(mdd)   
  }
}

## rbind大数据模
rbind.BigData <- function(L.DataTable){
  if(length(L.DataTable)>0){
    if(is.data.table(L.DataTable[[1]])){
      if(ncol(L.DataTable[[1]])>0){
        ColNum <- ncol(L.DataTable[[1]])
        ColNames <- names(L.DataTable[[1]])
      }
    }
  }
  if(is.null(ColNum)){
    stop('NoColNum')
  }
  L.NewData <- list()
  for(j in 1:ColNum){
    Ary.Data <- list()
    for(i in 1:length(L.DataTable)){
      Ary.Data <- c(Ary.Data, list(L.DataTable[[i]][, j,with=F]))
    }
    Ary.Data <- rbindlist(Ary.Data)
    L.NewData <- c(L.NewData, list(Ary.Data))
  }
  rm(L.DataTable)
  L.NewData <- as.data.table(L.NewData)
  return(L.NewData)
}

## 获取时间长数据，需要foreach+doParallel
GetDateRange2 <- function(DT.Data, BegDate, EndDate, Data.TradingDay){
  require(doParallel)
  F.DT.Data <- copy(DT.Data)
  F.DT.Data[, Group := 1:.N]
  setnames(F.DT.Data, BegDate, 'F.BegDate')
  setnames(F.DT.Data, EndDate, 'F.EndDate')
  if(nrow(F.DT.Data[F.BegDate > F.EndDate]) > 0){
    stop("存在BegDate > EndDate!")
  }
  cl <- makeCluster(detectCores() - 1)
  registerDoParallel(cl) ## 注册并行
  F.Result <- foreach(i = 1:nrow(F.DT.Data),.packages = "data.table") %dopar% {
    Ary.TradingDay <- Data.TradingDay[Data.TradingDay >=  F.DT.Data$F.BegDate[i]
                                      & Data.TradingDay <=  F.DT.Data$F.EndDate[i]]
    data.table(TradingDay = Ary.TradingDay
               , Group = i
               , Rank = 1:length(Ary.TradingDay))
  }
  stopCluster(cl)
  stopImplicitCluster() ## 结束并行
  F.Result <- rbindlist(F.Result)
  F.DT.Data <- merge(F.DT.Data, F.Result, by = 'Group', all.x=T)
  setnames(F.DT.Data, 'F.BegDate', BegDate)
  setnames(F.DT.Data, 'F.EndDate', EndDate)
  return(F.DT.Data)
}

NAHandle <- function(input, flag){
  # input为时间序列向量，flag为缺失值处理方法
  if (flag==1){    # 按序列前值填充
    naIndex <- which(is.na(input)==TRUE)
    if (length(naIndex)==0){  # 无缺省值 直接返回
      return (input)
    }else{
      for (i in 1:length(naIndex)){
        iindex <- naIndex[i]
        j <- iindex-1
        
        if (j==0){
          input[iindex] <- NA
        }else{
          input[iindex] <- input[iindex-1] # 不断回溯寻找非na数据
        }
      }
      return (input)
    }
  }
}

PriceSeasonality <- function(ipriceDf, IfPlot=F, ilaby=''){
  # 计算价格d等季节性图
  # ipriceDf的字段：[EndDate, 商品1， 商品2]
  # ilaby:char y轴名字
  
  icommodityList <- colnames(ipriceDf)[2:ncol(ipriceDf)]
  ipriceDf$year <- year(ipriceDf$EndDate)
  ipriceDf$day <- as.vector(ipriceDf$EndDate - as.Date(paste(ipriceDf$year, '01', '01', sep='-'))) # 转成int类型
  
  for (icommodity in icommodityList){
    # 各商品单独画图
    if (IfPlot)
      windows(width=12, height=8)
    tempDf1 <- ipriceDf[, c('EndDate', icommodity, 'year', 'day')]
    tempDf1 <- na.omit(tempDf1)  # 删除NA行
    maxValue <- max(tempDf1[, icommodity]) # 画图坐标限制
    minValue <- min(tempDf1[, icommodity])
    yearList <- unique(tempDf1$year)
    
    itempDfPlot <- data.frame(list(day=seq(366)))  # 绘图数据
    
    for (i in seq(length(yearList))){
      iyear <- yearList[i]
      tempDf2 <- subset(tempDf1, year==iyear)[, c('day', icommodity)] # 某年度数
      colnames(tempDf2) <- c('day', iyear)
      itempDfPlot <- join(itempDfPlot, tempDf2, by='day', type='left')  # 按同一坐标轴画图
      itempDfPlot[, as.character(iyear)] <- NAHandle(itempDfPlot[, as.character(iyear)], flag=1)
      
      if (i==1){
        plot(itempDfPlot$day, itempDfPlot[, as.character(iyear)], type='l', lty=1, lwd=2, pch=20, cex=1, col=i, 
             ylim = c(minValue, maxValue), xaxt="n",  main = icommodity, xlab = 'date', ylab = ilaby)
      }else{
        par(new=T)
        plot(itempDfPlot$day, itempDfPlot[, as.character(iyear)], type='l', lty=1, lwd=2, pch=20, cex=1, col=i, 
             ylim = c(minValue, maxValue), xaxt="n", main = icommodity, xlab = 'date', ylab = ilaby)
      }
      
    }
    grid(nx=16, ny=16,lwd=0.25,lty=3,col="grey")
    legend('bottomright', legend=yearList, col=seq(length(yearList)), lty=1)
    ilabels <- seq(as.Date("2012-01-01"), length=366, by="day")
    ilabels <- paste(month(ilabels), day(ilabels), sep = '-')
    iat <- seq(from=1, to=366, by= 30)
    axis(side=1, at=iat, labels=ilabels[iat]) # 修改坐标
  }
  
}


FactorNumSeasonality <- function(iDT, IfReturnDT=F, IfPercent=F, IfPlot=F){
  # =================================================================================== #
  # 函数功能：根据因子或者数据输入，画出每日非零数值数，
  #
  # 输入变量说明：datatable iDT，第一列为时间，第二列开始为数据；
  #               IfReturnDT 是否返回绘制数据
  #               IfPercent  是否返回非零百分比
  # 
  # 输出变量说明：输出绘图
  #
  # 示例：FactorNumSeasonality(DT.Factor[TradingDay>'2020-01-01',c('TradingDay', 'Factor'), with=F],IfPercent=T,IfReturnDT=T)
  # =================================================================================== #
  
  if (ncol(iDT) == 2) # 单独处理只有一列分析数据的情况
    iDT$Add <- iDT[,2]
  
  colnames(iDT)[1] <- 'EndDate'
  colnames0 <- colnames(iDT)
  
  iDTNew <- data.table()
  for (i in seq(2, ncol(iDT))){
    iDT_ <- iDT[,c(1, i), with=F]
    
    colnames(iDT_)[2] <- 'Temp'
    
    if (IfPercent){
      iDT_1 <- iDT_[, list(temp = sum(!is.na(Temp)) / .N), by='EndDate']
    }else{
      iDT_1 <- iDT_[, list(temp = sum(!is.na(Temp))), by='EndDate']
    }
    
    colnames(iDT_1)[2] <- colnames0[i]
    
    if (!nrow(iDTNew)){
      iDTNew <- iDT_1
      
    }else{
      iDTNew <- join(iDTNew, iDT_1, by='EndDate', type='full')
    }
    
  }
  
  if ('Add' %in% colnames(iDTNew)){
    iDTNew <- subset(iDTNew, select = -Add) 
  }
  
  PriceSeasonality(as.data.frame(iDTNew), IfPlot)
  
  if (IfReturnDT)
    return(iDTNew)
}

