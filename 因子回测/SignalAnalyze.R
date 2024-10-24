### 日期: 2022-12-16
### 该文件用于仓单回测分析
### 作者: HuanfengDong  AnranLiu
### =================================================================================== ### 
### 补充说明：
### 1）单因子分析框架可以作为函数调用，也可以在函数内直接将输入确定下来单文件运行
### 2）返回变量说明（list格式）：FactorAna为因子基本分析，BTResults为回测指标、Return为回测详细日收益、YearSummary为年度表现；调用方法res$
### =================================================================================== ### 
install.packages("lmtest")
### TODO：1)补充简单PNL不同股票池与调仓方式收益;2)未来N日收益分析这里，去掉第一天跳空计算；
source('F:/BackTest/load_library.R', encoding='UTF-8') 
BackTestResult <- function(DT.DailyQuote.Fast, BegDate='2011-01-01', DF.Signal, 
                           EndDate = Sys.Date()-1,feeRatio = 0.002, tradePrice = 'VWAP60',availableFund = 2e6)
{ 
  # 计算一次选股回测结果
  # S1:数据准备-----------------------------------------------------------------------
  #载入行情
  load(paste0(Dir.Data,'MarketIndex.Rdata'))
  DT.Market <- dcast(Data.MarketIndex, TradingDay ~ IndexName, value.var = 'Return')
  DT.Market <- as.data.table(DT.Market)
  #载入数据
  DT.DailyQuote.LastDay <- copy(DT.DailyQuote.Fast[,.(TradingDay, SecuCode, OpenPrice, ClosePrice,PrevClosePrice,
                                                      APrevPrice,AClosePrice,AOpenPrice,TurnoverValue,GZ2000,ZZ500,FirstIndustryCode,
                                                      UpLtd,IfTrade)])
  
  DT.DailyQuote.Final <- merge(DT.DailyQuote.LastDay, DF.Signal[,c('TradingDay','SecuCode','OpenRatio')], 
                               by = c('TradingDay','SecuCode'),all.x = T)
  Ary.AdjustDate <- DF.Signal$TradingDay
  
  DT.DailyQuote.Final[TradingDay %in% Ary.AdjustDate & is.na(OpenRatio), OpenRatio := 0]   #未选中股票生成持仓占比=0
  DT.DailyQuote.Final[TradingDay %in% Ary.AdjustDate & TurnoverValue == 0, OpenRatio := 0]   #停牌股票生成持仓占比=0
  DT.DailyQuote.Final[TradingDay %in% Ary.AdjustDate & UpLtd == 1, OpenRatio := 0]   #去掉涨停的买不进，持仓股票可能小于100只
  DT.DailyQuote.Final[,OpenRatio := na.locf(OpenRatio, na.rm = F, fromLast=F), by='SecuCode']  #填补非换仓日
  
  # S3:向量化回测---------------------------------------------------------------------
  StartDate <- DT.DailyQuote.Final$TradingDay[1]
  availableFund <- availableFund#初始资金
  
  #cat('初始可用资金：',availableFund,'元\n')
  DT.HoldingRecord.History <- data.table()#持仓记录
  DT.HoldingList <- copy(DT.DailyQuote.Final)
  DT.HoldingList <- DT.HoldingList[TradingDay >= StartDate,] #去掉第一次开仓前数据
  DT.HoldingList[TradingDay %in% Ary.AdjustDate, HoldVolume := availableFund*OpenRatio/APrevPrice, by = 'TradingDay'] #生成所有换仓日持股
  DT.HoldingList[, HoldVolume := na.locf(HoldVolume, na.rm = F, fromLast=F), by='SecuCode']  #补全非换仓日
  DT.HoldingList[, HoldVolume := na.fill(HoldVolume, 0), by='SecuCode']  #填补缺失的
  DT.HoldingList[, PrevHold := lag(HoldVolume, 1), by='SecuCode']  #昨日持股数
  DT.HoldingList[, PrevHold := na.fill(PrevHold, 0), by='SecuCode']  #填补lag之后缺失的
  DT.HoldingList[, changehold := HoldVolume-PrevHold, by='SecuCode']  #持股变化
  DT.HoldingList[changehold < 0, cost := abs(changehold*AOpenPrice*feeRatio)]  #单只股票换仓cost,changehold < 0为卖出
  DT.HoldingList[, cost := na.fill(cost, 0)]  #补全cost=0
  DT.HoldingList[, HoldValue := HoldVolume*AClosePrice]   #单只股票价值
  DT.HoldingList[, PrevHoldValue := HoldVolume*APrevPrice]  #单只股票昨日价值
  #非换仓日收益率
  DT.HoldingList[, dayreturn:= (sum(HoldValue-PrevHoldValue,na.rm = T))/sum(PrevHoldValue,na.rm = T), by = 'TradingDay']   #用总股票价值计算收益
  #换仓日收益率
  DT.HoldingList[TradingDay %in% Ary.AdjustDate,return1 := max(changehold,0)*(AClosePrice-AOpenPrice), by = c('TradingDay','SecuCode')]  #买入部分收益
  DT.HoldingList[TradingDay %in% Ary.AdjustDate,return3 := abs(min(changehold,0))*(AOpenPrice-APrevPrice)-cost, by = c('TradingDay','SecuCode')]  #卖出部分收益
  DT.HoldingList[TradingDay %in% Ary.AdjustDate &changehold < 0, return2 := (PrevHold+changehold)*(AClosePrice-APrevPrice)]  #本来持仓部分收益
  DT.HoldingList[TradingDay %in% Ary.AdjustDate &changehold >= 0, return2 := (HoldVolume-changehold)*(AClosePrice-APrevPrice)]  #本来持仓部分收益
  DT.HoldingList[TradingDay %in% Ary.AdjustDate,dayreturn := sum(return1+return2+return3,na.rm = T)/2e6, by = 'TradingDay']  #按单利估算，如果用昨日持仓的价值，可能已经比2e6小很多（持仓小于100的时候）
  #DT.HoldingList[TradingDay %in% Ary.AdjustDate,PrevHoldValue := (PrevHold+min(changehold,0))*APrevPrice+abs(min(changehold,0)*AOpenPrice), by = c('TradingDay','SecuCode')]
  #DT.HoldingList[TradingDay %in% Ary.AdjustDate,dayreturn := sum(return1+return2+return3,na.rm = T)/sum(PrevHoldValue,na.rm = T), by = 'TradingDay']
  #开仓第一天
  DT.HoldingList[TradingDay == StartDate,dayreturn := sum(HoldVolume*(AClosePrice-AOpenPrice),na.rm = T)/sum(HoldVolume*AOpenPrice,na.rm = T)]
  
  # 生成回测日收益、净值等信息
  DT.DayReturn <- DT.HoldingList[, .SD[.N], by = TradingDay]
  DT.DayReturn <- DT.DayReturn[,c('TradingDay','dayreturn')]
  #DT.DayReturn[,Net:= cumprod(dayreturn + 1)]
  DT.DayReturn <- DT.DayReturn[!is.na(dayreturn),]
  DT.DayReturn[,Net:= cumprod(dayreturn + 1)]
  DT.DayReturn[,Net.DailyReturn:= dayreturn]
  DT.Summary<-merge(DT.DayReturn, DT.Market, by = 'TradingDay', all.x = T)
  DT.HoldingRecord = DT.HoldingList[OpenRatio>0,c('TradingDay','SecuCode','OpenRatio')]#持仓记录
  setkey(DT.HoldingRecord, TradingDay, SecuCode)
  setnames(DT.Summary, c("GZ2000", "HS300",'ZZ1000','ZZ2000','ZZ500','ZZ800'), 
           c("GZ2000.DailyReturn", "HS300.DailyReturn",'ZZ1000.DailyReturn','ZZ2000.DailyReturn','ZZ500.DailyReturn','ZZ800.DailyReturn'))
  
  return(list(DT.Summary = DT.Summary   #净值及日收益
              ,DT.HoldingRecord = DT.HoldingRecord  #持仓记录
  ))
  
}


FastSignalBackTest <- function(DT.DailyQuote.Analyze,DF.Signal, BegDate = as.Date('2011-01-01'), EndDate = Sys.Date()-1, 
                               indexBase='ZZ500',feeRatio = 0.002, tradePrice = 'VWAP60', availableFund = 2e6, ifSTIB = F)
{
  #1、Vwap作为调仓交易价格，feeRatio设置千二；
  #2、adjustPara为粘滞系数，可选择：('StockNum200', 'AdjustPara100')，数字0为无粘滞
  DT.DailyQuote.Fast <- copy(DT.DailyQuote.Analyze)
  ##合并Vwap
  {
    # VWAP数据载入改为fst加速
    DT.VWAP <- read.fst(paste0('\\\\192.168.8.38\\shares\\wap_rdata\\tickvwap_first', str_extract(tradePrice , '\\d+'), '.fst'), as.data.table=T)
    setDT(DT.VWAP)
    DT.VWAP[, ':='(SecuCode=as.numeric(SecuCode), TradingDay=as.Date(TradingDay))]
    if (ifSTIB == F){
      DT.VWAP <- DT.VWAP[SecuCode < 688001] # 去掉科创板股票
    }
    setnames(DT.DailyQuote.Fast, 'OpenPrice', 'OpenPrice1')
    DT.DailyQuote.Fast <- merge(DT.DailyQuote.Fast, DT.VWAP, by=c('TradingDay','SecuCode'),all.x=T)
    DT.DailyQuote.Fast[is.na(OpenPrice), OpenPrice:=OpenPrice1] # 处理VWAP缺失部分数据
    DT.DailyQuote.Fast[OpenPrice == 0, OpenPrice:=OpenPrice1] # 处理VWAP为0的部分数据
    DT.DailyQuote.Fast[OpenPrice == 0, OpenPrice:=PrevClosePrice] # 处理日度数据OpenPrice也为0的部分数据
    DT.DailyQuote.Fast[,OpenPrice1:=NULL]
    
  }
  
  ##复权RatioAdjustingFactor
  {
    load(paste0(Dir.Data,'AdjustingFactor.Rdata'))
    Data.AdjustingFactor[, PrevAdj := lag(RatioAdjustingFactor,1), by = 'SecuCode']
    Data.AdjustingFactor[, PrevAdj:=na.fill(PrevAdj, 1)]
    Data.AdjustingFactor[, RatioAdjustingFactor := RatioAdjustingFactor/PrevAdj, by = 'SecuCode']
    Data.AdjustingFactor[, c("RatioAdjustingFactor") :=  lapply(.SD,  function(x) ifelse(x < 0, 0, x))
                         , .SDcols = c("RatioAdjustingFactor"),by='SecuCode']
    DT.DailyQuote.Fast <- merge(DT.DailyQuote.Fast
                                , Data.AdjustingFactor[,.(TradingDay, SecuCode, RatioAdjustingFactor)]
                                , by= c('TradingDay','SecuCode')
                                , all.x =T) ## 修改all为all.x
    DT.DailyQuote.Fast[, c("RatioAdjustingFactor") := lapply(.SD, na.locf, na.rm=F)
                       , .SDcols = c("RatioAdjustingFactor"),by='SecuCode']
    
    DT.DailyQuote.Fast[, RatioAdjustingFactor:=na.fill(RatioAdjustingFactor, 1)]
    # DT.DailyQuote.Fast <- DT.DailyQuote.Fast[!is.na(ClosePrice)] 
    DT.DailyQuote.Fast[, AClosePrice := ClosePrice * RatioAdjustingFactor]
    DT.DailyQuote.Fast[, AOpenPrice := OpenPrice * RatioAdjustingFactor]
    DT.DailyQuote.Fast[, APrevPrice := PrevClosePrice * RatioAdjustingFactor]   
    DT.DailyQuote.Fast[, c("RatioAdjustingFactor") := NULL]
  }
  
  
  Result <- data.table()
  DT.ReturnAll <- data.table()
  
  R <- BackTestResult(DT.DailyQuote.Fast, DF.Signal, BegDate = BegDate, EndDate = EndDate, feeRatio = feeRatio, availableFund = availableFund)
  
  DT.Return <- R$DT.Summary[,.(TradingDay, Strategy=Net.DailyReturn, 
                               ZZ500=ZZ500.DailyReturn, ZZ500Hedged=Net.DailyReturn-ZZ500.DailyReturn,
                               ZZ1000=ZZ1000.DailyReturn, ZZ1000Hedged=Net.DailyReturn-ZZ1000.DailyReturn,
                               GZ2000=GZ2000.DailyReturn, GZ2000Hedged=Net.DailyReturn-GZ2000.DailyReturn)]
  DT.Return <- DT.Return[!is.na(Strategy),]
  R$DT.Return <- DT.Return
  
  Expr <- paste0("DT.Return1 <- DT.Return[,.(TradingDay, CumProdReturn=cumprod(1 + ", str_to_upper(indexBase), "Hedged))]")
  eval(parse(text = Expr))
  
  # 年度收益
  DT.Temp <- GetProfitSummary2(DT.Return1, DeleteZero = F)
  DT.Temp1 <- GetProfitSummary2(DT.Return1[TradingDay>'2019-01-01'],DeleteZero = F)
  
  DT.YearSummary <- DT.Return1[,.(TradingDay, CumProdReturn, Year=year(TradingDay))]
  DT.YearSummary <- DT.YearSummary[, GetProfitSummary(CumProdReturn, DeleteZero = F),by='Year']
  DT.YearSummary <- DT.YearSummary[,.(Year, AnnualReturn=round(AnnualReturn,3), SharpeRatio = round(SharpeRatio,3), 
                                      MaxDrawdown = round(MaxDrawdown,3))]
  
  # 换手率计算
  DF.Signal.Open <- R$DT.HoldingRecord
  TradingDay.Open <- unique(DF.Signal.Open$TradingDay)
  TradingDay.Open <- sort(TradingDay.Open)
  DF.Signal.Close <- copy(DF.Signal.Open)
  DF.Signal.Close[,':='(
    OpenRatio = 0
    , TradingDay = TradingDay.Open[match(TradingDay, TradingDay.Open)+1]
  )]
  DF.Signal.Open[,Tag:=1]
  DF.Signal.Close[,Tag:=0]
  ## 并入开仓，平仓，退市信
  DF.Signal <- rbind(DF.Signal.Open,DF.Signal.Close)
  setorder(DF.Signal,TradingDay,SecuCode,-Tag)
  DF.Signal[,Rowid:=rowidv(.SD,cols=c('TradingDay','SecuCode'))]
  DF.Signal <- DF.Signal[Rowid == 1,.(TradingDay,SecuCode,OpenRatio)]
  setkey(DF.Signal,TradingDay,SecuCode)
  DF.Signal[,Order:=1:.N] 
  DF.Signal <- DF.Signal[!is.na(TradingDay)]
  DT.ChangeRatio <-  DF.Signal
  DT.ChangeRatio[, PreHold := lag(OpenRatio,1),by = c('SecuCode')]
  DT.ChangeRatio[, PreHold := na.fill(PreHold,0)]
  DT.ChangeRatio[, DiffHold := OpenRatio - PreHold]
  DT.ChangeRatio <- DT.ChangeRatio[DiffHold > 0,.(ChangeRatio = sum(DiffHold, na.rm= T)),by='TradingDay']
  
  DT.Results <- data.table(StrategyName='DF.Siganl',
                           AR=round(DT.Temp$AnnualReturn[1], 3),
                           SR=round(DT.Temp$SharpeRatio[1],2),
                           MDD=round(DT.Temp$MaxDrawdown[1],3),
                           YearTO=round(mean(DT.ChangeRatio$ChangeRatio, na.rm=T) * 250,2),
                           AR19=round(DT.Temp1$AnnualReturn[1], 3),
                           SR19=round(DT.Temp1$SharpeRatio[1], 3),
                           MDD19=round(DT.Temp1$MaxDrawdown[1],3),
                           AR18Y=round(DT.YearSummary[Year == 2018, AnnualReturn],3),
                           AR19Y=round(DT.YearSummary[Year == 2019, AnnualReturn],3),
                           AR20Y=round(DT.YearSummary[Year == 2020, AnnualReturn],3),
                           AR21Y=round(DT.YearSummary[Year == 2021, AnnualReturn],3),
                           AR22Y=round(DT.YearSummary[Year == 2022, AnnualReturn],3),
                           AR23Y=round(DT.YearSummary[Year == 2023, AnnualReturn],3),
                           AR24Y=round(DT.YearSummary[Year == 2024, AnnualReturn],3),
                           MDDFrom=DT.Temp$From[1],
                           To=DT.Temp$To[1],
                           Params=paste(format(as.Date(BegDate),'%Y%m%d'), format(as.Date(EndDate),'%Y%m%d'),
                                        indexBase,tradePrice,ifSTIB,sep='_'))
  print(DT.Results)
  
  #返回
  BestResult <- list(DT.Results = DT.Results
                     ,DT.Return =  R$DT.Return
                     ,DT.HoldingRecord=R$DT.HoldingRecord[,c('TradingDay','SecuCode','OpenRatio')])  #持仓
  return(BestResult)
}

SignalAnalyze <- function(loadMode='Local',fileName, DF.Signal, indexBase='ZZ500', 
                          BTstartDate='2011-01-01', BTenddate=Sys.Date()-1, ifDetail=1, ifSTIB=F){
  
  gc()
  # source('./RHeadFiles/load_library.R', encoding='UTF-8') 
  
  # 回测参数说明
  if (F){ 
    loadMode <- 'Local'        # 可输入Local/SQL，基础计算数据的载入模式，默认Local直接从Share里面取现成数据；SQL为调用数据库数据
    indexBase <- 'ZZ500'       # 基准指数 支持：ZZ1000/ZZ500/GZ2000
    fileName <- 'test'         # 名称
    BTstartDate <- '2011-01-01'# 回测开始时间（需要存在该时段因子）
    BTenddate <- Sys.Date()-1  # Sys.Date()-1  # 回测结束时间（需要存在该时段仓单）'2023-05-30'
    ifDetail <- 1              # 1为使用快速简版回测，2为详细回测
    ifSTIB <- F                # ifSTIB表示因子是否包含科创板股票，默认是否
    load('F:/others/组合优化回测示例代码/Data/TestSignal1.Rdata') #DF.Signal
  }
  
  if (ifSTIB)
    Dir.Data <- Dir.DataSTIB
  
  # S1:数据载入 -----------------------------------------------------------------
  if (loadMode != 'Local'){
    ## 数据库基础数据
    {
      ###载入行情数据Data.DailyQuote
      load(paste0(Dir.Data,'DailyQuote.Rdata'))
      setkey(Data.DailyQuote,TradingDay,SecuCode)
      
      ###载入交易日数据Data.TradingDay
      load(paste0(Dir.Data,'TradingDay.Rdata'))
      Ary.TradingDay <- Data.TradingDay
      
      ###载入ZZ500 ZZ1000 GZ2000行业权重、成分股
      for (iindex in c('ZZ500', 'ZZ1000', 'GZ2000')){
        # 载入权重
        load(paste0(Dir.Data, iindex, 'IndustryWeight.Rdata'))
        Expr <- paste0("Data.", iindex, "IndustryWeight[,IndBase:=paste0('IndBase',FirstIndustryCode)]")
        eval(parse(text = Expr))
        Expr <- paste0("DT.",iindex,"IndustryWeight<-dcast(Data.",iindex,"IndustryWeight,TradingDay~IndBase,value.var='IndustryWeight')")
        eval(parse(text = Expr))
      }
      
      ###载入市场基准指数数据Data.MarketIndex
      load(paste0(Dir.Data,'MarketIndex.Rdata'))
      DT.MarketIndex <- dcast(Data.MarketIndex, TradingDay ~ IndexName, value.var = 'Return')
      setkey(DT.MarketIndex,TradingDay)
      
      ###载入退市最后一个交易日数据Data.DelistLastQuote
      load(paste0(Dir.Data,'DelistLastQuote.Rdata'))
      ###载入资产负债表数据Data.BalanceSheetAll
      load(paste0(Dir.Data,'BalanceSheetAll.Rdata'))
      Data.BalanceSheetAll[, TradingDay := GetTradingDayLeadLag(TradingDay, Data.TradingDay, 1)]
      ###载入利润表数据Data.IncomeStatementAll
      load(paste0(Dir.Data,'IncomestatementAll.Rdata'))
      Data.IncomeStatementAll[, TradingDay := GetTradingDayLeadLag(TradingDay, Data.TradingDay, 1)]
      ###载入现金流量表数据Data.CashFlowstatementAll
      load(paste0(Dir.Data,'CashFlowstatementAll.Rdata'))
      Data.CashFlowStatementAll[, TradingDay := GetTradingDayLeadLag(TradingDay, Data.TradingDay, 1)]
      ###载入行业分类数据Data.ExgIndustry
      load(paste0(Dir.Data,'ExgIndustry.Rdata'))
      ## FirstIndustry
      Data.DailyQuote <- Combine.ExgIndustry(Data.DailyQuote, Data.ExgIndustry, IndustryLevel = 1)#选择一级行
      ###载入股本结构变动数据Data.ShareStru
      load(paste0(Dir.Data,'ShareStru.Rdata'))
      ###载入行业数据Data.IndustryIndex
      load(paste0(Dir.Data,'IndustryIndex.Rdata'))
      ###载入个股表现数据
      load(paste0(Dir.Data,'PerformanceData.Rdata'))
      ##载入复牌数据
      load(paste0(Dir.Data,'SuspendResumptionDT.Rdata')) # 注意这里是清洗后的复牌数据
      ## 读取复权因子数据
      load(paste0(Dir.Data,'AdjustingFactor.Rdata'))
      ## 市场Return
      Data.DailyQuote <- merge(Data.DailyQuote, DT.MarketIndex, by='TradingDay',all.x=T)
      ## Return & Return.H & Return.Ind
      Data.DailyQuote[, Return := ClosePrice / PrevClosePrice - 1]
      for (iindex in c('ZZ500', 'ZZ1000', 'GZ2000')){
        setnames(Data.DailyQuote, iindex, paste0('Return.',iindex))
        
        Expr <- paste0("Data.DailyQuote[, Return.H.",iindex, " := ifelse(OpenPrice == 0, 0, Return- Return.", iindex, ")]")
        eval(parse(text = Expr))
        # setnames(Data.DailyQuote, 'ZZ1000', 'Return.ZZ1000')
        # Data.DailyQuote[, Return.H := ifelse(OpenPrice == 0, 0, Return- Return.ZZ1000)]
        
        # 成分股合并
        # 载入成分股
        load(paste0(Dir.Data, iindex, 'Stock.Rdata'))
        load(paste0(Dir.Data, iindex, 'StockWeight.Rdata'))
        
        Data.DailyQuote[, c(iindex):=NULL]
        Expr <- paste0("Data.DailyQuote <- merge(Data.DailyQuote, Data.", iindex, "Stock, by=c('TradingDay','SecuCode'),all.x=T)")
        eval(parse(text = Expr))
        Expr <- paste0("Data.DailyQuote[, ", iindex, ":= na.fill(", iindex, ", 0)]")
        eval(parse(text = Expr))
      }
      
      # Return.H 超额收益更新
      Expr <- paste0("Data.DailyQuote[, Return.H := Return.H.", indexBase, "]")
      eval(parse(text = Expr))
      
      Data.DailyQuote <- merge(Data.DailyQuote, Data.IndustryIndex[,.(TradingDay, FirstIndustryCode, Return.Ind = Return)]
                               , by=c('TradingDay','FirstIndustryCode'),all.x=T) # 并入行业Return
      Data.DailyQuote[,Return.H.Ind := ifelse(OpenPrice == 0, NA, Return- Return.Ind)]
      ## 黑名单数
      load(paste0(Dir.Data,'BlackList.Rdata'))
      Data.DailyQuote <- merge(Data.DailyQuote, Data.BlackList, by = c('TradingDay','SecuCode'), all.x = T)
      
      ## 数据处理 
      ## 时间缩小
      DT.DailyQuote <- Data.DailyQuote[TradingDay > as.Date('2010-01-01')] # '2005-01-01'
      DT.DailyQuote <- DT.DailyQuote[TradingDay <= as.Date(BTenddate)] # '2005-01-01'
      
      ## ResumptionDate
      DT.DailyQuote <- merge(DT.DailyQuote, 
                             DT.SuspendResumption[, .(TradingDay = ResumptionDate, SecuCode, ResumptionDate)],
                             by=c('TradingDay','SecuCode'), all.x=T)
      DT.DailyQuote[, ResumptionDate := na.locf(ResumptionDate, na.rm = F), by='SecuCode']
      DT.DailyQuote[, ResumptionDayCount := GetTradingDayDiff(ResumptionDate,TradingDay,Data.TradingDay)]
      
    }
    
    ## 基础因子
    {
      # DT.DailyQuote[, Mom21 := RollMeanNoLeast(ifelse(OpenPrice!=0, Return, NA), 21),by='SecuCode']
      DT.DailyQuote[, Mom63.H := RollMeanNoLeastCpp(ifelse(OpenPrice!=0, Return.H, NA), 63),by='SecuCode']
      DT.DailyQuote[, Std21.H := RollStdNoLeastCpp(ifelse(OpenPrice!=0, Return.H, NA), 21),by='SecuCode']
      DT.DailyQuote[, RTV21 := RollMeanNoLeastCpp(ifelse(OpenPrice!=0, TurnoverValue, NA), 21),by='SecuCode']
      ## 一字涨跌停及停
      DT.DailyQuote <- merge(DT.DailyQuote, Data.PerformanceData[,.(TradingDay, SecuCode, UpLtd = StockBoard, DownLtd = LimitBoard, 
                                                                    SurgedLimit, DeclineLimit)],by=c('TradingDay', 'SecuCode'), all.x=T)
      
      rm(Data.PerformanceData)
      DT.DailyQuote[, TrdHalt := ifelse(OpenPrice == 0, 1, 0)]
      ## HBeta, RVar, Size
      # load(paste0(Dir.Data, 'HBeta.Rdata'))
      DT.HBeta <- read.fst(paste0(Dir.Data,'HBeta.fst'), as.data.table=T)
      DT.DailyQuote <- merge(DT.DailyQuote, DT.HBeta[,.(TradingDay= GetTradingDayLeadLag(TradingDay, Data.TradingDay, 1)
                                                        , SecuCode = as.numeric(SecuCode), HBeta = Factor)], by=c('TradingDay','SecuCode'),all.x=T)
      rm(DT.HBeta)
      # load(paste0(Dir.Data, 'RVar.Rdata'))
      DT.RVar <- read.fst(paste0(Dir.Data,'RVar.fst'), as.data.table=T)
      DT.DailyQuote <- merge(DT.DailyQuote, DT.RVar[,.(TradingDay= GetTradingDayLeadLag(TradingDay, Data.TradingDay, 1)
                                                       , SecuCode = as.numeric(SecuCode), RVar = Factor)], by=c('TradingDay','SecuCode'),all.x=T)
      rm(DT.RVar)
      # load(paste0(Dir.Data, 'Size.Rdata'))
      DT.Size <- read.fst(paste0(Dir.Data,'Size.fst'), as.data.table=T)
      DT.DailyQuote <- merge(DT.DailyQuote, DT.Size[,.(TradingDay= GetTradingDayLeadLag(TradingDay, Data.TradingDay, 1)
                                                       , SecuCode = as.numeric(SecuCode), Size = Factor)], by=c('TradingDay','SecuCode'),all.x=T)
      
      # load(paste0(Dir.Data, 'Factor3.Rdata'))
      DT.Factor3 <- read.fst(paste0(Dir.Data,'Factor3.fst'), as.data.table=T)
      DT.DailyQuote <- merge(DT.DailyQuote, DT.Factor3[,.(TradingDay= GetTradingDayLeadLag(TradingDay, Data.TradingDay, 1)
                                                          , SecuCode = as.numeric(SecuCode), Value = Factor)], by=c('TradingDay','SecuCode'),all.x=T)
      rm(DT.Factor3)
      
      rm(DT.Size)
    }
    
    ## Size相关
    {
      # 总市值
      DT.DailyQuote[, TotalShares := NULL]
      DT.DailyQuote <- Combine.ShareStru(DT.DailyQuote, Data.ShareStru, 'TotalShares')
      DT.DailyQuote[, TotalMV := TotalShares * ClosePrice]
      ## 流通市值
      Data.ShareStru[is.na(NonRestrictedShares), NonRestrictedShares := AFloatListed]
      DT.DailyQuote[, NonRestrictedShares := NULL]
      DT.DailyQuote <- Combine.ShareStru(DT.DailyQuote, Data.ShareStru, 'NonRestrictedShares')
      DT.DailyQuote[, NegotiableMV := NonRestrictedShares * ClosePrice]
      ## 总资产
      DT.DailyQuote[, TotalAssets := ReportData2TradingDayData(TradingDay,GetReportData(Data.BalanceSheetAll,'TotalAssets'),'TotalAssets',SecuCode)]
    }
    
    ## 风险因子
    {
      DT.DailyQuote[, Mom := Mom63.H]
      DT.DailyQuote[is.infinite(Mom) | is.na(Mom), Mom := NA]
      DT.DailyQuote[!is.na(Mom), Mom := scale(Mom), by = c('TradingDay', 'FirstIndustryCode')]
      DT.DailyQuote[, Mom := ifelse(abs(Mom)>3, 3*sign(Mom), Mom)]
      
      DT.DailyQuote[, RVar := RVar]
      DT.DailyQuote[, Size := Size]
      DT.DailyQuote[, HBeta := HBeta]
      Ary.StyleFactor <- c('Mom','Size','RVar','HBeta')
    }
    
    #未来收益及其他辅助数据
    {
      DT.DailyQuote[,N:=.N, by='SecuCode']
      DT.DailyQuote[, Profit1.H := RollSumNoLeastCpp(Return.H, 1),by='SecuCode']
      DT.DailyQuote[, Profit1.H.Fut := c(Profit1.H[-(1)], array(0,1)),by='SecuCode']
      DT.DailyQuote[, Profit2.H := RollSumNoLeastCpp(Return.H, 2),by='SecuCode']
      DT.DailyQuote[N>=2, Profit2.H.Fut := c(Profit2.H[-(1:2)], array(0,2)),by='SecuCode']
      
      DT.DailyQuote[, Profit5.H := RollSumNoLeastCpp(Return.H, 5),by='SecuCode']
      DT.DailyQuote[N>=5, Profit5.H.Fut := c(Profit5.H[-(1:5)], array(0,5)),by='SecuCode']
      DT.DailyQuote[, Profit21.H := RollSumNoLeastCpp(Return.H, 21),by='SecuCode']
      DT.DailyQuote[N>=21, Profit21.H.Fut := c(Profit21.H[-(1:21)], array(0,21)),by='SecuCode']
      DT.DailyQuote[, Profit42.H := RollSumNoLeastCpp(Return.H, 42),by='SecuCode']
      DT.DailyQuote[N>=42, Profit42.H.Fut := c(Profit42.H[-(1:42)], array(0,42)),by='SecuCode']
      DT.DailyQuote[N>=80, Profit80.H.Fut := c(Profit42.H[-(1:80)], array(0,80)),by='SecuCode']
      DT.DailyQuote[N>=5, TradingDay5 := c(TradingDay[-(1:5)], as.Date(array(0,5))),by='SecuCode'] # 加入了array的as date转换
      DT.DailyQuote[, c('Profit5.H','Profit42.H') := NULL]
      DT.DailyQuote[, N := 1:.N, by='SecuCode']
      gc()
    }
  }else{
    load(paste0(Dir.Data, 'RDataForFactorAnalyze_', indexBase,'.RData'))
    DT.DailyQuote <- read.fst(paste0(Dir.Data, 'RDataForFactorAnalyze_', indexBase,'.fst'), as.data.table=T)
    # DT.DailyQuote[,SecuAbbr:=NULL]
    Ary.TradingDay <- Data.TradingDay
    DT.DailyQuote <- DT.DailyQuote[TradingDay <= as.Date(BTenddate)]
  }
  
  
  pdf(file.path(Dir.Research, 'SingleFactorAnalyze', paste0(fileName, '_Small', '_', indexBase, '_', format(Sys.Date(), '%Y%m%d'), ".pdf")),width = 12,height = 7, family="GB1")
  
  
  # S2:仓单回测 -----------------------------------------------------------------

  DT.Results <- data.table() # 回测表现
  DT.Return <- data.table() # 收益率序列
  
  if(ifDetail == 1){
    DT.DailyQuote.Analyze <- DT.DailyQuote
    # S2.1:快速向量化回测 -----------------------------------------------------------------
    FastR <- FastSignalBackTest(DT.DailyQuote.Analyze,Data, BegDate =as.Date(BTstartDate), EndDate = as.Date(BTenddate),
                                indexBase='ZZ500',feeRatio = 0.002, tradePrice = 'VWAP60', availableFund = 2e6, ifSTIB = F)

    DT.Results <- copy(FastR$DT.Results) #组合情况，包含AR,SR,MDD等
    DT.ReturnAll <- FastR$DT.ReturnAll #日收益及净值
    DT.Return <- FastR$DT.Return 
    DT.HoldingRecord <- FastR$DT.HoldingRecord #组合持仓股票与权重
    # fwrite(DT.Return[,.(TradingDay, ZZ500Hedged)], file=paste0('./Analysis/PnLData/Test/PNL-', fileName, 'Fast.csv'))
    
    # 超额画图 
    {
      Today <- Sys.Date()-1
      TradingDay.OneMonth <- GetTradingDayLeadLag(Today, Data.TradingDay, 21)
      TradingDay.HalfYear <- GetTradingDayLeadLag(Today, Data.TradingDay, 125)
      TradingDay.OneYear <- GetTradingDayLeadLag(Today, Data.TradingDay, 252)
      
      Expr <- paste0("DT.Return[,CumHedge:=1+cumsum(Strategy - ", indexBase, ")]")
      eval(parse(text = Expr))
      
      p <- DFplot(DT.Return[,.(TradingDay, CumHedge)],  
                  Xlabel ='日期', Ylabel ='对冲净值Cumsum', Xbreaks = '8 week', Title =paste0('Fast对冲净值',DT.Results$StrategyName))
      
      DT.Return.Graph <- DT.Return[TradingDay > TradingDay.OneYear]
      DT.Return.Graph <- DT.Return.Graph[, c(list(TradingDay = TradingDay), lapply(.SD, function(x){return(x-first(x)+1)})),
                                         .SDcols=c('CumHedge')]
      p <- DFplot(DT.Return.Graph,  
                  Xlabel = '日期', Ylabel = '最近一年对冲净值Cumsum', Xbreaks = '1 week', Title = paste0('Fast对冲净值',DT.Results$StrategyName))
      
      DT.Return.Graph <- DT.Return[TradingDay > TradingDay.HalfYear]
      DT.Return.Graph <- DT.Return.Graph[, c(list(TradingDay = TradingDay), lapply(.SD, function(x){return(x-first(x)+1)})),
                                         .SDcols=c('CumHedge')]
      p <- DFplot(DT.Return.Graph,  
                  Xlabel = '日期', Ylabel = '最近半年对冲净值Cumsum', Xbreaks = '1 week', Title = paste0('Fast对冲净值',DT.Results$StrategyName))
    }
    
  }
  if(ifDetail == 2){
    # S2.2:详细回测-按OnBar考虑停牌等 -------------------------------------------------------------
    
    # onbar回测
    R <- BackTest(DF.Signal,  BegDate =as.Date(BTstartDate), EndDate = as.Date(BTenddate), 
                  FeeRatio = 0.002, TradePrice = 'VWAP60', IfSTIB=ifSTIB)
    
    DT.HoldingRecord <- R$DT.HoldingRecord.History[,.(TradingDay, SecuCode, HoldShares)]
    ## Return数据处理
    DT.Return <- data.table(TradingDay = R$DT.Summary$TradingDay)
    DT.Return$Strategy <- R$DT.Summary$Net.DailyReturn
    DT.Return$ZZ500 <- R$DT.Summary$ZZ500.DailyReturn * R$DT.Summary$Account.Holding/R$DT.Summary$Account.Total
    DT.Return$ZZ1000 <- R$DT.Summary$ZZ1000.DailyReturn * R$DT.Summary$Account.Holding/R$DT.Summary$Account.Total
    DT.Return$GZ2000 <- R$DT.Summary$GZ2000.DailyReturn * R$DT.Summary$Account.Holding/R$DT.Summary$Account.Total
    
    DT.Return[,':='(ZZ500Hedged = Strategy - ZZ500,
                    ZZ1000Hedged = Strategy - ZZ1000,
                    GZ2000Hedged = Strategy - GZ2000
    )]
    
    ## 换手率统计
    DT.ChangeRatio <- R$DT.HoldingRecord.History[,.(ChangeRatio = sum(TodayShares * TodayClosePrice, na.rm= T)
                                                    / sum(HoldShares * TodayClosePrice, na.rm = T)),by='TradingDay']
    cat('年化换手单边: ', mean(DT.ChangeRatio$ChangeRatio, na.rm=T) * 250, '\n')
    
    # 超额画图
    Today <- Sys.Date()
    TradingDay.OneMonth <- GetTradingDayLeadLag(Today, Data.TradingDay, 21)
    TradingDay.HalfYear <- GetTradingDayLeadLag(Today, Data.TradingDay, 125)
    TradingDay.OneYear <- GetTradingDayLeadLag(Today, Data.TradingDay, 252)
    
    DT.Return$StrategyNet <- 1+cumsum(DT.Return$Strategy)
    p <- DFplot(DT.Return[, c('TradingDay','StrategyNet')],  # 多头收益
                Xlabel = '日期', Ylabel = '纯多头净值（cumsum）', Xbreaks = '8 week', Title = '策略多头净值')
    
    Expr <- paste0("DT.Return[,CumHedge:=1+cumsum(Strategy - ", str_to_upper(indexBase), ")]")
    eval(parse(text = Expr))
    
    
    p <- DFplot(DT.Return[,.(TradingDay, CumHedge)],  # 对冲收益
                Xlabel = '日期', Ylabel = '对冲净值Cumsum', Xbreaks = '8 week', Title = '策略对冲净值')
    
    DT.Return.Graph <- DT.Return[TradingDay > TradingDay.OneYear]
    DT.Return.Graph <- DT.Return.Graph[, c(list(TradingDay = TradingDay), lapply(.SD, function(x){return(x-first(x)+1)})),
                                       .SDcols=c('CumHedge')]
    p <- DFplot(DT.Return.Graph,  # 对冲收益
                Xlabel = '日期', Ylabel = '对冲净值Cumsum', Xbreaks = '1 week', Title = '一年策略对冲净值')
    
    DT.Return.Graph <- DT.Return[TradingDay > TradingDay.HalfYear]
    DT.Return.Graph <- DT.Return.Graph[, c(list(TradingDay = TradingDay), lapply(.SD, function(x){return(x-first(x)+1)})),
                                       .SDcols=c('CumHedge')]
    p <- DFplot(DT.Return.Graph,  # 对冲收益
                Xlabel = '日期', Ylabel = '对冲净值Cumsum', Xbreaks = '1 week', Title = '六个月策略对冲净值')
    
    # 详细回测指标输出
    # GetProfitSummary(cumprod(1+DT.Return$Strategy), DeleteZero = F)
    # GetProfitSummary(cumprod(1+DT.Return$Strategy - DT.Return$ZZ500), DeleteZero = F)
    
    Expr <- paste0("DT.Return1 <- DT.Return[,.(TradingDay, CumProdReturn=cumprod(1 + ", str_to_upper(indexBase), "Hedged))]")
    eval(parse(text = Expr))
    
    DT.Temp <- GetProfitSummary2(DT.Return1, DeleteZero = F)
    DT.Temp1 <- GetProfitSummary2(DT.Return1[TradingDay>'2019-01-01'],DeleteZero = F)
    DT.Temp2 <- GetProfitSummary2(DT.Return1[TradingDay>=Sys.Date()-365],DeleteZero = F)
    DT.Temp3 <- GetProfitSummary2(DT.Return1[TradingDay>='2023-01-01'],DeleteZero = F)
    #各个年份的超额
    DT.YearSummary <- DT.Return1[,.(TradingDay, CumProdReturn, Year=year(TradingDay))]
    DT.YearSummary <- DT.YearSummary[, GetProfitSummary(CumProdReturn, DeleteZero = F),by='Year']
    DT.YearSummary <- DT.YearSummary[,.(Year, AnnualReturn=round(AnnualReturn,3), SharpeRatio = round(SharpeRatio,3), 
                                        MaxDrawdown = round(MaxDrawdown,3))]
    # print(DT.Temp)
    DT.Results <- data.table(StrategyName=fileName,
                             AR=round(DT.Temp$AnnualReturn[1], 3),
                             SR=round(DT.Temp$SharpeRatio[1],2),
                             MDD=round(DT.Temp$MaxDrawdown[1],3),
                             YearTO=round(mean(DT.ChangeRatio$ChangeRatio, na.rm=T) * 250,2),
                             AR19=round(DT.Temp1$AnnualReturn[1],3),
                             AR1Y=round(DT.Temp2$AnnualReturn[1],3),
                             AR23=round(DT.Temp3$AnnualReturn[1],3),
                             SR19=round(DT.Temp1$SharpeRatio[1],2),
                             MDD19=round(DT.Temp1$MaxDrawdown[1],3),
                             AR19Y=round(DT.YearSummary[Year == 2019, AnnualReturn],3),
                             AR20Y=round(DT.YearSummary[Year == 2020, AnnualReturn],3),
                             AR21Y=round(DT.YearSummary[Year == 2021, AnnualReturn],3),
                             AR22Y=round(DT.YearSummary[Year == 2022, AnnualReturn],3),
                             AR23Y=round(DT.YearSummary[Year == 2023, AnnualReturn],3),
                             AR24Y=round(DT.YearSummary[Year == 2024, AnnualReturn],3),
                             From=DT.Temp$From[1],
                             To=DT.Temp$To[1],
                             From19=DT.Temp1$From[1],
                             To19=DT.Temp1$To[1])
    print(DT.Results)
    
    ## 分年度指标及上一期对比
    {
      DT.YearSummary <- DT.Return1[,.(TradingDay, CumProdReturn, Year=year(TradingDay))]
      DT.YearSummary <- DT.YearSummary[, GetProfitSummary(CumProdReturn, DeleteZero = F),by='Year']
      DT.YearSummary <- DT.YearSummary[,.(Year, AnnualReturn=round(AnnualReturn,3), SharpeRatio = round(SharpeRatio,3), 
                                          MaxDrawdown = round(MaxDrawdown,3))]
      
      print(paste0('2019年以来资金持仓有效时间比例:', round(nrow(DT.Return[year(TradingDay)>=2019 & Strategy!=0]) /
                                                 nrow(DT.Return1[year(TradingDay)>=2019]),3)))
      
      DT.YearSummary
      # if(exists('DT.YearSummary.Last')){
      #   DT.YearSummary <- merge(DT.YearSummary, DT.YearSummary.Last[,.(Year, AnnualReturn.Diff = AnnualReturn, 
      #                                                                  SharpeRatio.Diff = SharpeRatio,
      # MaxDrawdown.Diff = MaxDrawdown)], by='Year', all.x=T)
      #   DT.YearSummary[, ":="(AnnualReturn.Diff = AnnualReturn - AnnualReturn.Diff
      #                         , SharpeRatio.Diff = SharpeRatio - SharpeRatio.Diff
      #                         , MaxDrawdown.Diff = MaxDrawdown - MaxDrawdown.Diff)]
      # }
      # DT.YearSummary.Last <- copy(DT.YearSummary)
    }
    
    ## 分月度指标及上一期对比
    {
      DT.MonthSummary <- copy(DT.Return)
      DT.MonthSummary[, Year := year(TradingDay)]
      DT.MonthSummary[, Month := month(TradingDay)]
      DT.MonthSummary[, c('AnnualReturn','SharpeRatio','MaxDrawdown'):=GetProfitSummary(cumprod(1+Strategy - ZZ500), DeleteZero = F),
                      by=c('Year','Month')]
      DT.MonthSummary <- unique(DT.MonthSummary[,.(Year,Month, AnnualReturn = na.fill(round(AnnualReturn,3),0), 
                                                   SharpeRatio = na.fill(round(SharpeRatio,3),0), 
                                                   MaxDrawdown = na.fill(round(MaxDrawdown,3),0))])
      # if(exists('DT.MonthSummary.Last')){
      #   DT.MonthSummary <- merge(DT.MonthSummary, DT.MonthSummary.Last[,.(Year,Month, AnnualReturn.Diff = AnnualReturn, 
      #                            SharpeRatio.Diff = SharpeRatio, MaxDrawdown.Diff = MaxDrawdown)], by=c('Year','Month'), all.x=T)
      #   DT.MonthSummary[, ":="(AnnualReturn.Diff = AnnualReturn - AnnualReturn.Diff
      #                          , SharpeRatio.Diff = SharpeRatio - SharpeRatio.Diff
      #                          , MaxDrawdown.Diff = MaxDrawdown - MaxDrawdown.Diff)]
      # }
      # DT.MonthSummary.Last <- copy(DT.MonthSummary)
      DT.MonthSummary <- DT.MonthSummary[SharpeRatio!=0]
      
      
      ## 月度箱线
      p <- (ggplot(DT.MonthSummary,aes(x=as.factor(Month), y=AnnualReturn))
            + geom_boxplot()
            + labs(x='月', y='收益率',title ='季节性表现分析'))
      print(p)
      
    }
    
    DT.ReturnAll <- copy(DT.Return)
    # 回测结果保存
    # save(R, ...
  }
 
  dev.off()
  print(DT.Results)
  # save(DT.FactorAna, DT.ResultsAll, DT.Results, DT.Return, DT.HoldingRecord,
  #      file = file.path(Dir.Research, paste('SingleFactorAnalyze/', 
  #      fileName, 'Holding', stockNum, blackList, 'Small',ifSmall, indexBase, format(Sys.Date(), '%Y%m%d'), ".RData", sep='_')))
  
  return(list(BTResults=DT.Results, Return=DT.Return,HoldingRecord=DT.HoldingRecord))
}




library(data.table)
load("C:/Users/po/Desktop/组合优化/equal.weight.new.Rdata")
load("C:/Users/po/Desktop/组合优化/Data.ceshi12_10.Rdata")


Data
Data = read.csv("C:/Users/po/Desktop/组合优化/sss.csv")
Data = read.csv("C:/Users/po/Desktop/组合优化/answer2.csv")
Data = read.csv("C:/Users/po/Desktop/组合优化/answer_dev_size0.8.csv")
Data = read.csv("C:/Users/po/Desktop/组合优化/head100.csv")
{
  setDT(Data)
  Data[,SecuCode := as.integer(SecuCode)]
  Data[,TradingDay:= as.Date(TradingDay)]
  Data <- Data[,.(TradingDay,SecuCode,OpenRatio)]
  Data[TradingDay=='2024-1-30']
  Data <- Data[TradingDay>"2024-01-28"]
  Data <- Data[TradingDay<"2024-02-08"]
  res1 <- SignalAnalyze(loadMode='Local',fileName='opt0',Data,indexBase='ZZ500',BTstartDate="2024-01-28", BTenddate="2024-02-08", ifDetail=1, ifSTIB = F)
  
}
Data[OpenRatio!=0]

head(Data,100)
Data[TradingDay=="2019-09-12"&OpenRatio!=0]

#sasa<-Data[TradingDay=="2024-4-15" & !Weight.new==0,.(SecuCode,TradingDay,expected_return,Weight.new)at
Data <- Data[,.(SecuCode,TradingDay,Weight.new)]
Data <- as.data.table(Data)
setnames(Data,old = "Weight.new",new = "OpenRatio")
Data[,SecuCode := as.integer(SecuCode)]
Data[,return:= sum(),by=.(TradingDay)]
Data[,.(sums=sum(OpenRatio)),by=.(TradingDay)]
Data[OpenRatio==0.01]
head(Data,100)
Data[TradingDay="2024-"]
if (sys.nframe() == 0){
  
  rm(list=ls())
  gc()
  
  source('F:/BackTest/load_library.R', encoding='UTF-8') 
  Data<-Data[TradingDay>"2019-01-01"]
  # 快速回测
  res1 <- SignalAnalyze(loadMode='Local',fileName='opt0',Data,indexBase='ZZ500',BTstartDate='2019-03-04', BTenddate="2020-01-01", ifDetail=1, ifSTIB = F)
  
  res1$Return
  final <- res1$Return[,.(TradingDay,CumHedge)]
  final
  final <-merge(final,res1$Return[,.(TradingDay,CumHedge)],by="TradingDay")
  names(final)[2]<- "mvo"
  final
  save(final,file = "2024-10-1.Rdata")
  #res1$BTResults
  #res1$Return
  final
  finals <-final[TradingDay>"2024-01-01"]
  finals <- finals[, lapply(.SD, function(x) x / x[1]),.SDcols = c("mvo")]
  finals <- finals[,TradingDay:=final[TradingDay>"2024-01-01"]$TradingDay]
  finals
  ggplot(finals, aes(x = TradingDay)) +
    geom_line(aes(y = mvo, color = "mvo"))
  ggplot(final, aes(x = TradingDay)) +
    geom_line(aes(y = mvo_1-equal.weight, color = "mvo_size0.8_indus0.03 - bentch"))
    geom_line(aes(y = mvo_2-equal.weight,color="mvo_sizeF_indus0.03 - bentch"))
  plot(x=c(1,2,3),y=c(4,5,6))
  ggplot(finals, aes(x = final[TradingDay>"2023-01-01"]$TradingDay)) +
    geom_line(aes(y = CumHedge, color = "lambda0.7"),size = 2) 
  
    geom_line(aes(y = opt19, color = "lambda1")) +
    geom_line(aes(y = opt1915, color = "lambda1.5")) +
    geom_line(aes(y = opt19175, color = "lambda1.75")) +
    geom_line(aes(y = opt1919, color = "lambda1.9"),size=2) +
    geom_line(aes(y = opt191, color = "lambda2")) +
    geom_line(aes(y = opt1925, color = "lambda2.5")) +
    geom_line(aes(y = opt193, color = "lambda3"))+
    labs(title = "CumHedge Over Time",
         x = "Trading Day",
         y = "Cumulative Hedge",
         color = "Legend") +
    theme_minimal()
  
  data <- data.frame(
    StrategyName = c("opt11", "opt19", "opt1915", "opt19175", "opt1919", "opt191", "opt1925", "opt193", "opt194", "opt12", "20", "40", "60", "80", "100"),
    Lambda = c(0, 1, 1.5, 1.75, 1.9, 2, 2.5, 3, 4, 10, 20, 40, 60, 80, 100),
    AR = c(0.175, 0.172, 0.187, 0.177, 0.189, 0.203, 0.18, 0.157, 0.186, 0.178, 0.176, 0.157, 0.158, 0.154, 0.165),
    SR = c(1.94, 1.96, 2.13, 2, 2.11, 2.33, 2.05, 1.81, 2.15, 2.11, 2.09, 1.91, 1.94, 1.95, 2.08)
  )
  setDT(data)
  # 提取向量
  lambda_vector <- data$Lambda
  ar_vector <- data$AR
  sr_vector <- data$SR
  
  # 输出向量
  lambda_vector
  ar_vector
  sr_vector
  ggplot(data,mapping = aes(x =Lambda))+
    geom_line(aes(y = AR), color = "blue")+
    geom_hline(aes(yintercept = 0.193,color ="等权"))
  ggplot(data[Lambda<4],mapping = aes(x =Lambda))+
    geom_line(aes(y = AR), color = "blue")+
    geom_hline(aes(yintercept = 0.193,color ="等权"))
  ggplot(data[Lambda<4],mapping = aes(x =Lambda))+
    geom_line(aes(y = SR), color = "blue")+
    geom_hline(aes(yintercept = 2.32,color ="等权"))
  
  finals <-final[TradingDay>"2023-01-01"]
  finals <- finals[, lapply(.SD, function(x) x / x[1]),.SDcols = names(finals)[-1]]
  finals
  ggplot(final, aes(x = TradingDay)) +
    geom_line(aes(y = opt0, color = "等权")) +
    geom_line(aes(y = opt3, color = "lambda 4")) +
    geom_line(aes(y = opt4, color = "lambda10")) +
    geom_line(aes(y = opt5, color = "lambda20")) +
    geom_line(aes(y = opt6, color = "lambda40")) +
    geom_line(aes(y = opt7, color = "lambda100")) +
    labs(title = "CumHedge Over Time",
         x = "Trading Day",
         y = "Cumulative Hedge",
         color = "Legend") +
    theme_minimal()
  #res1$HoldingRecord
  #HoldingRecord1 <- res1$HoldingRecord
  
  finals <-final[TradingDay>"2023-01-01"]
  finals <- finals[, lapply(.SD, function(x) x / x[1]),.SDcols = names(finals)[-1]]
  finals
  ggplot(final, aes(x = TradingDay)) +
    geom_line(aes(y = opt0, color = "等权")) +
    geom_line(aes(y = opt11, color = "lambda4")) +
    geom_line(aes(y = opt12, color = "lambda10")) +
    geom_line(aes(y = opt10, color = "lambda20")) +
    geom_line(aes(y = opt9, color = "lambda40")) +
    geom_line(aes(y = opt8, color = "lambda100")) +
    labs(title = "CumHedge Over Time",
         x = "Trading Day",
         y = "Cumulative Hedge",
         color = "Legend") +
    theme_minimal()
  # 详细回测
  res2 <- SignalAnalyze(loadMode='Local',fileName='test',DF.Signal,indexBase='ZZ500',BTstartDate='2011-01-01', BTenddate=Sys.Date()-1, ifDetail=2, ifSTIB = F)
  res2$BTResults
  res2$Return
  res2$HoldingRecord
  HoldingRecord2 <- res2$HoldingRecord
}
