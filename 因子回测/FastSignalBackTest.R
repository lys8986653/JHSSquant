### 日期: 2023-12-12
### 作者: Anran Liu; Huanfeng Dong 
### 该文件是因子快速回测的算法文件(向量化)；可设置选股池和交易日
### 更新20231207：1、股票池分为GZ2000、全市场、Size < 0 or -1 or -1.5；2、调仓周期为2D、5D、20D；
#              3、Vwap60作为调仓交易价格，feeRatio设置千二；
#              4、adjustPara为粘滞系数，可选择：('StockNum200', 'AdjustPara100', NULL)，数字部分可以修改
#              5、目前返回结果包含 1整体情况（AR,SR,MDD,每年超额，最大回撤时间），2日收益及净值，3历史持仓列表。
### 更新20231220:1、简化和统一不同indexBase的计算；简化指标计算；2、修改adjustPara参数格式,其他参数均改为小写，函数默认值改为非引用；
#              3、tradedays stockpools作为参数输入；
### 更新20240627:复权系数采用相对值，即除以上期的复权系数
### =================================================================================== ### 
#需以下文件 TradingDay.Rdata|MarketIndex.Rdata|VWAP.RData

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
  DT.HoldingList[changehold < 0, cost := abs(changehold*AOpenPrice*0.002)]  #单只股票换仓cost,changehold < 0为卖出
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
  
  # 换手率
  DT.ChangeRatio <-  R$DT.HoldingRecord
  DT.ChangeRatio <- DT.ChangeRatio[,.(list(SecuCode)), by = TradingDay]
  setnames(DT.ChangeRatio,'V1','Hold')
  DT.ChangeRatio[, PreHold := lag(Hold,1)]
  DT.ChangeRatio[1, PreHold := Hold]
  DT.ChangeRatio[, ChangeNum := length(setdiff(unlist(PreHold), unlist(Hold))),by = TradingDay]
  DT.ChangeRatio[, ChangeRatio := as.numeric(ChangeNum)/length(unlist(PreHold)),by = TradingDay]
  
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



if (sys.nframe() == 0){
  
  # 快速回测返回的是SR最好的情况
  FastR <- FastBackTest(DT.DailyQuote.Analyze, BegDate =as.Date(BTstartDate), EndDate = as.Date(BTenddate), 
           indexBase='ZZ500', feeRatio = 0.002, tradePrice = 'VWAP60', availableFund = 2e6, ifSTIB = F)
  print(FastR$DT.ResultsAll) #整体情况，包含AR,SR,MDD等数字
  print(FastR$DT.Results) #最佳组合情况，包含AR,SR,MDD等数字
  print(FastR$DT.Return) #日收益及净值
  print(FastR$DT.HoldingRecord) #持仓股票

  DT.Return <- FastR$DT.Return
  DT.Return$StrategyNet <- 1+cumsum(DT.Return$dayreturn)
  p <- DFplot(DT.Return[, c('TradingDay','StrategyNet')], # 多头收益
              Xlabel = '日期', Ylabel = '纯多头净值（cumsum）', Xbreaks = '8 week', Title = '策略多头净值')
  
}
