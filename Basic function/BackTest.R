### 日期: 2019-05-13
### 作者: Huanfeng Dong
### 该文件是因子回测的算法文件
### 更新日期20231023：1、补充中证1000对冲收益回报；2、涨跌停板价格UpLimit、DownLimit计算采用数据库数据，原计算方法使用1.1 0.9已不合理；
#              3、支持OpenVwap作为调仓交易价格，参数通过原TradePrice设置，比如VWAP5/15/30/60等；4、科创板数据文件支持，增加IfSTIB参数，可以设置为T；
#              5、fst格式读取提速
#   更新日期20240104：1补充行情数据：包括历史上暂停上市数据、退市后延长数据；2、增加国证2000对标计算
### =================================================================================== ### 
#需以下文件 TradingDay.Rdata|MarketIndex.Rdata|DailyQuote.Rdata|Dividend.Rdata|PriceLimit.Rdata|VWAP.RData

library(fst)

## 计算区间市场指数收益
RangeMarketReturn <- function(DT.Market, IndexName, BegDate, EndDate){
  MarketReturn <- c()
  for(i in 1:length(BegDate)){
    MarketReturn <- c(MarketReturn, sum(DT.Market[TradingDay >= BegDate[i] & TradingDay <= EndDate[i], IndexName, with=F]))
  }
  return(MarketReturn)
}


BackTest <- function(DT.Signal, BegDate, EndDate = Sys.Date()-1, IfReTrade = T, IfShort = F, IfAddReduce = T, FeeRatio = 0, 
                     AvailableFund = NULL, TradePrice = 'OpenPrice', IfSTIB=F)
{
  ## 如果IfReTrade为True，那么当天因一字板，停牌无法买入的股票会移到下一个交易日交易。
  ## 不管IFReTrade为True或者False，当天因一字板，停牌无法卖出的股票都会移到下一个交易日卖出。
  ## IfShort为True则允许做空，即股票持仓可以为负值。
  ## IfAddReduce = T 则允许加减仓，为F则不允许加减仓，只许平开仓
  ## IfTSTIB=F,默认不含科创板；T为包含科创板
  ## TradePrice默认为OpenPrice，即按照开盘价；可以选ClosePrice，按照收盘价；也可以选择VWAP5/10/15/30/60，此时FeeRatio建议设置千二
  
  ###载入交易日数据Data.TradingDay
  load(paste0(Dir.Data,'TradingDay.Rdata'))
  Ary.TradingDay <- Data.TradingDay
  if(BegDate<min(Data.TradingDay)){
    BegDate <- as.Date(min(Data.TradingDay))
  }
  if(EndDate>max(Data.TradingDay)){
    EndDate <- as.Date(max(Data.TradingDay))
  }
  Ary.TradingDay <- Ary.TradingDay[Ary.TradingDay >= as.Date(BegDate) & Ary.TradingDay <= as.Date(EndDate)]
  
  # 科创板数据单独导入
  if (IfSTIB){ 
    Dir.Data <- Dir.DataSTIB
  }
  # load(paste0(Dir.Data,'DailyQuote.Rdata'))
  Data.DailyQuote <- read.fst(paste0(Dir.Data,'DailyQuote.fst'), as.data.table=T)
  load(paste0(Dir.Data,'Dividend.Rdata'))
  load(paste0(Dir.Data,'PriceLimit.Rdata'))
  
  
  # VWAP数据载入
  if (substr(TradePrice,1,4) == 'VWAP'){
    
    # load(paste0('\\\\192.168.8.38\\shares\\wap_rdata\\tickvwap_first', str_extract(TradePrice , '\\d+'), '.Rdata'))
    # Expr <- paste0("DT.VWAP <- copy(tickvwap_first",str_extract(TradePrice , '\\d+'),")")
    # eval(parse(text = Expr))
    # Expr <- paste0("rm(tickvwap_first",str_extract(TradePrice , '\\d+'),")")
    # eval(parse(text = Expr))
    
    # 改为fst加速
    DT.VWAP <- read.fst(paste0('\\\\192.168.8.38\\shares\\wap_rdata\\tickvwap_first', str_extract(TradePrice , '\\d+'), '.fst'), as.data.table=T)
 
    setDT(DT.VWAP)
    DT.VWAP[, ':='(SecuCode=as.numeric(SecuCode), TradingDay=as.Date(TradingDay))]
    
    if (IfSTIB == F){
      DT.VWAP <- DT.VWAP[SecuCode < 688001] # 去掉科创板股票
    }
    
    setnames(Data.DailyQuote, 'OpenPrice', 'OpenPrice1')
    Data.DailyQuote <- merge(Data.DailyQuote, DT.VWAP, by=c('TradingDay','SecuCode'),all.x=T)
    Data.DailyQuote[is.na(OpenPrice), OpenPrice:=OpenPrice1] # 处理VWAP缺失部分数据
    Data.DailyQuote[,OpenPrice1:=NULL]
    
    TradePrice <- 'OpenPrice' # 注意要改回
  }
  
  Data.DailyQuote <- merge(Data.DailyQuote,DT.PriceLimit[,.(TradingDay, SecuCode, UpLimit=PriceCeiling,DownLimit=PriceFloor)],
                           by=c('TradingDay', 'SecuCode'), all.x=T)
  
  Data.Dividend[, TradingDay := ExDiviDate]
  Data.Dividend <- Data.Dividend[!is.na(TradingDay)]
  setkey(Data.Dividend, TradingDay, SecuCode)
  load(paste0(Dir.Data,'MarketIndex.Rdata'))
  DT.Market <- dcast(Data.MarketIndex, TradingDay ~ IndexName, value.var = 'Return')
  DT.Market <- as.data.table(DT.Market)
  #DT.TradingRecord.History中的ChangeReason：1、开仓，2、平仓，3、加仓，4、减仓
  
  # 补充行情数据：包括历史上暂停上市数据、退市后延长数据
  load(paste0(Dir.Data, 'DailyQuoteFill.Rdata'))
  Data.DailyQuote <- rbind(Data.DailyQuote, Data.DailyQuoteFill)
  setkey(Data.DailyQuote, TradingDay,SecuCode)
  
  #初始化数据
  if(is.null(AvailableFund)){
    AvailableFund <- 2000000#可用资金
  }
  cat('初始可用资金：',AvailableFund,'元\n')
  TotalFund <- AvailableFund#总资金
  HoldFund <- 0#可用资金
  DT.Summary <- data.table(TradingDay = Ary.TradingDay
                           , Net = 1
                           , Net.DailyReturn = 0
                           , Account.Total = AvailableFund
                           , Account.Available = AvailableFund
                           , Account.Holding = 0
                           , FundUnits = AvailableFund) #初始化基金情况，包括市值，可用资金
  setkey(DT.Summary,TradingDay)
  DT.TradingRecord.History <- data.table()#交易记录
  DT.HoldingRecord.History <- data.table()#持仓记录
  DT.HoldingRecord.Today <- data.table()#今持仓记录
  DT.TradingRecord.Today<- data.table()#今交易记录
  DT.Close.History <- data.table()#平仓单记录
  SubDate.Length <- round(sqrt(length(Ary.TradingDay)))
  SubDate.StartDate <- Ary.TradingDay[1]
  SubDate.EndDate <- Ary.TradingDay[1+SubDate.Length-1]
  Ary.TradingDay.Sub <- Ary.TradingDay[Ary.TradingDay>=SubDate.StartDate & Ary.TradingDay <= SubDate.EndDate]
  DT.DailyQuote.Sub <-  Data.DailyQuote[list(Ary.TradingDay.Sub)]
  setkey(DT.Signal,TradingDay,SecuCode)
  for(Today in Ary.TradingDay)
  {
    Today <- as.Date(Today)
    #print(Today)
    #if(Today == as.Date('2016-10-10')){
    #  browser()
    #}
    if(Today==SubDate.EndDate){#生成行情子序列
      SubDate.StartDate <- SubDate.EndDate
      SubDate.EndDate <- Ary.TradingDay[min(match(SubDate.EndDate,Ary.TradingDay)+SubDate.Length-1, length(Ary.TradingDay))]
      Ary.TradingDay.Sub <- Ary.TradingDay[Ary.TradingDay>=SubDate.StartDate & Ary.TradingDay <= SubDate.EndDate]
      DT.DailyQuote.Sub <-  Data.DailyQuote[list(Ary.TradingDay.Sub)]
      setkey(DT.DailyQuote.Sub,TradingDay, SecuCode)
    }
    ##更新持仓信息
    if(nrow(DT.HoldingRecord.Today) != 0){
      ##除权除息的股本调整即开仓均价的调整
      DT.HoldingRecord.Today[, ':=' (#日期变为今天
        TradingDay = Today
      )]
      setkey(DT.HoldingRecord.Today,TradingDay,SecuCode)
      DF.Dividend.Today <- DT.HoldingRecord.Today[Data.Dividend, nomatch=0]
      if(nrow(DF.Dividend.Today)!=0){#如果今日持仓中有除权股，则进行除权
        # print(DF.Dividend.Today)
        DF.Dividend.Today[,':='(
          New.HoldShares = HoldShares + HoldShares / 10 * (BonusShareRatio + TranAddShareRaio)
          , New.YesterdayClosePrice = round((YesterdayClosePrice - CashDiviRMB/10)/(1 + (BonusShareRatio + TranAddShareRaio)/10),2)
          , New.TodayClosePrice = round((TodayClosePrice - CashDiviRMB/10)/(1 + (BonusShareRatio + TranAddShareRaio)/10),2)
          , New.HoldValue = HoldShares * TodayClosePrice - HoldShares/10 * CashDiviRMB
          , CashDiviRMB = HoldShares/10 * CashDiviRMB
          , New.TodayShares = TodayShares + TodayShares / 10 * (BonusShareRatio + TranAddShareRaio)
          , New.YesterdayShares = YesterdayShares + YesterdayShares / 10 * (BonusShareRatio + TranAddShareRaio)
        )]
        DT.HoldingRecord.Today[list(Today, DF.Dividend.Today$SecuCode),':='(
          HoldShares =DF.Dividend.Today$New.HoldShares
          , HoldValue = DF.Dividend.Today$New.HoldValue
          , AveCostPrice = AveCostPrice * DF.Dividend.Today$HoldShares / DF.Dividend.Today$New.HoldShares
          , YesterdayClosePrice = DF.Dividend.Today$New.YesterdayClosePrice
          , TodayClosePrice = DF.Dividend.Today$New.TodayClosePrice
          , TodayShares = DF.Dividend.Today$New.TodayShares
          , YesterdayShares = DF.Dividend.Today$New.YesterdayShares
        )]
        #重新计算资金
        CashDiviRMBSum <- sum(DF.Dividend.Today$CashDiviRMB)
        AvailableFund <- AvailableFund + CashDiviRMBSum
        HoldFund <- HoldFund - CashDiviRMBSum
      }
      
      #更新ClosePrice
      DF.DailyQuote.Holding <- DT.DailyQuote.Sub[list(Today,DT.HoldingRecord.Today$SecuCode)]
      #data.table的':='不是实时的，mutate是实时的
      DT.HoldingRecord.Today[, ':=' (
        SecuAbbr = DF.DailyQuote.Holding$SecuAbbr
        , YesterdayClosePrice = DF.DailyQuote.Holding$PrevClosePrice
        , TodayClosePrice = DF.DailyQuote.Holding$ClosePrice
        , HoldValue = HoldShares * DF.DailyQuote.Holding$OpenPrice
        , YesterdayShares = YesterdayShares + TodayShares
        , TodayShares = 0
      )]
      HoldFund <-  sum(DT.HoldingRecord.Today$HoldValue)
      TotalFund <- HoldFund + AvailableFund
      #更新基金情况表
      
      DT.Summary[list(Today),':='(
        Net = (TotalFund) / FundUnits
        , Account.Available = AvailableFund
        , Account.Holding  = HoldFund
        , Account.Total = TotalFund 
      )]
    }
    
    ##调仓模块
    {
      DT.Signal.Today <- unique(DT.Signal[TradingDay %in% Today])#今日开仓
      DF.DailyQuote.Adjust <-DT.Signal.Today[DT.DailyQuote.Sub,nomatch=0]#并表
      #停牌处理
      DF.DailyQuote.Adjust.Fail <- DF.DailyQuote.Adjust[OpenPrice==0] 
      DF.DailyQuote.Adjust <- DF.DailyQuote.Adjust[OpenPrice!=0]
      #把停牌的交易信号移到下一个交易日
      if(nrow(DF.DailyQuote.Adjust.Fail)!=0){
        Temp <- DF.DailyQuote.Adjust.Fail[,names(DT.Signal),with=F]
        Temp[,TradingDay:=GetTradingDayLeadLag(TradingDay,Data.TradingDay,LeadLag = -1)]
        #删除DT.Signal里面已有的单子
        Temp.AlreadyIn <- DT.Signal[list(Temp[,.(TradingDay,SecuCode)])]
        setnames(Temp.AlreadyIn,'OpenRatio','Tag')
        Temp <- merge(Temp,Temp.AlreadyIn[,.(TradingDay, SecuCode, Tag)],by=c('TradingDay','SecuCode'),all.x=T)[is.na(Tag)]
        Temp <- Temp[,names(DT.Signal),with=F]
        if(nrow(Temp)!=0){
          if(IfReTrade)
          {
            DT.Signal <- rbindlist(list(DT.Signal,Temp))
          }else{
            DT.Signal <- rbindlist(list(DT.Signal,Temp[OpenRatio == 0]))
          }
        }
        setkey(DT.Signal,TradingDay,SecuCode)
      }
      #先减仓或平仓
      if(nrow(DT.HoldingRecord.Today)==0 & IfShort & nrow(DF.DailyQuote.Adjust[OpenRatio <0])!=0){
        DF.DailyQuote.Sell <- copy(DF.DailyQuote.Adjust[OpenRatio <0])
        setkey(DF.DailyQuote.Sell, TradingDay, SecuCode)
        if(TradePrice == 'OpenPrice'){
          DF.DailyQuote.Sell[, TargetShares:= 100*round(OpenRatio * TotalFund / (PrevClosePrice*100))] 
          DF.DailyQuote.Sell[, SellPrice := OpenPrice]
        }else if(TradePrice == 'ClosePrice'){
          DF.DailyQuote.Sell[, TargetShares:= 100*round(OpenRatio * TotalFund / (PrevClosePrice*100))] 
          DF.DailyQuote.Sell[, SellPrice := ClosePrice]
        }
        
        #跌停价处理
        {
          
          # DF.DailyQuote.Sell[,DownLimit := round(PrevClosePrice*0.9,2)]
          DF.DailyQuote.Sell.Fail <- DF.DailyQuote.Sell[SellPrice <= DownLimit]
          #把跌停的交易信号移到下一个交易日
          if(nrow(DF.DailyQuote.Sell.Fail)!=0){
            Temp <- DF.DailyQuote.Sell.Fail[,names(DT.Signal),with=F]
            Temp[,TradingDay:=GetTradingDayLeadLag(TradingDay,Data.TradingDay,LeadLag = -1)]
            #如果删除DT.Signal里面已有的单子
            Temp.AlreadyIn <- DT.Signal[list(Temp[,.(TradingDay,SecuCode)])]
            setnames(Temp.AlreadyIn,'OpenRatio','Tag')
            Temp <- merge(Temp,Temp.AlreadyIn[,.(TradingDay, SecuCode, Tag)],by=c('TradingDay','SecuCode'),all.x=T)[is.na(Tag)]
            Temp <- Temp[,names(DT.Signal),with=F]
            if(nrow(Temp)!=0){
              
              DT.Signal <- rbindlist(list(DT.Signal,Temp))
              
            }
            setkey(DT.Signal,TradingDay,SecuCode)
          }
          DF.DailyQuote.Sell <-  DF.DailyQuote.Sell[SellPrice > DownLimit]
        }
        #无今仓限制
        {
        }
        DF.DailyQuote.Sell[, SellShares := TargetShares]#需要做空的仓位就是目标仓位
        DF.DailyQuote.Sell[, SellValue :=  SellShares * SellPrice]
        if(nrow(DF.DailyQuote.Sell)!=0){
          #更新可用资金
          AvailableFund <- AvailableFund -  sum(DF.DailyQuote.Sell$SellValue)
          AvailableFund <- AvailableFund - abs(sum(DF.DailyQuote.Sell$SellValue))*FeeRatio
          #添加交易记录-买入
          DF.DailyQuote.Sell[, ChangeReason := 5]
          DT.TradingRecord.Today <- DF.DailyQuote.Sell[,c('TradingDay','SecuCode','SecuAbbr', 'SellShares', 'SellValue','SellPrice','PrevClosePrice','ClosePrice','ChangeReason')]
          names(DT.TradingRecord.Today) <- c('TradingDay','SecuCode','SecuAbbr', 'ChangeShares', 'ChangeValue','TradePrice','PrevClosePrice','ClosePrice','ChangeReason')
          DT.TradingRecord.History <- rbind(DT.TradingRecord.History,DT.TradingRecord.Today)
          #更新持仓表                      
          DT.HoldingRecord.Today <- data.table(TradingDay = Today
                                               , SecuCode = DT.TradingRecord.Today$SecuCode
                                               , SecuAbbr = DT.TradingRecord.Today$SecuAbbr
                                               , HoldShares = DT.TradingRecord.Today$ChangeShares
                                               , HoldValue = DT.TradingRecord.Today$ChangeValue
                                               , AveCostPrice = DT.TradingRecord.Today$TradePrice
                                               , YesterdayClosePrice = DT.TradingRecord.Today$PrevClosePrice
                                               , TodayClosePrice = DT.TradingRecord.Today$ClosePrice
                                               , FirstUpdateDay = Today
                                               , LastUpdateDay = Today
                                               , TodayShares = 0
                                               , YesterdayShares = DT.TradingRecord.Today$ChangeShares
          )
          
        }
      }else if(nrow(DT.HoldingRecord.Today)!=0){
        setkey(DT.HoldingRecord.Today,TradingDay, SecuCode)
        # DF.DailyQuote.Sell <- DT.HoldingRecord.Today[DF.DailyQuote.Adjust,nomatch = 0]
        DF.DailyQuote.Sell <- merge(DF.DailyQuote.Adjust, DT.HoldingRecord.Today, by=c('TradingDay','SecuCode','SecuAbbr'),all=T)
        setkey(DF.DailyQuote.Sell, TradingDay, SecuCode)
        if(TradePrice == 'OpenPrice'){
          DF.DailyQuote.Sell[, TargetShares:= 100*round(OpenRatio * TotalFund / (PrevClosePrice*100))] 
          DF.DailyQuote.Sell[, HoldShares := na.fill(HoldShares, 0)]#如果存在做空未持有的股票，则需要补全HoldShares
          DF.DailyQuote.Sell[is.na(AveCostPrice), AveCostPrice := OpenPrice]#如果存在做空未持有的股票，其成本为开盘价
          if(!IfAddReduce){ # 如果不接受加减仓，那么只需要TargetShares==0的单子
            DF.DailyQuote.Sell <- DF.DailyQuote.Sell[TargetShares==0]
          }else{
            DF.DailyQuote.Sell <- DF.DailyQuote.Sell[TargetShares < HoldShares]
          }
          DF.DailyQuote.Sell[, SellPrice := OpenPrice]
        }else if(TradePrice == 'ClosePrice'){
          DF.DailyQuote.Sell[, TargetShares:= 100*round(OpenRatio * TotalFund / (PrevClosePrice*100))] 
          DF.DailyQuote.Sell[, HoldShares := na.fill(HoldShares, 0)]#如果存在做空未持有的股票，则需要补全HoldShares
          DF.DailyQuote.Sell[is.na(AveCostPrice), AveCostPrice := ClosePrice]#如果存在做空未持有的股票，其成本为开盘价
          if(!IfAddReduce){ # 如果不接受加减仓，那么只需要TargetShares==0的单子
            DF.DailyQuote.Sell <- DF.DailyQuote.Sell[TargetShares==0]
          }else{
            DF.DailyQuote.Sell <- DF.DailyQuote.Sell[TargetShares < HoldShares]
          }
          DF.DailyQuote.Sell[, SellPrice := ClosePrice]
        }
        
        
        #跌停价处理
        {
          
          # DF.DailyQuote.Sell[,DownLimit := round(PrevClosePrice*0.9,2)]
          DF.DailyQuote.Sell.Fail <- DF.DailyQuote.Sell[SellPrice <= DownLimit]
          #把跌停的交易信号移到下一个交易日
          if(nrow(DF.DailyQuote.Sell.Fail)!=0){
            Temp <- DF.DailyQuote.Sell.Fail[,names(DT.Signal),with=F]
            Temp[,TradingDay:=GetTradingDayLeadLag(TradingDay,Data.TradingDay,LeadLag = -1)]
            #如果删除DT.Signal里面已有的单子
            Temp.AlreadyIn <- DT.Signal[list(Temp[,.(TradingDay,SecuCode)])]
            setnames(Temp.AlreadyIn,'OpenRatio','Tag')
            Temp <- merge(Temp,Temp.AlreadyIn[,.(TradingDay, SecuCode, Tag)],by=c('TradingDay','SecuCode'),all.x=T)[is.na(Tag)]
            Temp <- Temp[,names(DT.Signal),with=F]
            if(nrow(Temp)!=0){
              
              DT.Signal <- rbindlist(list(DT.Signal,Temp))
              
            }
            setkey(DT.Signal,TradingDay,SecuCode)
          }
          DF.DailyQuote.Sell <-  DF.DailyQuote.Sell[SellPrice > DownLimit]
        }
        #今仓限制
        {
          #把今仓限制的交易信号移到下一个交易日
          Temp <- DF.DailyQuote.Sell[TargetShares < TodayShares]
          Temp <- Temp[,names(DT.Signal),with=F]
          if(nrow(Temp)!=0 & !IfShort){
            #如果删除DT.Signal里面已有的单子
            Temp.AlreadyIn <- DT.Signal[list(Temp[,.(TradingDay,SecuCode)])]
            setnames(Temp.AlreadyIn,'OpenRatio','Tag')
            Temp <- merge(Temp,Temp.AlreadyIn[,.(TradingDay, SecuCode, Tag)],by=c('TradingDay','SecuCode'),all.x=T)[is.na(Tag)]
            Temp <- Temp[,names(DT.Signal),with=F]
            if(nrow(Temp)!=0){
              DT.Signal <- rbindlist(list(DT.Signal,Temp))
            }
            setkey(DT.Signal,TradingDay,SecuCode)
          }
          if(!IfShort){#不可做空
            DF.DailyQuote.Sell[,SellShares := pmax(TargetShares - HoldShares, -YesterdayShares)]#比较需要卖掉的仓位和昨仓数目
          }else{#可做空
            DF.DailyQuote.Sell[,SellShares := TargetShares - HoldShares]#比较需要卖掉的仓位和昨仓数目
          }
          DF.DailyQuote.Sell <- DF.DailyQuote.Sell[SellShares < 0]
          DF.DailyQuote.Sell[, SellValue :=  SellShares * SellPrice]
        }
        if(nrow(DF.DailyQuote.Sell)!=0){
          #更新可用资金
          AvailableFund <- AvailableFund -  sum(DF.DailyQuote.Sell$SellValue)
          AvailableFund <- AvailableFund - abs(sum(DF.DailyQuote.Sell$SellValue))*FeeRatio
          #添加交易记录-卖出
          DF.DailyQuote.Sell[OpenRatio>0, ChangeReason:= 4]
          DF.DailyQuote.Sell[OpenRatio==0, ChangeReason:= 2]
          DF.DailyQuote.Sell[OpenRatio<0, ChangeReason:= 5]
          
          DT.TradingRecord.Today <- DF.DailyQuote.Sell[,c('TradingDay','SecuCode','SecuAbbr', 'SellShares', 'SellValue','SellPrice','PrevClosePrice','ClosePrice','ChangeReason')]
          names(DT.TradingRecord.Today) <- c('TradingDay','SecuCode','SecuAbbr', 'ChangeShares', 'ChangeValue','TradePrice','PrevClosePrice','ClosePrice','ChangeReason')
          DT.TradingRecord.History <- rbind(DT.TradingRecord.History,DT.TradingRecord.Today)
          #更新持仓表                      
          DF.HoldingRecord.New <- data.table(TradingDay = Today #新交易产生的临时持有表
                                             , SecuCode = DT.TradingRecord.Today$SecuCode
                                             , SecuAbbr = DT.TradingRecord.Today$SecuAbbr
                                             , HoldShares = DT.TradingRecord.Today$ChangeShares
                                             , HoldValue = DT.TradingRecord.Today$ChangeValue
                                             , AveCostPrice = DF.DailyQuote.Sell$AveCostPrice
                                             , YesterdayClosePrice = DT.TradingRecord.Today$PrevClosePrice
                                             , TodayClosePrice = DT.TradingRecord.Today$ClosePrice
                                             , FirstUpdateDay = Today
                                             , LastUpdateDay = Today
                                             , TodayShares = 0
                                             , YesterdayShares = DT.TradingRecord.Today$ChangeShares
          )
          #并表形成新的持有表
          DF.HoldingRecord.New <- rbind(DT.HoldingRecord.Today, DF.HoldingRecord.New)
          DT.HoldingRecord.Today <- DF.HoldingRecord.New[,list(HoldShares = sum(HoldShares) #股数相加
                                                               , HoldValue = sum(HoldValue) #市值相加
                                                               , AveCostPrice = ifelse(sum(HoldShares)!=0, round(sum(HoldShares*AveCostPrice)/sum(HoldShares),2), first(AveCostPrice)) #重新计算平均成本价
                                                               , YesterdayClosePrice = mean(YesterdayClosePrice)
                                                               , TodayClosePrice = mean(TodayClosePrice)
                                                               , FirstUpdateDay = min(FirstUpdateDay) #取最早的为第一次开仓日
                                                               , LastUpdateDay = max(LastUpdateDay) #取最晚的为最近一次开仓日
                                                               , TodayShares = sum(TodayShares) 
                                                               , YesterdayShares = sum(YesterdayShares)
          ),by=.(TradingDay,SecuCode,SecuAbbr)]
          
          DF.Close.Today <- DT.HoldingRecord.Today[HoldShares == 0]
          if(nrow(DF.Close.Today)!=0){
            
            Temp.SellPrice <-  DF.DailyQuote.Sell[list(DF.Close.Today$TradingDay,DF.Close.Today$SecuCode),.(SellPrice)]
            DF.Close.Today[, SellPrice := Temp.SellPrice]
            rm(Temp.SellPrice)
            DF.Close.Today <- DF.Close.Today[, c('TradingDay','SecuCode','SecuAbbr','AveCostPrice','FirstUpdateDay','LastUpdateDay', 'SellPrice')]
            DF.Close.Today[, Profit := SellPrice/AveCostPrice-1]
            DF.Close.Today[, Profit.ZZ500 := RangeMarketReturn(DT.Market
                                                               , 'ZZ500'
                                                               , FirstUpdateDay
                                                               , GetTradingDayLeadLag(LastUpdateDay, Data.TradingDay, 1))]
            DF.Close.Today[, Profit.HS300 := RangeMarketReturn(DT.Market
                                                               , 'HS300'
                                                               , FirstUpdateDay
                                                               , GetTradingDayLeadLag(LastUpdateDay, Data.TradingDay, 1))]
            DF.Close.Today[, Profit.ZZ1000 := RangeMarketReturn(DT.Market
                                                                , 'ZZ1000'
                                                                , FirstUpdateDay
                                                                , GetTradingDayLeadLag(LastUpdateDay, Data.TradingDay, 1))]
            DF.Close.Today[, Profit.GZ2000 := RangeMarketReturn(DT.Market
                                                                , 'GZ2000'
                                                                , FirstUpdateDay
                                                                , GetTradingDayLeadLag(LastUpdateDay, Data.TradingDay, 1))]
            DF.Close.Today[, ':='(
              Profit.HedgeZZ500 = Profit - Profit.ZZ500
              , Profit.HedgeHS300 = Profit - Profit.HS300
              , Profit.HedgeZZ1000 = Profit - Profit.ZZ1000
              , Profit.HedgeGZ2000 = Profit - Profit.GZ2000
            )]
            DF.Close.Today <- DF.Close.Today[, -c('Profit.ZZ500', 'Profit.HS300', 'Profit.ZZ1000', 'Profit.GZ2000')]
            setnames(DF.Close.Today, 'SellPrice', 'CloseTradePrice')
            DF.Close.Today[, Dir := 1]
            DT.Close.History <- rbind(DT.Close.History, DF.Close.Today)
          }
          rm(DF.Close.Today)
          DT.HoldingRecord.Today <- DT.HoldingRecord.Today[HoldShares != 0]
        }
      }
      #后开仓加仓
      {
        if(nrow(DT.HoldingRecord.Today) == 0){
          DF.DailyQuote.Buy <- copy(DF.DailyQuote.Adjust)
          setkey(DF.DailyQuote.Buy, TradingDay, SecuCode)
          if(TradePrice == 'OpenPrice'){
            DF.DailyQuote.Buy[, BuyShares:= 100*round(OpenRatio * TotalFund / (PrevClosePrice*100))] 
            DF.DailyQuote.Buy[, BuyPrice := OpenPrice]
            DF.DailyQuote.Buy[, BuyValue := BuyShares*OpenPrice]
            DF.DailyQuote.Buy <- DF.DailyQuote.Buy[BuyShares>0]
          }else if(TradePrice == 'ClosePrice'){
            DF.DailyQuote.Buy[, BuyShares:= 100*round(OpenRatio * TotalFund / (PrevClosePrice*100))] 
            DF.DailyQuote.Buy[, BuyPrice := ClosePrice]
            DF.DailyQuote.Buy[, BuyValue := BuyShares*ClosePrice]
            DF.DailyQuote.Buy <- DF.DailyQuote.Buy[BuyShares>0]
          }
          #涨停价处理
          {
            # DF.DailyQuote.Buy[,UpLimit:=  round(PrevClosePrice*1.1,2)]
            DF.DailyQuote.Buy.Fail <- DF.DailyQuote.Buy[BuyPrice >= UpLimit]
            #把涨停限制的交易信号移到下一个交易日
            if(nrow(DF.DailyQuote.Buy.Fail)!=0){
              Temp <- DF.DailyQuote.Buy.Fail[,names(DT.Signal),with=F]
              Temp[,TradingDay:=GetTradingDayLeadLag(TradingDay,Data.TradingDay,LeadLag = -1)]
              #如果删除DT.Signal里面已有的单子
              Temp.AlreadyIn <- DT.Signal[list(Temp[,.(TradingDay,SecuCode)])]
              setnames(Temp.AlreadyIn,'OpenRatio','Tag')
              Temp <- merge(Temp,Temp.AlreadyIn[,.(TradingDay, SecuCode, Tag)],by=c('TradingDay','SecuCode'),all.x=T)[is.na(Tag)]
              Temp <- Temp[,names(DT.Signal),with=F]
              if(nrow(Temp)!=0){
                if(IfReTrade){
                  Temp$Order <- 1000000
                  DT.Signal <- rbindlist(list(DT.Signal,Temp))
                }
              }
              setkey(DT.Signal,TradingDay,SecuCode)
            }
            DF.DailyQuote.Buy <-  DF.DailyQuote.Buy[BuyPrice < UpLimit]
          }
          #买入资金限制
          BuyValueSum <- sum(DF.DailyQuote.Buy$BuyValue)
          if(BuyValueSum > AvailableFund & !IfShort)
          { 
            setorder(DF.DailyQuote.Buy, Order) # 根据已有的Order决定买入顺序
            DF.DailyQuote.Buy <- DF.DailyQuote.Buy[1:min(which(cumsum(DF.DailyQuote.Buy$BuyValue)>=AvailableFund)),]
            BuyValueSum <- sum(DF.DailyQuote.Buy$BuyValue)
            DeleteShares <- ceiling((BuyValueSum - AvailableFund)/ (100*as.numeric(DF.DailyQuote.Buy[.N, .(BuyPrice)])))*100
            DF.DailyQuote.Buy[.N, BuyShares :=  BuyShares- DeleteShares]
            DF.DailyQuote.Buy[.N, BuyValue :=  BuyValue - DeleteShares * BuyPrice]
            DF.DailyQuote.Buy <- DF.DailyQuote.Buy[BuyShares > 0]
            setkey(DF.DailyQuote.Buy, TradingDay, SecuCode)
          }
          if(nrow(DF.DailyQuote.Buy)!=0){
            #更新可用资金
            AvailableFund <- AvailableFund - sum(DF.DailyQuote.Buy$BuyValue)
            #添加交易记录-买入
            DF.DailyQuote.Buy[, ChangeReason := 1]
            DT.TradingRecord.Today <- DF.DailyQuote.Buy[,c('TradingDay','SecuCode','SecuAbbr', 'BuyShares', 'BuyValue','BuyPrice','PrevClosePrice','ClosePrice','ChangeReason')]
            names(DT.TradingRecord.Today) <- c('TradingDay','SecuCode','SecuAbbr', 'ChangeShares', 'ChangeValue','TradePrice','PrevClosePrice','ClosePrice','ChangeReason')
            DT.TradingRecord.History <- rbind(DT.TradingRecord.History,DT.TradingRecord.Today)
            #更新持仓表                      
            DT.HoldingRecord.Today <- data.table(TradingDay = Today
                                                 , SecuCode = DT.TradingRecord.Today$SecuCode
                                                 , SecuAbbr = DT.TradingRecord.Today$SecuAbbr
                                                 , HoldShares = DT.TradingRecord.Today$ChangeShares
                                                 , HoldValue = DT.TradingRecord.Today$ChangeValue
                                                 , AveCostPrice = DT.TradingRecord.Today$TradePrice
                                                 , YesterdayClosePrice = DT.TradingRecord.Today$PrevClosePrice
                                                 , TodayClosePrice = DT.TradingRecord.Today$ClosePrice
                                                 , FirstUpdateDay = Today
                                                 , LastUpdateDay = Today
                                                 , TodayShares = DT.TradingRecord.Today$ChangeShares
                                                 , YesterdayShares = 0
            )
          }
          
        }else{ # 已有昨仓的处理
          setkey(DT.HoldingRecord.Today,TradingDay, SecuCode)
          DF.DailyQuote.Buy <- copy(DF.DailyQuote.Adjust)
          DF.DailyQuote.Buy <- merge(DF.DailyQuote.Buy,DT.HoldingRecord.Today,by=key(DF.DailyQuote.Buy),all.x=T)
          setkey(DF.DailyQuote.Buy, TradingDay, SecuCode)
          if(TradePrice == 'OpenPrice'){
            DF.DailyQuote.Buy[, TargetShares:= 100*round(OpenRatio * TotalFund / (PrevClosePrice*100))] 
            DF.DailyQuote.Buy[, BuyPrice := OpenPrice]
            DF.DailyQuote.Buy[, TargetValue := TargetShares*OpenPrice]
            DF.DailyQuote.Buy[, HoldShares:=na.fill(HoldShares, 0)]
            DF.DailyQuote.Buy[, BuyShares:= TargetShares - HoldShares] 
            DF.DailyQuote.Buy[, BuyValue := BuyShares * OpenPrice]
            if(!IfAddReduce){
              DF.DailyQuote.Buy <- DF.DailyQuote.Buy[BuyShares>0 & HoldShares == 0]
            }else{
              DF.DailyQuote.Buy <- DF.DailyQuote.Buy[BuyShares>0]
            }
            
          }else if(TradePrice == 'ClosePrice'){
            DF.DailyQuote.Buy[, TargetShares:= 100*round(OpenRatio * TotalFund / (PrevClosePrice*100))] 
            DF.DailyQuote.Buy[, BuyPrice := ClosePrice]
            DF.DailyQuote.Buy[, TargetValue := TargetShares*ClosePrice]
            DF.DailyQuote.Buy[, HoldShares:=na.fill(HoldShares, 0)]
            DF.DailyQuote.Buy[, BuyShares:= TargetShares - HoldShares] 
            DF.DailyQuote.Buy[, BuyValue := BuyShares * ClosePrice]
            if(!IfAddReduce){
              DF.DailyQuote.Buy <- DF.DailyQuote.Buy[BuyShares>0 & HoldShares == 0]
            }else{
              DF.DailyQuote.Buy <- DF.DailyQuote.Buy[BuyShares>0]
            }
          }
          #涨停价处理
          {
            # DF.DailyQuote.Buy[,UpLimit:=  round(PrevClosePrice*1.1,2)]
            DF.DailyQuote.Buy.Fail <- DF.DailyQuote.Buy[BuyPrice >= UpLimit]
            #把涨停限制的交易信号移到下一个交易日
            if(nrow(DF.DailyQuote.Buy.Fail)!=0){
              Temp <- DF.DailyQuote.Buy.Fail[,names(DT.Signal),with=F]
              Temp[,TradingDay:=GetTradingDayLeadLag(TradingDay,Data.TradingDay,LeadLag = -1)]
              #如果删除DT.Signal里面已有的单子
              Temp.AlreadyIn <- DT.Signal[list(Temp[,.(TradingDay,SecuCode)])]
              setnames(Temp.AlreadyIn,'OpenRatio','Tag')
              Temp <- merge(Temp,Temp.AlreadyIn[,.(TradingDay, SecuCode, Tag)],by=c('TradingDay','SecuCode'),all.x=T)[is.na(Tag)]
              Temp <- Temp[,names(DT.Signal),with=F]
              if(nrow(Temp)!=0){
                if(IfReTrade){
                  Temp$Order <- 1000000
                  DT.Signal <- rbindlist(list(DT.Signal,Temp))
                  
                }
              }
              setkey(DT.Signal,TradingDay,SecuCode)
            }
            DF.DailyQuote.Buy <-  DF.DailyQuote.Buy[BuyPrice < UpLimit]
          }
          #买入资金限制
          BuyValueSum <- sum(DF.DailyQuote.Buy$BuyValue)
          if(BuyValueSum > AvailableFund & !IfShort)
          { 
            setorder(DF.DailyQuote.Buy, Order) # 根据已有的Order决定买入顺序
            DF.DailyQuote.Buy <- DF.DailyQuote.Buy[1:min(which(cumsum(DF.DailyQuote.Buy$BuyValue)>=AvailableFund)),]
            BuyValueSum <- sum(DF.DailyQuote.Buy$BuyValue)
            DeleteShares <- ceiling((BuyValueSum - AvailableFund)/ (100*as.numeric(DF.DailyQuote.Buy[.N, .(BuyPrice)])))*100
            DF.DailyQuote.Buy[.N, BuyShares :=  BuyShares- DeleteShares]
            DF.DailyQuote.Buy[.N, BuyValue :=  BuyValue - DeleteShares * BuyPrice]
            DF.DailyQuote.Buy <- DF.DailyQuote.Buy[BuyShares > 0]
            setkey(DF.DailyQuote.Buy, TradingDay, SecuCode)
          }
          if(nrow(DF.DailyQuote.Buy)!=0){
            #更新可用资金
            AvailableFund <- AvailableFund - sum(DF.DailyQuote.Buy$BuyValue)
            #添加交易记录-买入
            DF.DailyQuote.Buy[HoldShares > 0 ,ChangeReason := 3]#加仓
            DF.DailyQuote.Buy[HoldShares == 0 ,ChangeReason := 1]#开多
            DF.DailyQuote.Buy[HoldShares < 0 ,ChangeReason := 6]#平空
            
            DT.TradingRecord.Today <- DF.DailyQuote.Buy[,c('TradingDay','SecuCode','SecuAbbr.x', 'BuyShares', 'BuyValue','BuyPrice','PrevClosePrice','ClosePrice','ChangeReason')]
            names(DT.TradingRecord.Today) <- c('TradingDay','SecuCode','SecuAbbr', 'ChangeShares', 'ChangeValue','TradePrice','PrevClosePrice','ClosePrice','ChangeReason')
            DT.TradingRecord.History <- rbind(DT.TradingRecord.History,DT.TradingRecord.Today)
            DF.HoldingRecord.New <- data.table(TradingDay = Today #新交易产生的临时持有表
                                               , SecuCode = DT.TradingRecord.Today$SecuCode
                                               , SecuAbbr = DT.TradingRecord.Today$SecuAbbr
                                               , HoldShares = DT.TradingRecord.Today$ChangeShares
                                               , HoldValue = DT.TradingRecord.Today$ChangeValue
                                               , AveCostPrice = DT.TradingRecord.Today$TradePrice
                                               , YesterdayClosePrice = DT.TradingRecord.Today$PrevClosePrice
                                               , TodayClosePrice = DT.TradingRecord.Today$ClosePrice
                                               , FirstUpdateDay = Today
                                               , LastUpdateDay = Today
                                               , TodayShares = DT.TradingRecord.Today$ChangeShares
                                               , YesterdayShares = 0
            )
            #并表形成新的持有表
            DF.HoldingRecord.New <- rbind(DT.HoldingRecord.Today, DF.HoldingRecord.New)
            DT.HoldingRecord.Today <- DF.HoldingRecord.New[,list(HoldShares = sum(HoldShares) #股数相加
                                                                 , HoldValue = sum(HoldValue) #市值相加
                                                                 , AveCostPrice = ifelse(sum(HoldShares)!=0, round(sum(HoldShares*AveCostPrice)/sum(HoldShares),2), first(AveCostPrice)) #重新计算平均成本价
                                                                 , YesterdayClosePrice = mean(YesterdayClosePrice)
                                                                 , TodayClosePrice = mean(TodayClosePrice)
                                                                 , FirstUpdateDay = min(FirstUpdateDay) #取最早的为第一次开仓日
                                                                 , LastUpdateDay = max(LastUpdateDay) #取最晚的为最近一次开仓日
                                                                 , TodayShares = sum(TodayShares) 
                                                                 , YesterdayShares = sum(YesterdayShares)
            ),by=.(TradingDay,SecuCode,SecuAbbr)]
            DT.HoldingRecord.Today[YesterdayShares < 0, SharesDiff := TodayShares + YesterdayShares]
            DT.HoldingRecord.Today[YesterdayShares < 0, ":=" (TodayShares = pmax(SharesDiff, 0)
                                                              , YesterdayShares = pmin(SharesDiff, 0))]
            DT.HoldingRecord.Today[, SharesDiff:=NULL]
            
            
            DF.Close.Today <- DT.HoldingRecord.Today[HoldShares == 0]
            if(nrow(DF.Close.Today)!=0){
              Temp.BuyPrice<-  DF.DailyQuote.Buy[list(DF.Close.Today$TradingDay,DF.Close.Today$SecuCode),.(BuyPrice)]
              DF.Close.Today[, BuyPrice := Temp.BuyPrice]
              rm(Temp.BuyPrice)
              DF.Close.Today <- DF.Close.Today[, c('TradingDay','SecuCode','SecuAbbr','AveCostPrice','FirstUpdateDay','LastUpdateDay', 'BuyPrice')]
              DF.Close.Today[, Profit := -(BuyPrice/AveCostPrice-1)]
              DF.Close.Today[, Profit.ZZ500 := -(RangeMarketReturn(DT.Market
                                                                   , 'ZZ500'
                                                                   , FirstUpdateDay
                                                                   , GetTradingDayLeadLag(LastUpdateDay, Data.TradingDay, 1)))]
              DF.Close.Today[, Profit.HS300 := -(RangeMarketReturn(DT.Market
                                                                   , 'HS300'
                                                                   , FirstUpdateDay
                                                                   , GetTradingDayLeadLag(LastUpdateDay, Data.TradingDay, 1)))]
              DF.Close.Today[, Profit.ZZ1000 := -(RangeMarketReturn(DT.Market
                                                                    , 'ZZ1000'
                                                                    , FirstUpdateDay
                                                                    , GetTradingDayLeadLag(LastUpdateDay, Data.TradingDay, 1)))]
              DF.Close.Today[, Profit.GZ2000 := -(RangeMarketReturn(DT.Market
                                                                    , 'GZ2000'
                                                                    , FirstUpdateDay
                                                                    , GetTradingDayLeadLag(LastUpdateDay, Data.TradingDay, 1)))]
              DF.Close.Today[, ':='(
                Profit.HedgeZZ500 = Profit - Profit.ZZ500
                , Profit.HedgeHS300 = Profit - Profit.HS300
                , Profit.HedgeZZ1000 = Profit - Profit.ZZ1000
              )]
              DF.Close.Today <- DF.Close.Today[, -c('Profit.ZZ500', 'Profit.HS300', 'Profit.ZZ1000', 'Profit.GZ2000')]
              setnames(DF.Close.Today, 'BuyPrice', 'CloseTradePrice')
              DF.Close.Today[, Dir := 0]
              DT.Close.History <- rbind(DT.Close.History, DF.Close.Today)
            }
            rm(DF.Close.Today)
            DT.HoldingRecord.Today <- DT.HoldingRecord.Today[HoldShares != 0]
          }
        }
      }
    }
    #盘后处理模块
    if(nrow(DT.HoldingRecord.Today)!=0){
      DT.HoldingRecord.Today[,':='(
        HoldValue = HoldShares * TodayClosePrice
      )]
      HoldFund <-  sum(DT.HoldingRecord.Today$HoldValue)
      TotalFund <- HoldFund + AvailableFund
    }else{
      HoldFund <- 0
      TotalFund <- AvailableFund
    }
    #追加历史持仓表
    DT.HoldingRecord.History <- rbind(DT.HoldingRecord.History, DT.HoldingRecord.Today)
    #更新基金情况表
    
    DT.Summary[list(Today),':='(
      Net = (TotalFund) / FundUnits
      , Account.Available = AvailableFund
      , Account.Holding  = HoldFund
      , Account.Total = TotalFund 
      , ZZ500.DailyReturn = DT.Market$ZZ500[DT.Market$TradingDay == as.Date(Today)]
      , HS300.DailyReturn = DT.Market$HS300[DT.Market$TradingDay == as.Date(Today)]
      , ZZ1000.DailyReturn = DT.Market$ZZ1000[DT.Market$TradingDay == as.Date(Today)]
      , GZ2000.DailyReturn = DT.Market$GZ2000[DT.Market$TradingDay == as.Date(Today)]
    )]
  }
  DT.Summary[, Net.DailyReturn := diff(c(1, Net))/c(1,Net[1:(.N-1)])]
  return(list(DT.Summary = DT.Summary
              ,DT.Signal = DT.Signal
              ,DT.TradingRecord.History = DT.TradingRecord.History#交易记录
              ,DT.HoldingRecord.History = DT.HoldingRecord.History#持仓记录
              ,DT.HoldingRecord.Today = DT.HoldingRecord.Today#今持仓记录
              ,DT.TradingRecord.Today = DT.TradingRecord.Today#今交易记录
              ,DT.Close.History = DT.Close.History#平仓单记录
  ))
  
}
