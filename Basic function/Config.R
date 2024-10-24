### 日期: 2019-04-30
### 作者: HuanfengDong
### 该文件是配置头文件，储存各种通用配置，一般脚本只从该文件source通用配置。
### =================================================================================== ### 
library(data.table)
### =================================================================================== ### 
## 路径配置
Dir.Work <- "F:/"
Dir.Data <- paste0('//192.168.8.38/shares/JYDBRData/') #可设置含科创板与不含科创板数据路径
Dir.MinuteRData <- paste0(Dir.Data,"Sto_MinuteRdata/")
Dir.DataDailyUpdate <- paste0(Dir.Work,'Data/')
Dir.BlackList <- paste0(Dir.Work,'BlackList/')
# Dir.FactorGenerate <- paste0(Dir.Work,'FactorGenerate/')
Dir.FactorGenerate <- paste0(Dir.Work,'others/给实习生的资料/因子生成回测示例代码/并行计算/')
Dir.MonitorPDF <-  paste0(Dir.Work,'Monitor/MonitorPDF/')
Dir.Monitor <-  paste0(Dir.Work,'Monitor/')
Dir.ProblemRecord <-  paste0(Dir.Work,'ProblemRecord/')
Dir.Research <- paste0(Dir.Work,'StockResearch/')
## 账号密码配置
Trader <- ""
## ODBC信息
ODBC.Server <- "server"
ODBC.StockTrade <- "stocktrade"
ODBC.StockResearch <- "JYDB"
ODBC.JYDB <- "JYDB"
ODBC.StockMinute <- "stockminute"
## 数据库信息
SQL.Address <- "192.168.8.38"
SQL.User <- 'AnRanLiu'
SQL.Pwd <- 'Sql1234'

SQL.DB.StockTrade <- "StockTrade"
SQL.DB.StockResearch <- "StockResearch"
SQL.DB.Personal <- ""
## 数据表
SQL.Table.StrategyPortfolio <- "Sto_StrategyPortfolio"
SQL.Table.IndexComponent <- "IndexComponent"
SQL.Table.BlackList <- "BlackList"
SQL.Table.Sto_DailyHolding <- "Sto_DailyHolding"
SQL.Table.LGT_StockHold <- "LGT_StockHold"

