setwd("D:/Data_Science/GitHub/makroSVA")

# Read from MS Access Database
library(RODBC)
channel<- odbcConnectAccess("sva")
# Extract the stores stock per article
stores_init<-sqlFetch(channel, "STORES_DATA_EXPORT")
names(stores_init)
# Table per Store for Stock NNBP checkng Later
library(plyr)
init_stock_check_stores<-ddply(stores_init,("STORE_NO"), summarize, STOCK_VALUE_MUV=sum(STOCK_VALUE_MUV))
# Extract the third parties stock per article
third_parties_init<-sqlFetch(channel, "6000_Third_Parties")
names(third_parties_init)
# Table per Store for Stock NNBP checkng Later
init_stock_check_TP<-ddply(third_parties_init,("STORE_NO"), summarize, STOCK_VALUE_MUV=sum(STOCK_VALUE_MUV))
# Extract the Other TP stock per article
TP99_init<-sqlFetch(channel, "99_oct14")
names(TP99_init)<-c("ART_NO", "ART_GRP_NO", "Sub", "DESCR",  "STOCK", "STOCK_VALUE_MUV", "Buyer")
init_stock_check_99<-data.frame("STORE_NO" = 99, "STOCK_VALUE_MUV" = sum(TP99_init$STOCK_VALUE_MUV))
init_stock_check<-rbind(init_stock_check_stores, init_stock_check_TP, init_stock_check_99)
rm(init_stock_check_stores,init_stock_check_TP,init_stock_check_99)
# Extract the HO prices fot the 99 WH
HO_prices<-sqlFetch(channel, "1000_HO_Articles")
# Close the channel with the MS Access Database
odbcClose(channel)
#Read the stores 10 and 11
library(xlsx)
# store_10<-read.xlsx2("./Original/files received/Stores_10_and_11.xlsx", sheetIndex=1,
#                     startRow = 10, header=FALSE,stringsAsFactors=FALSE,colClasses =
#         c("factor", "factor", "factor", "factor","factor", "factor", "numeric", "numeric", "numeric"))
store_10<-read.xlsx2("./Original/files received/Stores_10_and_11.xlsx", sheetIndex=1,
                     startRow = 10, header=FALSE,stringsAsFactors=FALSE)
store_10<-store_10[,!names(store_10) %in% c("X1","X4")]
names(store_10)<-c("F_NF", "ART_GRP_NO", "ART_NO", "DESCR", "STOCK_VALUE_MUV", "STOCK", "STOCK_VALUE_SELL_PR")
store_10$F_NF<- gsub("NONFOOD", "NON_FOOD", store_10$F_NF)
store_10$STOCK_VALUE_MUV<-as.numeric(store_10$STOCK_VALUE_MUV)
store_10$STOCK<-as.numeric(store_10$STOCK)
store_10$STOCK_VALUE_SELL_PR<-as.numeric(store_10$STOCK_VALUE_SELL_PR)
store_10$tot<- store_10$STOCK_VALUE_MUV+store_10$STOCK+store_10$STOCK_VALUE_SELL_PR 
store_10<-store_10[store_10$tot!=0,]
store_10$tot<-NULL
store_11<-read.xlsx2("./Original/files received/Stores_10_and_11.xlsx", sheetIndex=2
                     ,startRow = 10, header=FALSE,stringsAsFactors=FALSE)
store_11<-store_11[,!names(store_11) %in% c("X1","X4")]
names(store_11)<-c("F_NF", "ART_GRP_NO", "ART_NO", "DESCR", "STOCK_VALUE_MUV", "STOCK", "STOCK_VALUE_SELL_PR")
store_11$F_NF<- gsub("NONFOOD", "NON_FOOD", store_11$F_NF)
store_11$STOCK_VALUE_MUV<-as.numeric(store_11$STOCK_VALUE_MUV)
store_11$STOCK<-as.numeric(store_11$STOCK)
store_11$STOCK_VALUE_SELL_PR<-as.numeric(store_11$STOCK_VALUE_SELL_PR)
store_11$tot<- store_11$STOCK_VALUE_MUV+store_11$STOCK+store_11$STOCK_VALUE_SELL_PR 
store_11<-store_11[store_11$F_NF=="FOOD" |store_11$F_NF=="NON_FOOD" ,]
store_11<-store_11[store_11$tot!=0,]
store_11$tot<-NULL

init_stock_check_10<-data.frame("STORE_NO" = 10, "STOCK_VALUE_MUV" = sum(store_10$STOCK_VALUE_MUV))
init_stock_check_11<-data.frame("STORE_NO" = 11, "STOCK_VALUE_MUV" = sum(store_11$STOCK_VALUE_MUV))
init_stock_check<-rbind(init_stock_check, init_stock_check_10, init_stock_check_11)
rm(init_stock_check_10,init_stock_check_11)
#Breakdown the store_init DataFrame to 9 smaller Dataframes

#Breakdown the third_parties_init DataFrame to 3 smaller Dataframes


# COP_expenses
cop<-read.xlsx("./Original/SVA2014_COP_Sep14.xls", sheetName="COP",
               colIndex=1:20, rowIndex=42:43, header=FALSE)
cop<-subset(cop, select=c(X1, X17))
names(cop)<- c("F_NF", "COP%")


# Selling Cost Expenses
sellcost<-read.xlsx("./Original/SVA2014_SellCost_Sep14.xls", sheetName="SC",
               colIndex=1:15, rowIndex=44:45, header=FALSE)
sellcost<-subset(sellcost, select=c(X1, X11))
names(sellcost)<- c("F_NF", "SellCost%")



library(XLConnect)
wk <- loadWorkbook("~/Original/Stat_Margin_0115.xls")

library(xlsx)
# Percentage Use

# Aging
