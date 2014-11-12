setwd("D:/Data_Science/GitHub/makroSVA")
####
# Set timer
####
ptm <- proc.time()
# Read from MS Access Database
library(RODBC)
channel<- odbcConnectAccess("sva")
# Extract the stores stock per article
stores_init<-sqlFetch(channel, "STORES_DATA_EXPORT")
proc.time() - ptm
# Table per Store for Stock NNBP checkng Later
library(plyr)
init_stock_check_stores<-ddply(stores_init,("STORE_NO"), summarize, STOCK_VALUE_MUV=sum(STOCK_VALUE_MUV)
                               ,STOCK_VALUE_SELL_PR=sum(STOCK_VALUE_SELL_PR))
# Extract the third parties stock per article
third_parties_init<-sqlFetch(channel, "6000_Third_Parties")
names(third_parties_init)
# Table per Store for Stock NNBP checkng Later
init_stock_check_TP<-ddply(third_parties_init,("STORE_NO"), summarize, STOCK_VALUE_MUV=sum(STOCK_VALUE_MUV)
                           ,STOCK_VALUE_SELL_PR=sum(STOCK_VALUE_SELL_PR))
# Extract the HO prices fot the 99 WH
HO_prices<-sqlFetch(channel, "1000_HO_Articles")
# Extract the Other TP stock per article
TP99_init<-sqlFetch(channel, "99_oct14")
names(TP99_init)<-c("ART_NO", "ART_GRP_NO", "Sub", "DESCR",  "STOCK", "STOCK_VALUE_MUV", "Buyer")
TP99_sell_pr<-merge(x = TP99_init, y = HO_prices,all.x = TRUE)
TP99_sell_pr<-merge(x = TP99_init, y = HO_prices,all.x = TRUE, by.x = "ART_NO", by.y = "ART_NO")
TP99_sell_pr<-TP99_sell_pr[,names(TP99_sell_pr) %in% c("F_NF", "ART_GRP_NO.x","ART_NO","SELL_PR", "DESCR.x", "STOCK", "STOCK_VALUE_MUV" )]
TP99_sell_pr$STOCK_VALUE_SELL_PR<-TP99_sell_pr$SELL_PR * TP99_sell_pr$STOCK
init_stock_check_99<-data.frame("STORE_NO" = 99, "STOCK_VALUE_MUV" = sum(TP99_sell_pr$STOCK_VALUE_MUV)
                                ,STOCK_VALUE_SELL_PR=sum(TP99_sell_pr$STOCK_VALUE_SELL_PR))
init_stock_check<-rbind(init_stock_check_stores, init_stock_check_TP, init_stock_check_99)
rm(init_stock_check_stores,init_stock_check_TP,init_stock_check_99)

# Close the channel with the MS Access Database
odbcClose(channel)
proc.time() - ptm
#Read the stores 10 and 11
library(xlsx)
gc()
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
gc()
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
gc()
init_stock_check_10<-data.frame("STORE_NO" = 10, "STOCK_VALUE_MUV" = sum(store_10$STOCK_VALUE_MUV) 
                                ,STOCK_VALUE_SELL_PR=sum(store_10$STOCK_VALUE_SELL_PR))
init_stock_check_11<-data.frame("STORE_NO" = 11, "STOCK_VALUE_MUV" = sum(store_11$STOCK_VALUE_MUV)
                                ,STOCK_VALUE_SELL_PR=sum(store_11$STOCK_VALUE_SELL_PR))
init_stock_check<-rbind(init_stock_check, init_stock_check_10, init_stock_check_11)
rm(init_stock_check_10,init_stock_check_11)
proc.time() - ptm
# Check Stock Value with Stat_Margin
officialstock<-data.frame("STORE_NO" = 1:13, "off_stock_muv" = rep(0,13))
officialstock$off_stock_muv[1]<-read.xlsx("./Original/Stat_Margin_0115.xls", sheetName="Kif",
                         colIndex=26, rowIndex=410, header=FALSE)
officialstock$off_stock_muv[2]<-read.xlsx("./Original/Stat_Margin_0115.xls", sheetName="Pal",
                            colIndex=26, rowIndex=410, header=FALSE)
officialstock$off_stock_muv[3]<-read.xlsx("./Original/Stat_Margin_0115.xls", sheetName="The",
                            colIndex=26, rowIndex=410, header=FALSE)
gc()
officialstock$off_stock_muv[4]<-read.xlsx("./Original/Stat_Margin_0115.xls", sheetName="Cre",
                            colIndex=26, rowIndex=410, header=FALSE)
officialstock$off_stock_muv[5]<-read.xlsx("./Original/Stat_Margin_0115.xls", sheetName="Pat",
                            colIndex=26, rowIndex=410, header=FALSE)
officialstock$off_stock_muv[6]<-read.xlsx("./Original/Stat_Margin_0115.xls", sheetName="Lar",
                            colIndex=26, rowIndex=410, header=FALSE)
gc()
officialstock$off_stock_muv[7]<-read.xlsx("./Original/Stat_Margin_0115.xls", sheetName="TheII",
                            colIndex=26, rowIndex=410, header=FALSE)
officialstock$off_stock_muv[8]<-read.xlsx("./Original/Stat_Margin_0115.xls", sheetName="Xan",
                            colIndex=26, rowIndex=410, header=FALSE)
officialstock$off_stock_muv[9]<-read.xlsx("./Original/Stat_Margin_0115.xls", sheetName="Vol",
                            colIndex=26, rowIndex=410, header=FALSE)
gc()
officialstock$off_stock_muv[10]<-read.xlsx("./Original/Stat_Margin_0115.xls", sheetName="ST94",
                            colIndex=26, rowIndex=410, header=FALSE)
officialstock$off_stock_muv[11]<-read.xlsx("./Original/Stat_Margin_0115.xls", sheetName="ST95",
                             colIndex=26, rowIndex=410, header=FALSE)
officialstock$off_stock_muv[12]<-read.xlsx("./Original/Stat_Margin_0115.xls", sheetName="ST97",
                             colIndex=26, rowIndex=410, header=FALSE)
gc()
officialstock$off_stock_muv[13]<-read.xlsx("./Original/Store_98_97_96_0115.xls", sheetName="OtherTP",
                             colIndex=10, rowIndex=440, header=FALSE)
gc()
officialstock$STORE_NO[10:13]<-c(89,95,97,99)
officialstock$received<-rep(0,13)
officialstock$received[1]<-init_stock_check$STOCK_VALUE_MUV[init_stock_check$STORE_NO==10]+
        init_stock_check$STOCK_VALUE_MUV[init_stock_check$STORE_NO==1]
officialstock$received[4]<-init_stock_check$STOCK_VALUE_MUV[init_stock_check$STORE_NO==11]+
        init_stock_check$STOCK_VALUE_MUV[init_stock_check$STORE_NO==4]
officialstock$received[2]<-init_stock_check$STOCK_VALUE_MUV[init_stock_check$STORE_NO==2]
officialstock$received[3]<-init_stock_check$STOCK_VALUE_MUV[init_stock_check$STORE_NO==3]
officialstock$received[5]<-init_stock_check$STOCK_VALUE_MUV[init_stock_check$STORE_NO==5]
officialstock$received[6]<-init_stock_check$STOCK_VALUE_MUV[init_stock_check$STORE_NO==6]
officialstock$received[7]<-init_stock_check$STOCK_VALUE_MUV[init_stock_check$STORE_NO==7]
officialstock$received[8]<-init_stock_check$STOCK_VALUE_MUV[init_stock_check$STORE_NO==8]
officialstock$received[9]<-init_stock_check$STOCK_VALUE_MUV[init_stock_check$STORE_NO==9]
officialstock$received[10]<-init_stock_check$STOCK_VALUE_MUV[init_stock_check$STORE_NO==89]
officialstock$received[11]<-init_stock_check$STOCK_VALUE_MUV[init_stock_check$STORE_NO==95]
officialstock$received[12]<-init_stock_check$STOCK_VALUE_MUV[init_stock_check$STORE_NO==97]
officialstock$received[13]<-init_stock_check$STOCK_VALUE_MUV[init_stock_check$STORE_NO==99]
officialstock$off_stock_muv<-c(do.call("cbind",officialstock$off_stock_muv)) 
officialstock$check<-round(officialstock$received - officialstock$off_stock_muv, 2)
gc()
proc.time() - ptm
#Breakdown the store_init DataFrame to 9 smaller Dataframes

#Breakdown the third_parties_init DataFrame to 3 smaller Dataframes


# COP_expenses
cop<-read.xlsx("./Original/SVA2014_COP_Sep14.xls", sheetName="COP",
               colIndex=1:20, rowIndex=42:43, header=FALSE)
cop<-subset(cop, select=c(X1, X17))
names(cop)<- c("F_NF", "COP%")
gc()

# Selling Cost Expenses
sellcost<-read.xlsx("./Original/SVA2014_SellCost_Sep14.xls", sheetName="SC",
               colIndex=1:15, rowIndex=44:45, header=FALSE)
sellcost<-subset(sellcost, select=c(X1, X11))
names(sellcost)<- c("F_NF", "SellCost%")
gc()
# Percentage Use
#20.40 1-7, 11-12
Perc_2040_kif<-read.xlsx("./Original/PercentageUse_Oct2014.xls", sheetIndex=1,
                         colIndex=1:8, rowIndex=1:398, header=TRUE)
Perc_2040_kif<- Perc_2040_kif[ -c(2:7) ]
names(Perc_2040_kif)<-c("ART_GRP_NO", "OPC")
Perc_2040_kif$Store<-1
Perc_2040_pal<-read.xlsx("./Original/PercentageUse_Oct2014.xls", sheetIndex=2,
                         colIndex=1:8, rowIndex=1:398, header=TRUE)
Perc_2040_pal<- Perc_2040_pal[ -c(2:7) ]
names(Perc_2040_pal)<-c("ART_GRP_NO", "OPC")
Perc_2040_pal$Store<-2
Perc_2040_the<-read.xlsx("./Original/PercentageUse_Oct2014.xls", sheetIndex=3,
                         colIndex=1:8, rowIndex=1:398, header=TRUE)
Perc_2040_the<- Perc_2040_the[ -c(2:7) ]
names(Perc_2040_the)<-c("ART_GRP_NO", "OPC")
Perc_2040_the$Store<-3
gc()
Perc_2040_cre<-read.xlsx("./Original/PercentageUse_Oct2014.xls", sheetIndex=4,
                         colIndex=1:8, rowIndex=1:398, header=TRUE)
Perc_2040_cre<- Perc_2040_cre[ -c(2:7) ]
names(Perc_2040_cre)<-c("ART_GRP_NO", "OPC")
Perc_2040_cre$Store<-4
Perc_2040_pat<-read.xlsx("./Original/PercentageUse_Oct2014.xls", sheetIndex=5,
                         colIndex=1:8, rowIndex=1:398, header=TRUE)
Perc_2040_pat<- Perc_2040_pat[ -c(2:7) ]
names(Perc_2040_pat)<-c("ART_GRP_NO", "OPC")
Perc_2040_pat$Store<-5
Perc_2040_lar<-read.xlsx("./Original/PercentageUse_Oct2014.xls", sheetIndex=6,
                         colIndex=1:8, rowIndex=1:398, header=TRUE)
Perc_2040_lar<- Perc_2040_lar[ -c(2:7) ]
names(Perc_2040_lar)<-c("ART_GRP_NO", "OPC")
Perc_2040_lar$Store<-6
gc()
Perc_2040_ion<-read.xlsx("./Original/PercentageUse_Oct2014.xls", sheetIndex=7,
                         colIndex=1:8, rowIndex=1:398, header=TRUE)
Perc_2040_ion<- Perc_2040_ion[ -c(2:7) ]
names(Perc_2040_ion)<-c("ART_GRP_NO", "OPC")
Perc_2040_ion$Store<-7
Perc_2040_xan<-read.xlsx("./Original/PercentageUse_Oct2014.xls", sheetIndex=11,
                         colIndex=1:8, rowIndex=1:398, header=TRUE)
Perc_2040_xan<- Perc_2040_xan[ -c(2:7) ]
names(Perc_2040_xan)<-c("ART_GRP_NO", "OPC")
Perc_2040_xan$Store<-8
Perc_2040_vol<-read.xlsx("./Original/PercentageUse_Oct2014.xls", sheetIndex=12,
                         colIndex=1:8, rowIndex=1:398, header=TRUE)
Perc_2040_vol<- Perc_2040_vol[ -c(2:7) ]
names(Perc_2040_vol)<-c("ART_GRP_NO", "OPC")
Perc_2040_vol$Store<-9
gc()
Perc_2040<-rbind(Perc_2040_kif, Perc_2040_pal, Perc_2040_the, 
                 Perc_2040_cre, Perc_2040_pat, Perc_2040_lar, 
                 Perc_2040_ion, Perc_2040_xan, Perc_2040_vol)
rm(Perc_2040_kif, Perc_2040_pal, Perc_2040_the, 
                 Perc_2040_cre, Perc_2040_pat, Perc_2040_lar, 
                 Perc_2040_ion, Perc_2040_xan, Perc_2040_vol)


# Retros + ICD's 15:23
Perc_reticd_kif<-read.xlsx("./Original/PercentageUse_Oct2014.xls", sheetIndex=15,
                colIndex=1:14, rowIndex=1:398, header=TRUE)
Perc_reticd_kif<- Perc_reticd_kif[ -c(2:9, 11:13) ]
names(Perc_reticd_kif)<-c("ART_GRP_NO", "RETROS", "ICD")
Perc_reticd_kif$Store<-1
Perc_reticd_pal<-read.xlsx("./Original/PercentageUse_Oct2014.xls", sheetIndex=15,
                           colIndex=1:14, rowIndex=1:398, header=TRUE)
Perc_reticd_pal<- Perc_reticd_pal[ -c(2:9, 11:13) ]
names(Perc_reticd_pal)<-c("ART_GRP_NO", "RETROS", "ICD")
Perc_reticd_pal$Store<-2
Perc_reticd_the<-read.xlsx("./Original/PercentageUse_Oct2014.xls", sheetIndex=15,
                           colIndex=1:14, rowIndex=1:398, header=TRUE)
Perc_reticd_the<- Perc_reticd_the[ -c(2:9, 11:13) ]
names(Perc_reticd_the)<-c("ART_GRP_NO", "RETROS", "ICD")
Perc_reticd_the$Store<-3
gc()
Perc_reticd_cre<-read.xlsx("./Original/PercentageUse_Oct2014.xls", sheetIndex=15,
                           colIndex=1:14, rowIndex=1:398, header=TRUE)
Perc_reticd_cre<- Perc_reticd_cre[ -c(2:9, 11:13) ]
names(Perc_reticd_cre)<-c("ART_GRP_NO", "RETROS", "ICD")
Perc_reticd_cre$Store<-4
Perc_reticd_pat<-read.xlsx("./Original/PercentageUse_Oct2014.xls", sheetIndex=15,
                           colIndex=1:14, rowIndex=1:398, header=TRUE)
Perc_reticd_pat<- Perc_reticd_pat[ -c(2:9, 11:13) ]
names(Perc_reticd_pat)<-c("ART_GRP_NO", "RETROS", "ICD")
Perc_reticd_pat$Store<-5
Perc_reticd_lar<-read.xlsx("./Original/PercentageUse_Oct2014.xls", sheetIndex=15,
                           colIndex=1:14, rowIndex=1:398, header=TRUE)
Perc_reticd_lar<- Perc_reticd_lar[ -c(2:9, 11:13) ]
names(Perc_reticd_lar)<-c("ART_GRP_NO", "RETROS", "ICD")
Perc_reticd_lar$Store<-6
gc()
Perc_reticd_ion<-read.xlsx("./Original/PercentageUse_Oct2014.xls", sheetIndex=15,
                           colIndex=1:14, rowIndex=1:398, header=TRUE)
Perc_reticd_ion<- Perc_reticd_ion[ -c(2:9, 11:13) ]
names(Perc_reticd_ion)<-c("ART_GRP_NO", "RETROS", "ICD")
Perc_reticd_ion$Store<-7
Perc_reticd_xan<-read.xlsx("./Original/PercentageUse_Oct2014.xls", sheetIndex=15,
                           colIndex=1:14, rowIndex=1:398, header=TRUE)
Perc_reticd_xan<- Perc_reticd_xan[ -c(2:9, 11:13) ]
names(Perc_reticd_xan)<-c("ART_GRP_NO", "RETROS", "ICD")
Perc_reticd_xan$Store<-8
Perc_reticd_vol<-read.xlsx("./Original/PercentageUse_Oct2014.xls", sheetIndex=15,
                           colIndex=1:14, rowIndex=1:398, header=TRUE)
Perc_reticd_vol<- Perc_reticd_vol[ -c(2:9, 11:13) ]
names(Perc_reticd_vol)<-c("ART_GRP_NO", "RETROS", "ICD")
Perc_reticd_vol$Store<-9
gc()
Perc_reticd<-rbind(Perc_reticd_kif, Perc_reticd_pal, Perc_reticd_the, 
                   Perc_reticd_cre, Perc_reticd_pat, Perc_reticd_lar, 
                   Perc_reticd_ion, Perc_reticd_xan, Perc_reticd_vol)
rm(Perc_reticd_kif, Perc_reticd_pal, Perc_reticd_the, 
   Perc_reticd_cre, Perc_reticd_pat, Perc_reticd_lar, 
   Perc_reticd_ion, Perc_reticd_xan, Perc_reticd_vol)
Perc_store<-cbind(Perc_2040, Perc_reticd)
if (sum(Perc_store[,4] == Perc_store[,1])==max(dim(Perc_store))){
Perc_store<- Perc_store[-c(4,7)]
}
rm(Perc_2040, Perc_reticd)
proc.time() - ptm
# else print message


# Aging
aging<-read.xlsx("./Original/StockValuation_Structure_COM's.xls", sheetName="Min",
                 colIndex=3:10, rowIndex=1:398, header=T)
gc()
names(aging)[1]<-"ART_GRP_NO"
# SO_per Group
so_grp<-read.xlsx("./Original/SO_per_group_Oct14.xlsx", sheetName="Sheet1",
                  colIndex=1:4,rowIndex=3:400,  header=T)
gc()
names(so_grp)<-c("ART_GRP_NO", "SalesP", "SO", "SO_pct") 


# CU_Discounts
#######################################
##### Check Methodology, Prefer raw MDW
#######################################
cu_disc=data.frame()
for (i in 1:9){
df_temp<-read.xlsx2("./Original/CustomerDisc_ALLFD&NF_Oct14.xls", sheetIndex=i ,colIndex=1:6, 
                    startRow = 4, header=F)
df_temp$X1<-i
cu_disc<-rbind(cu_disc,df_temp)
df_temp<-0
gc()
print (i)
}
gc()
#names
names(cu_disc)<- c("Store", "F_NF", "ART_NO", "DESCR", "DISC", "SALES")
# fix class of data
cu_disc$DISC<-as.numeric(as.character(cu_disc$DISC))
cu_disc$SALES<-as.numeric(as.character(cu_disc$SALES))
# drop unnecessary columns
cu_disc$F_NF<-NULL
cu_disc$DESCR<-NULL
# calculate percentage
cu_disc$DISC_pct<- (-cu_disc$DISC) / cu_disc$SALES
# correct percentage
cu_disc$DISC_pct[is.na(cu_disc$DISC_pct)] <- 0
cu_disc$DISC_pct[cu_disc$DISC_pct>0] <- 0
cu_disc$DISC_pct[cu_disc$DISC_pct< -0.8] <- 0
gc()

########################################
#### Third Party Allocation
########################################
tp_alloc <- data.frame("STORE_NO" = 99, "ART_GRP_NO" = aging$ART_GRP_NO)
tabnames<-c("Kif", "Pal", "The", "Cre", "Pat", "Lar", "TheII", "Xan", "Vol")
# 99 based on Sales (?)
for ( i in 1:length(tabnames)){
        df_temp<- read.xlsx("./Original/Stat_Margin_0115.xls", sheetName=tabnames[i],
                            colIndex=5, rowIndex=4:407, header=FALSE)
        df_temp<-df_temp[-c(95),]
        tp_alloc<- cbind(tp_alloc,df_temp)
        df_temp<-0
        gc()
}
tabnames_tp<-c("PROODOS", "MAKIOS", "FL_South")
col_index<-1:9*7+10
df_fin<-data.frame("STORE_NO" = c(rep(89,397),rep(95,397), rep(97,397)), "ART_GRP_NO" = aging$ART_GRP_NO)
df_inter<-data.frame()
for ( i in 1:length(tabnames_tp)){
        df_temp<- read.xlsx("./Original/Store_98_97_96_0115.xls", sheetName=tabnames_tp[i],
                            colIndex=col_index, rowIndex=4:407, header=FALSE, colClasses = rep("numeric", 9))
        df_temp<-df_temp[-c(95:101),]
        print (dim(df_temp))
        df_inter<- rbind(df_inter,df_temp)
        df_temp<-0
        gc()
}
if (dim(df_inter)[1]==3*397-1){
        df_inter<- rbind(df_inter[1:491,], 0, df_inter[492:dim(df_inter)[1],])
}
# dim(df_inter)
df_fin<-cbind(df_fin,df_inter)
names(df_fin)<-names(tp_alloc)
tp_alloc<-rbind(tp_alloc, df_fin)
rm(df_temp, df_inter, df_fin)
names(tp_alloc)[3:11]<- c("stst1","stst2", "stst3", "stst4", "stst5", "stst6", "stst7", "stst8", "stst9")
for (i in 1: dim(tp_alloc)[1]){
        tp_alloc$sumstst[i]<- sum(tp_alloc[i,3:11])
        if (tp_alloc$sumstst[i] ==0){
                tp_alloc[i,13:21]<-0
        } else {
                for (j in 13:21){
                        tp_alloc[i,j]<-tp_alloc[i,j-10]/tp_alloc[i,12]
                }
        }
}
names(tp_alloc)[13:21]<-c("pctst1","pctst2", "pctst3", "pctst4", "pctst5", "pctst6", "pctst7",
                          "pctst8", "pctst9")
gc()
# write.xlsx(x = tp_alloc, file = "tp_alloc.xlsx",
#            sheetName = "TestSheet", row.names = FALSE)

# Finish  - Print Timer
(proc.time() - ptm)/60