#Well centre results prediction
#Author:Amber Huang(qian.huang@ucalgary.ca/qhuang@360eec.com)

################################## Test Data Loading #################################
rm(list = ls(all = TRUE))
library(reticulate)
library(dplyr)
library(readxl)
library(writexl)
library(stringi)

setwd("/Users/qianhuang/Desktop/360/model/extracting attributes")   #Change the path to where the files are saved


###Obtain attributes from CARTOFACT.com using Python
use_python("/opt/homebrew/bin/python3")

attribute <- c('licence','uwi_formatted','geom',
               'spud_date','cumulative_oil_production_m3',
               'cumulative_gas_production_e3m3','cumulative_water_production_m3',
               'cumulative_condensate_production_bbl','completion_interval_bottom_mkb',
               'prod_ip3_oil_bbld','prod_ip3_gas_mcfd',
               'ground_elevation','status_full',
               'well_abandoned_date',
               'true_vertical_depth','measured_depth',
               'last_production_date',
               'prod_ip3_boe_boed','prod_mr3_wtr_bbld',
               'prod_mr3_oil_bbld')
table <- c('live_well_sk')
at_table <- as.data.frame(cbind(attribute,table))
write_xlsx(at_table,"/Users/qianhuang/Desktop/360/model/extracting attributes/at_table.xlsx")


#Obtain the attributes
library(reticulate)

# Specify the path to your Python file with spaces, escaping the spaces with a backslash
py_run_file("/Users/qianhuang/Desktop/360/model/extracting attributes/extracting cartofact attributes sk.py")
testdata <- read.csv("/Users/qianhuang/Desktop/360/model/extracting attributes/attribute_df.csv")



#Rename columns
colnames(testdata)[colnames(testdata) == "licence"] <- "Licence"
colnames(testdata)[colnames(testdata) == "uwi_formatted"] <- "UWI"
colnames(testdata)[colnames(testdata) == "spud_date"] <- "Spud.Date"
colnames(testdata)[colnames(testdata) == "cumulative_oil_production_m3"] <- "Cum..Oil.Production..m3."
colnames(testdata)[colnames(testdata) == "cumulative_gas_production_e3m3"] <- "Cum..Gas.Production..e3m3."
colnames(testdata)[colnames(testdata) == "cumulative_water_production_m3"] <- "Cum..Water.Production..m3."
colnames(testdata)[colnames(testdata) == "cumulative_condensate_production_bbl"] <- "Cum..Condensate.Production..bbl."
colnames(testdata)[colnames(testdata) == "completion_interval_bottom"] <- "Completion.Bottom..mKB."
colnames(testdata)[colnames(testdata) == "prod_ip3_oil_bbld"] <- "Initial.3Mth.Prod..Oil..BBL.d."
colnames(testdata)[colnames(testdata) == "prod_ip3_gas_mcfd"] <- "Initial.3Mth.Prod..Gas..Mcf.d."
colnames(testdata)[colnames(testdata) == "ground_elevation"] <- "Ground.Elevation..m.above.sea.level."
colnames(testdata)[colnames(testdata) == "status_full"] <- "Full.Status"
colnames(testdata)[colnames(testdata) == "surf_aband_date"] <- "Surface.Aband..Date"
colnames(testdata)[colnames(testdata) == "true_vertical_depth"] <- "True.Vertical.Depth..m.TVD."
colnames(testdata)[colnames(testdata) == "measured_depth"] <- "Total.Well.Depth..mKB."
colnames(testdata)[colnames(testdata) == "oil_in_place_e3m3"] <- "Oil.In.Place..e3m3."
colnames(testdata)[colnames(testdata) == "last_production_date"] <- "Last.Production.Date"
colnames(testdata)[colnames(testdata) == "prod_ip3_boe_boed"] <- "Initial.3Mth.Prod..BOE..BOE.d."
colnames(testdata)[colnames(testdata) == "prod_mr3_wtr_bbld"] <- "Last.3Mth.Prod..Water..BBL.d."
colnames(testdata)[colnames(testdata) == "prod_mr3_oil_bbld"] <- "Last.3Mth.Prod..Oil..BBL.d."




lics <- testdata$Licence
uwis <- testdata$UWI


#####################################Matching attributes from different sheet
# #sheet CASING('Casing' only taken value of "SURFACE")   # Here is the reading sheet "CASING"'s attributes for the well data will be tested
# casing=read.csv("WELLS_SK_10_26_CASING.csv",as.is=TRUE, header=TRUE)
# casing <- as.data.frame(casing)
# casing <- casing[casing$Casing.Type == 'SURFACE',]
# testdata['Casing'] <- NA
# testdata['Outside.Diameter..mm.'] <- NA
# for (i in 1:dim(casing)[1]){
#   loc=which(sapply(uwis,function(x) casing$UWI[i] %in% x))
#   testdata[loc,"Casing"] <- casing[i,"Casing.Type"]
#   testdata[loc,'Outside.Diameter..mm.'] <- casing[i,"Outside.Diameter"]
# }



# ### Re-name columns of testdata
# testdata <- testdata %>% 
#   rename("Total.Well.Depth..mKB." = "Total.Depth..mKB.",
#          "Initial.3Mth.Prod..Oil..BBL.d." = "Initial.3.mth.Oil.Prod...bbl.d.",
#          "Last.3Mth.Prod..Oil..BBL.d." = "Last.3.mth.Oil.Prod...bbl.d.",
#          "Last.3Mth.Prod..Water..BBL.d." = "Last.3.mth.Water.Prod...bbl.d.",
#          "Initial.3Mth.Prod..BOE..BOE.d." = "Initial.3.mth.BOE.Prod...BOE.d.",
#          "Full.Status" = "Status")


### Add "Well.Type.Final" attributes
testdata['Well.Type.Final'] <- NA
for (i in 1:length(lics)){
  if (grepl("Oil", testdata$Full.Status[i])){
    testdata$Well.Type.Final[i]="oil"
  }
  else if (grepl("Water", testdata$Full.Status[i])){
    testdata$Well.Type.Final[i]="water"
  }
  else if (grepl("Gas", testdata$Full.Status[i])){
    testdata$Well.Type.Final[i]="gas"
  }
  else if ((testdata$Cum..Gas.Production..e3m3.[i]==0 || is.na(testdata$Cum..Gas.Production..e3m3.[i])) &
           (testdata$Cum..Water.Production..m3.[i]==0 || is.na(testdata$Cum..Water.Production..m3.[i])) &
           (testdata$Cum..Oil.Production..m3.[i]==0 || is.na(testdata$Cum..Oil.Production..m3.[i]))){
    testdata$Well.Type.Final[i]="did not produce"
  }
  else if(testdata$Cum..Oil.Production..m3.[i]!=0 & !(is.na(testdata$Cum..Oil.Production..m3.[i]))){
    testdata$Well.Type.Final[i]="oil"
  }
  else if((testdata$Cum..Oil.Production..m3.[i]==0 || is.na(testdata$Cum..Oil.Production..m3.[i])) &
          (testdata$Cum..Water.Production..m3.[i]!=0 & !(is.na(testdata$Cum..Water.Production..m3.[i])))){
    testdata$Well.Type.Final[i]="water"
  }
  else if((testdata$Cum..Oil.Production..m3.[i]==0 || is.na(testdata$Cum..Oil.Production..m3.[i])) &
          (testdata$Cum..Water.Production..m3.[i]==0 || is.na(testdata$Cum..Water.Production..m3.[i])) &
          (testdata$Cum..Gas.Production..e3m3.[i]!=0 & !(is.na(testdata$Cum..Gas.Production..e3m3.[i])))){
    testdata$Well.Type.Final[i]="gas"
  }
}





###
licence <- unique(testdata$Licence)
tab <- table(testdata$Licence)
tabname <- names(tab)
loc <- which(tab[as.character(licence)]>1)
loc <- names(loc)
nonrep <- testdata[!testdata$Licence %in% loc,]

#Build sub data frame of well with UWIs >= 2, name it repuwi
#Un-comment the "Well.Type.Final" if the data have this attributes
#For any attributes that are not applicable in CARTOFACT, comment the corresponding line in following loop.
repuwi=data.frame()
for (i in 1:length(loc)){       
  b=testdata[testdata$Licence %in% c(loc[i]),]
  a=b[1,]
  if ("gas" %in% b$Well.Type.Final && "oil" %in% b$Well.Type.Final && "water" %in% b$Well.Type.Final){
    a$Well.Type.Final = "oil"
  }
  else if("gas" %in% b$Well.Type.Final && "oil" %in% b$Well.Type.Final){
    a$Well.Type.Final = "oil"
  }
  else if("gas" %in% b$Well.Type.Final && "water" %in% b$Well.Type.Final){
    a$Well.Type.Final = "water"
  }
  else if("oil" %in% b$Well.Type.Final && "water" %in% b$Well.Type.Final){
    a$Well.Type.Final = "oil"
  }
  else if("gas" %in% b$Well.Type.Final){
    a$Well.Type.Final = "gas"
  }
  else if("oil" %in% b$Well.Type.Final){
    a$Well.Type.Final = "oil"
  }
  else if("water" %in% b$Well.Type.Final){
    a$Well.Type.Final = "water"
  }else{
    a$Well.Type.Final = "did not produce"
  }
  a$Spud.Date=min(b$Spud.Date,na.rm = TRUE)
  a$Cum..Water.Production..m3. = sum(b$Cum..Water.Production..m3.,na.rm=TRUE)
  a$Cum..Oil.Production..m3. = sum(b$Cum..Oil.Production..m3.,na.rm=TRUE)
  a$Cum..Gas.Production..e3m3. = sum(b$Cum..Gas.Production..e3m3.,na.rm=TRUE)
  a$Cum..Condensate.Production..bbl. = sum(b$Cum..Condensate.Production..bbl.,na.rm=TRUE)
  a$Initial.3Mth.Prod..Oil..BBL.d. = sum(b$Initial.3Mth.Prod..Oil..BBL.d.,na.rm=TRUE)
  a$Initial.3Mth.Prod..Gas..Mcf.d. = sum(b$Initial.3Mth.Prod..Gas..Mcf.d.,na.rm=TRUE)
  a$Last.3Mth.Prod..Oil..BBL.d. = sum(b$Last.3Mth.Prod..Oil..BBL.d.,na.rm=TRUE)
  a$Last.3Mth.Prod..Water..BBL.d. = sum(b$Last.3Mth.Prod..Water..BBL.d.,na.rm=TRUE)
  a$Initial.3Mth.Prod..BOE..BOE.d. = sum(b$Initial.3Mth.Prod..BOE..BOE.d.,na.rm=TRUE)
  
  repuwi=rbind(repuwi,a)
}

#combine nonrep and repuwi to testdata
testdata=rbind(repuwi, nonrep)
testdata[testdata==-Inf] <- NA
testdata[testdata==Inf] <- NA

#replace NA with 0 for part of attributes
prod=c("Cum..Gas.Production..e3m3.","Cum..Condensate.Production..bbl.",
       "Cum..Oil.Production..m3.","Cum..Water.Production..m3.",
       "Last.3Mth.Prod..Oil..BBL.d.","Last.3Mth.Prod..Water..BBL.d.",
       "Initial.3Mth.Prod..BOE..BOE.d.","Initial.3Mth.Prod..Oil..BBL.d.",
       "Initial.3Mth.Prod..Gas..Mcf.d.")#If any attributes not applicable in CARTOFACT for the data, delete it here.
for (i in 1:length(prod)){
  testdata[prod[i]][is.na(testdata[prod[i]])] <- 0
}


lics <- testdata$Licence
uwis <- testdata$UWI


################ select variables for the model
cate<-as.data.frame(testdata[,c("Well.Type.Final")]) # Add "LLR",Well.Type.Final" when it becomes available
names(cate)[1] <- "Well.Type.Final"


for (i in 1:length(cate)) {
  cate[,i]=as.factor(cate[,i]);
}

date <- testdata[,c("Spud.Date","Last.Production.Date")] 
date <- mutate_all(date, function(x) as.numeric(as.character(x)))

nume <- testdata[,c("Total.Well.Depth..mKB.","True.Vertical.Depth..m.TVD.",
                    "Cum..Water.Production..m3.","Cum..Condensate.Production..bbl.",
                    "Cum..Oil.Production..m3.","Cum..Gas.Production..e3m3.",
                    "Last.3Mth.Prod..Oil..BBL.d.","Initial.3Mth.Prod..Oil..BBL.d.",
                    "Last.3Mth.Prod..Water..BBL.d.","Initial.3Mth.Prod..BOE..BOE.d.",
                    "Initial.3Mth.Prod..Gas..Mcf.d.")]


# combine True.Vertical.Depth and Total.Well.Depth
nume$True.Vertical.Depth..m.TVD.[is.na(nume$True.Vertical.Depth..m.TVD.)] <- 0
for (i in 1:dim(nume)[1]) {
  if (nume$True.Vertical.Depth..m.TVD.[i]==0){
    nume$True.Vertical.Depth..m.TVD.[i]=nume$Total.Well.Depth..mKB.[i]
  }
}
nume$Total.Well.Depth..mKB.=c()

nume <- cbind(date,nume)


# #This is the complete version of attributes,
# #Delete the un-applicable attributes for the data, 
# var_nume=c("Spud.Date","Remaining.Energy.Content..TJ.","Last.Production.Date", "Cum..Oil.Production..m3.",
#            "Cum..Water.Production..m3.","Initial.3Mth.Prod..Oil..BBL.d.","Last.3Mth.Prod..Oil..BBL.d.",
#            "Last.3Mth.Prod..Water..BBL.d.","Initial.3Mth.Prod..BOE..BOE.d.",
#            "Volume.Recovered.1","Cumulative.Marketable.Production..e3m3.",
#            "Oil.In.Place..e3m3.")# select all the numerical attributes will be used in model

var_nume=c("Spud.Date","Cum..Oil.Production..m3.",
           "Cum..Water.Production..m3.","Initial.3Mth.Prod..Oil..BBL.d.",
           "Last.3Mth.Prod..Oil..BBL.d.","Last.3Mth.Prod..Water..BBL.d.",
           "Initial.3Mth.Prod..BOE..BOE.d.")# select all the numerical attributes will be used in model



lognume_test<-nume[,var_nume]




logcate_test <- cate[,c("Well.Type.Final")]
logcate_test <- as.data.frame(logcate_test)
colnames(logcate_test) <- c("Well.Type.Final")


logcate_test2 <- cate[,c("Well.Type.Final")]
logcate_test2 <- as.data.frame(logcate_test2)
colnames(logcate_test2) <- c("Well.Type.Final")



























###################################Well Center Models###########################
#load data
setwd("/Users/qianhuang/Desktop/360/model/model ph2 vs well center ")
Data=read.csv("Environmental Data Collection V1_Jul6_clean.csv", as.is=TRUE, header=TRUE)
Data <- as.data.frame(Data)
Sample=read.csv("DDP1 Dataset at Aug 10_MAIN.csv",as.is=TRUE, header=TRUE)
Sample0=read.csv("DDP1 Dataset at Aug 10_MAIN.csv",as.is=TRUE, header=TRUE)
Sample <- as.data.frame(Sample)


##############################Data file cleaning
################## Check the duplicated UWI rows in results Data file
# Remove white space in the UWI of Data
Data$UWI <- stri_replace_all_fixed(Data$UWI, " ", "")
Data$UWI <- stri_replace_all_fixed(Data$UWI, "&", ",")
##unify the 'Source Date'
#date type 2017-2022
Data$Source.Dates[which(Data$Source.Dates == "2017-2022")] <- 2022. #date type 2017-2022
#date type with month
mon <- c("Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec")
for (i in 1:length(Data$Source.Dates)){
  for(j in 1:length(mon)){
    if (grepl(mon[j], Data$Source.Dates[i])){
      Data$Source.Dates[i] <- as.numeric(stri_sub(Data$Source.Dates[i],-2,-1))+2000
    }
  }
}
#date type with format yyyy-mm-dd
for (i in 1:length(Data$Source.Dates)){
  if (nchar(Data$Source.Dates[i]) >=4){
    Data$Source.Dates[i] <- as.numeric(stri_sub(Data$Source.Dates[i],1,4))
  }
}

##Delete duplicated UWI and keep the latest one
dup <- subset(Data,duplicated(UWI))
Data <- Data %>% distinct(UWI, Phase.2.ESA.Result, .keep_all = TRUE)
dup <- subset(Data,duplicated(UWI))

for (i in 1:dim(dup)[1]){
  loc=which(Data$UWI == dup$UWI[i])
  loc=loc[-which.max(Data$Source.Dates[loc])]
  Data=Data[-loc,]
}
dup <- subset(Data,duplicated(UWI))

############### Add licence to Data
Data$UWI <- stri_replace_all_fixed(Data$UWI, "​", "")
Data<-data.frame(Licence=Data$Licence.Number, UWI=Data$UWI, 
                 PFW=Data$Phase.2.ESA.Well.Centre, date=Data$Source.Dates)

Data<-Data[Data$PFW!="",]
Data<-Data[Data$PFW!="-",]

#write.csv(Data,"Data_ph1 well center.csv")


for(i in 1:dim(Data)[1]){
  if(Data$UWI[i] %in% Sample0$UWI){
    loc=which(Data$UWI[i]==Sample0$UWI)
    Data$Licence[i]=Sample0$Licence[loc]
  }
}

# ###Data-combine the pass and fail results for duplicated UWI
# Data <- Data[!Data$Licence=="",]
# 
# dup <- subset(Data,duplicated(Licence))
# for (i in 1:dim(dup)[1]){
#   loc=which(Data$Licence == dup$Licence[i])
#   loc=loc[-which.max(Data$date[loc])]
#   Data=Data[-loc,]
# }
# dup <- subset(Data,duplicated(Licence))


##################
lics <- Sample$Licence
lic <- Data$Licence
uwis <- Sample$UWI
uwi <- Data$UWI


#####################################Matching attributes from different sheet
#sheet GAS FIELD
gas=read.csv("DDP1 Dataset at Aug 10_GASFIELD.csv",as.is=TRUE, header=TRUE)
gas <- as.data.frame(gas)

common_col <- intersect(names(Sample),names(gas))
unique_col <- setdiff(names(gas),common_col)
Sample <- merge(Sample, gas[, c('UWI', unique_col)], by = 'UWI', all.x = TRUE)



#sheet CASING('Casing' only taken value of "SURFACE")
casing=read.csv("DDP1 Dataset at Aug 10_CASING.csv",as.is=TRUE, header=TRUE)
casing <- as.data.frame(casing)
casing <- casing[casing$Casing == 'SURFACE',]

common_col <- intersect(names(Sample),names(casing))
unique_col <- setdiff(names(casing),common_col)
Sample <- merge(Sample, casing[, c('UWI', unique_col)], by = 'UWI', all.x = TRUE)



#sheet CEMENTING(only 750 unique UWI, duplicate UWIs)
cement=read.csv("DDP1 Dataset at Aug 10_CEMENTING.csv",as.is=TRUE, header=TRUE)
cement <- as.data.frame(cement)
#count cementing times
cem.table <- as.data.frame(table(cement$UWI))#count cementing times
colnames(cem.table) <- c("UWI", "Cement.Times")

common_col <- intersect(names(Sample),names(cem.table))
unique_col <- setdiff(names(cem.table),common_col)
Sample <- merge(Sample, cem.table[, c('UWI', unique_col)], by = 'UWI', all.x = TRUE)

#cementing amount
cement$Cement.Unit[cement$Cement.Unit=="Tonnes"] <- 1
cement$Cement.Unit[cement$Cement.Unit=="Cubic metres"] <- 0.3531
cement$Cement.Unit[cement$Cement.Unit=="00"] <- 0
cement$Cement.Unit[cement$Cement.Unit=="Slacks"] <- 0.182
cement$Cement.Unit=as.numeric(cement$Cement.Unit)
cement$Cement.Amount=cement$Cement.Amount*cement$Cement.Unit

cement_amount <- cement %>%
  group_by(UWI) %>%
  summarise(Cement.Amount = sum(Cement.Amount, na.rm = TRUE))

Sample <- merge(Sample, cement_amount[, c('UWI', "Cement.Amount")], by = 'UWI', all.x = TRUE)
Sample$Cement.Amount[Sample$Cement.Amount==0] <- NA



#sheet INCIDENTS
incident=read.csv("DDP1 Dataset at Aug 10_INCIDENTS.csv",as.is=TRUE, header=TRUE)
incident <- as.data.frame(incident)

common_col <- intersect(names(Sample),names(incident))
unique_col <- setdiff(names(incident),common_col)
Sample <- merge(Sample, incident[, c('UWI', unique_col)], by = 'UWI', all.x = TRUE)

Sample$Volume.Recovered.1[is.na(Sample$Volume.Recovered.1)] <- 0
Sample$Volume.Released.1[is.na(Sample$Volume.Released.1)] <- 0



#sheet OIL FIELD
oil=read.csv("DDP1 Dataset at Aug 10_OILFIELD.csv")
oil <- as.data.frame(oil)
oil <- oil[,c("UWI","Cumulative.Oil.Production..e3m3.","Initial.Establish.Reserves.Oil.Enhanced..e3m3.","Initial.Establish.Reserves.Oil.Primary..e3m3.","Initial.Establish.Reserves.Oil.Total..e3m3.","Oil.In.Place..e3m3.","Remaining.Established.Oil.Reserves..e3m3.")]

common_col <- intersect(names(Sample),names(oil))
unique_col <- setdiff(names(oil),common_col)
Sample <- merge(Sample, oil[, c('UWI', unique_col)], by = 'UWI', all.x = TRUE)





#############################
#Remove rows of well with UWIs >= 2, name the remaining nonrep
licence <- unique(Sample$Licence)
tab <- table(Sample$Licence)
tabname <- names(tab)
loc <- which(tab[as.character(licence)]>1)
loc <- names(loc)
nonrep <- Sample[!Sample$Licence %in% loc,]

#Build sub data frame of well with UWIs >= 2, name it repuwi
repuwi=data.frame()
for (i in 1:length(loc)){
  b=Sample[Sample$Licence %in% c(loc[i]),]
  a=b[1,]
  if ("gas" %in% b$Well.Type.Final && "oil" %in% b$Well.Type.Final && "water" %in% b$Well.Type.Final){
    a$Well.Type.Final = "oil"
  }
  else if("gas" %in% b$Well.Type.Final && "oil" %in% b$Well.Type.Final){
    a$Well.Type.Final = "oil"
  }
  else if("gas" %in% b$Well.Type.Final && "water" %in% b$Well.Type.Final){
    a$Well.Type.Final = "water"
  }
  else if("oil" %in% b$Well.Type.Final && "water" %in% b$Well.Type.Final){
    a$Well.Type.Final = "oil"
  }
  else if("gas" %in% b$Well.Type.Final){
    a$Well.Type.Final = "gas"
  }
  else if("oil" %in% b$Well.Type.Final){
    a$Well.Type.Final = "oil"
  }
  else if("water" %in% b$Well.Type.Final){
    a$Well.Type.Final = "water"
  }else{
    a$Well.Type.Final = "did not produce"
  }
  a$Spud.Date=min(b$Spud.Date,na.rm = TRUE)
  b$True.Vertical.Depth..m.TVD.[is.na(b$True.Vertical.Depth..m.TVD.)] <- 0
  a$True.Vertical.Depth..m.TVD. = max(b$True.Vertical.Depth..m.TVD.,na.rm = TRUE)
  a$Total.Well.Depth..mKB. = max(b$Total.Well.Depth..mKB.,na.rm = TRUE)
  a$Rig.Release.Date = min(b$Rig.Release.Date,na.rm = TRUE)
  lo=which(b$Rig.Release.Date==a$Rig.Release.Date)
  a$Drilling.Contractor.Name = b$Drilling.Contractor.Name[lo[1]]
  a$Completion.Length..m. = sum(b$Completion.Length..m.,na.rm=TRUE)
  a$Last.Production.Date = min(b$Last.Production.Date,na.rm = TRUE)
  a$Completion.Bottom..mKB. = max(b$Completion.Bottom..mKB.,na.rm = TRUE)
  a$Deepest.Form..Depth..mKB. = max(b$Deepest.Form..Depth..mKB., na.rm = TRUE)
  a$Cum..BOE..bbl. = sum(b$Cum..BOE..bbl.,na.rm=TRUE)
  lo=which(b$Completion.Bottom..mKB.==a$Completion.Bottom..mKB.)
  a$Geological.Formation = b$Geological.Formation[lo[1]]
  a$Producing.Formation = b$Producing.Formation[lo[1]]
  a$Cum..Gas.Injection...E3M3..e3m3. = sum(b$Cum..Gas.Injection...E3M3..e3m3.,na.rm=TRUE)
  a$Cum..Water.Production..m3. = sum(b$Cum..Water.Production..m3.,na.rm=TRUE)
  a$Cum..Oil.Production..m3. = sum(b$Cum..Oil.Production..m3.,na.rm=TRUE)
  a$Cum..Oil.Production..bbl. = sum(b$Cum..Oil.Production..bbl.,na.rm=TRUE)
  a$Final.Drill.Date = min(b$Final.Drill.Date,na.rm = TRUE)
  a$Initial.3Mth.Prod..Gas..Mcf.d. = sum(b$Initial.3Mth.Prod..Gas..Mcf.d.,na.rm=TRUE)
  a$Initial.3Mth.Prod..Oil..BBL.d. = sum(b$Initial.3Mth.Prod..Oil..BBL.d.,na.rm=TRUE)
  a$Initial.3Mth.Prod..Water..BBL.d. = sum(b$Initial.3Mth.Prod..Water..BBL.d.,na.rm=TRUE)
  a$Last.3Mth.Prod..Gas..Mcf.d. = sum(b$Last.3Mth.Prod..Gas..Mcf.d.,na.rm=TRUE)
  a$Last.3Mth.Prod..Oil..BBL.d. = sum(b$Last.3Mth.Prod..Oil..BBL.d.,na.rm=TRUE)
  a$Last.3Mth.Prod..Water..BBL.d. = sum(b$Last.3Mth.Prod..Water..BBL.d.,na.rm=TRUE)
  a$Initial.3Mth.Prod..BOE..BOE.d. = sum(b$Initial.3Mth.Prod..BOE..BOE.d.,na.rm=TRUE)
  a$Last.3Mth.Prod..BOE..BOE.d. = sum(b$Last.3Mth.Prod..BOE..BOE.d.,na.rm=TRUE)
  a$Last.3Mth.Prod..Condensate..BBL.d. = sum(b$Last.3Mth.Prod..Condensate..BBL.d.,na.rm=TRUE)
  a$Cum..Condensate.Production..bbl. = sum(b$Cum..Condensate.Production..bbl.,na.rm=TRUE)
  a$Cum..Condensate.Production..m3. = sum(b$Cum..Condensate.Production..m3.,na.rm=TRUE)
  a$Cum..Gas.Production..e3m3. = sum(b$Cum..Gas.Production..e3m3.,na.rm=TRUE)
  a$Cum..Gas.Production..mcf. = sum(b$Cum..Gas.Production..mcf.,na.rm=TRUE)
  a$Cum..Water.Injection...M3..m3. = sum(b$Cum..Water.Injection...M3..m3.,na.rm=TRUE)
  a$Initial.3Mth.Prod..Condensate..BBL.d. = sum(b$Initial.3Mth.Prod..Condensate..BBL.d.,na.rm=TRUE)
  a$Cum..CO2.Injection..mcf. = sum(b$Cum..CO2.Injection..mcf.,na.rm=TRUE)
  a$Cum..Water.Injection...BBL..bbl. = sum(b$Cum..Water.Injection...BBL..bbl.,na.rm=TRUE)
  a$Perforation.Count = sum(b$Perforation.Count,na.rm=TRUE)
  a$Cumulative.Marketable.Production..e3m3. = max(b$Cumulative.Marketable.Production..e3m3.,na.rm=TRUE)
  a$Cumulative.Oil.Production..e3m3. = max(b$Cumulative.Oil.Production..e3m3.,na.rm=TRUE)
  
  repuwi=rbind(repuwi,a)
}

#combine nonrep and repuwi to Sample
Sample=rbind(repuwi, nonrep)
Sample[Sample==-Inf] <- NA
Sample[Sample==Inf] <- NA

#replace NA with 0 for part of attributes
prod=c("Cum..Water.Production..bbl.","Cum..Oil.Production..bbl.",
       "Cum..Oil.Production..m3.","Cum..Water.Production..m3.",
       "Initial.3Mth.Prod..Gas..Mcf.d.","Initial.3Mth.Prod..Oil..BBL.d.",
       "Initial.3Mth.Prod..Water..BBL.d.","Last.3Mth.Prod..Gas..Mcf.d.",
       "Last.3Mth.Prod..Oil..BBL.d.","Last.3Mth.Prod..Water..BBL.d.",
       "Initial.3Mth.Prod..BOE..BOE.d.",
       "Last.3Mth.Prod..BOE..BOE.d.","Last.3Mth.Prod..Condensate..BBL.d.",
       "Cum..Condensate.Production..bbl.","Cum..Condensate.Production..m3.",
       "Cum..Gas.Production..e3m3.","Cum..Gas.Production..mcf.",
       "Initial.3Mth.Prod..Condensate..BBL.d.",
       "Cumulative.Marketable.Production..e3m3.","Cumulative.Oil.Production..e3m3.",
       "Initial.Producible.Gas.Cumulative.Raw.Production..e3m3.")
for (i in 1:length(prod)){
  Sample[prod[i]][is.na(Sample[prod[i]])] <- 0
}

prod=c("Cum..BOE..bbl." ,"Cum..Gas.Injection...E3M3..e3m3.",
       "Cum..Water.Injection...M3..m3.","Cum..Co2.Injection..e3m3.",
       "Cum..CO2.Injection..mcf.","Cum..Water.Injection...BBL..bbl.",
       "Initial.Est.Mkt.Gas.Reserves.Associated.And.Nonassociated..e3m3.",
       "Initial.Established.Marketable.Gas.Reserves.Solution..e3m3.",
       "Initial.Established.Marketable.Gas.Reserves.Total..e3m3.",
       "Initial.Gas.In.Place.Associated.And.Non.Associated.Gas..e3m3.",
       "Initial.Gas.In.Place.Solution.Gas..e3m3.",
       "Initial.Gas.In.Place.Total.Gas..e3m3.",
       "Initial.Producible.Gas.Associated.And.Non.Associated.Gas..e3m3.",
       "Initial.Producible.Gas.Solution.Gas..e3m3.",
       "Initial.Producible.Gas.Total.Gas..e3m3.",
       "Remaining.Energy.Content..TJ.",
       "Remaining.Established.Marketable.Gas..e3m3.",
       "Initial.Establish.Reserves.Oil.Enhanced..e3m3.",
       "Initial.Establish.Reserves.Oil.Primary..e3m3.",
       "Initial.Establish.Reserves.Oil.Total..e3m3.",
       "Oil.In.Place..e3m3.",
       "Remaining.Established.Oil.Reserves..e3m3." )
for (i in 1:length(prod)){
  Sample[prod[i]][is.na(Sample[prod[i]])] <- 0
}



###
lics <- Sample$Licence
lic <- Data$Licence
uwis <- Sample$UWI
uwi <- Data$UWI


######################
Sample['PFW'] <- NA
for (i in 1:length(lics)){
  if (lics[i] %in% lic){
    loc <- which(sapply(lic,function(x) lics[i] %in% x))
    pf=Data[loc,"PFW"]
    if("Fail" %in% pf){
      Sample[i,"PFW"] <- "Fail"
    }
    else if("Pass" %in% pf){
      Sample[i,"PFW"] <- "Pass"
    }
  }
}





####################
cate<-Sample[,c("Deepest.Form..Name","Full.Status","LLR.Abandonment.Area",
                "Lahee.Classification","Well.Trajectory","Projected.Formation",
                "Recovery.Mechanism","Geological.Formation","Producing.Formation",
                "Field","PSAC.Area.Code","Drilling.Contractor.Name",
                "Scheme.Sub.Type","EDCT","Type","Substance.Released.1",
                "Well.Type.Final")]

for (i in 1:length(cate)) {
  cate[,i]=as.factor(cate[,i]);
}

date <- Sample[,c("Spud.Date","Last.Production.Date","Surface.Aband..Date","Final.Drill.Date","Rig.Release.Date","Last.Injection.Date")] 
date <- mutate_all(date, function(x) as.numeric(as.character(x)))

nume <- Sample[,c("Deepest.Form..Depth..mKB.","Total.Well.Depth..mKB.","True.Vertical.Depth..m.TVD.","Completion.Length..m.","Base.Of.Groundwater.Protection.Elev...m.above.sea.level.",
                  "Base.Of.Groundwater.Protection.Depth..mKB.","Completion.Bottom..mKB.","Completion.Top..mKB.","Cum..BOE..bbl.","Cum..Gas.Production..e3m3.",
                  "Cum..Oil.Production..bbl.","Cum..Water.Production..m3.","Cum..Oil.Production..m3.","Ground.Elevation..m.above.sea.level.",
                  "Initial.3Mth.Prod..Gas..Mcf.d.","Initial.3Mth.Prod..Oil..BBL.d.","Initial.3Mth.Prod..Water..BBL.d.","Last.3Mth.Prod..Gas..Mcf.d.","Last.3Mth.Prod..Oil..BBL.d.",
                  "Last.3Mth.Prod..Water..BBL.d.","Initial.3Mth.Prod..BOE..BOE.d.","Last.3Mth.Prod..BOE..BOE.d.","Last.3Mth.Prod..Condensate..BBL.d.","Perforation.Count",
                  "Oil.Pool..Area.Pool.Ha..ha.","Oil.Pool.Average.Pay.Thickness..m.","Oil.Pool.Initial.Pressure..kpa.","Oil.Pool.Temperature..ºC.",
                  "Cum..Condensate.Production..bbl.","Cum..Condensate.Production..m3.","Cum..Water.Injection...M3..m3.","Cum..CO2.Injection..mcf.","Cum..Gas.Production..mcf.",
                  "Cum..Water.Injection...BBL..bbl.","Cum..Co2.Injection..e3m3.","Initial.3Mth.Prod..Condensate..BBL.d.","Outside.Diameter..mm.","Volume.Recovered.1",
                  "Volume.Released.1","Cement.Amount")]

nume$True.Vertical.Depth..m.TVD.[is.na(nume$True.Vertical.Depth..m.TVD.)] <- 0
for (i in 1:dim(nume)[1]) {
  if (nume$True.Vertical.Depth..m.TVD.[i]==0){
    nume$True.Vertical.Depth..m.TVD.[i]=nume$Total.Well.Depth..mKB.[i]
  }
}
nume$Total.Well.Depth..mKB.=c()
nume.gasfield <- Sample[,c("Cumulative.Marketable.Production..e3m3.","Initial.Est.Mkt.Gas.Reserves.Associated.And.Nonassociated..e3m3.","Initial.Established.Marketable.Gas.Reserves.Solution..e3m3.","Initial.Established.Marketable.Gas.Reserves.Total..e3m3.",
                           "Initial.Gas.In.Place.Associated.And.Non.Associated.Gas..e3m3.","Initial.Gas.In.Place.Solution.Gas..e3m3.","Initial.Gas.In.Place.Total.Gas..e3m3.","Initial.Producible.Gas.Associated.And.Non.Associated.Gas..e3m3.",
                           "Initial.Producible.Gas.Cumulative.Raw.Production..e3m3.","Initial.Producible.Gas.Solution.Gas..e3m3.","Initial.Producible.Gas.Total.Gas..e3m3.","Remaining.Energy.Content..TJ.","Remaining.Established.Marketable.Gas..e3m3.")]

nume.oil <- Sample[,c("Cumulative.Oil.Production..e3m3.","Initial.Establish.Reserves.Oil.Enhanced..e3m3.","Initial.Establish.Reserves.Oil.Primary..e3m3.","Initial.Establish.Reserves.Oil.Total..e3m3.","Oil.In.Place..e3m3.","Remaining.Established.Oil.Reserves..e3m3.")]



nume <- cbind(date,nume,nume.gasfield,nume.oil)

pfw <- Sample$PFW
pfw[pfw=="Fail"] <- 0
pfw[pfw=="Pass"] <- 1
pfw <- as.numeric(pfw)

#######
y=pfw

######################## variable selection
# var_nume=c("Spud.Date","Remaining.Energy.Content..TJ.","Last.Production.Date", "Cum..Oil.Production..m3.",
#            "Cum..Water.Production..m3.","Initial.3Mth.Prod..Oil..BBL.d.","Last.3Mth.Prod..Oil..BBL.d.",
#            "Last.3Mth.Prod..Water..BBL.d.","Initial.3Mth.Prod..BOE..BOE.d.",
#            "Outside.Diameter..mm.","Volume.Recovered.1","Cumulative.Marketable.Production..e3m3.",
#            "Oil.In.Place..e3m3.")

lognume<-nume[,var_nume]

lognume_r <- dim(lognume)[1]

data_full <- rbind(lognume,lognume_test)
data_full_pfw <- na.omit(data_full)
data_full <- as.data.frame(scale(data_full,center = T, scale = T))

lognume <- data_full[1:lognume_r,]
lognume_test1 <- data_full[(lognume_r+1):dim(data_full)[1],]
logdata_test1 <- as.data.frame(cbind(lognume_test1))
logdata_test1 <- as.data.frame(cbind(lognume_test1,logcate_test))

logcate<-cate[,c("Well.Type.Final")] # add "Well.Type.Final" when obtain the data
logcate <- as.data.frame(logcate)
colnames(logcate) <- c("Well.Type.Final")# add "Well.Type.Final" when obtain the data

logdata<-as.data.frame(cbind(lognume,logcate,y))

logdata <- na.omit(logdata)
logdata$y=as.factor(logdata$y)

### Over sampling
table(logdata$y)
set.seed(2023)
# Over-sampling (no need to run this chunk if logdata's "pass/fail" ratio is around 1:1)
# If there are fewer fail cases (we select 1.25 as the threshold ratio)
if (length(which(logdata$y==1))>1.25*length(which(logdata$y==0))){
  ind_fail<-which(logdata$y==0)
  ind_sample<-sample(ind_fail, size = (length(which(logdata$y==1))-length(which(logdata$y==0))),replace=T)
  logdata<-rbind(logdata,logdata[ind_sample,])
}

# If there are fewer pass cases (we select 1.25 as the threshold ratio)
if (1.25*length(which(logdata$y==1))<length(which(logdata$y==0))){
  ind_pass<-which(logdata$y==1)
  ind_sample<-sample(ind_pass, size = (length(which(logdata$y==0))-length(which(logdata$y==1))),replace=T)
  logdata<-rbind(logdata,logdata[ind_sample,])
}








###### random forest
library(randomForest)

# Run line 775 to get the fitted model based on all the sample points
rf.well<-randomForest(y~., data=logdata, mtry=3, ntree=800, nodesize=1,importance=TRUE) # run this line and turn to Mike or Ryan's file
pred.rf<-predict(rf.well, newdata=logdata)
# pred.rf<-predict(rf.well, newdata=logdata, type="prob")
tab=table(pred.rf,logdata$y)

#randomForest:::varImpPlot(rf.well)


### well center reuslts prediction
logdata_test11<-mutate(logdata_test1, y=as.factor(rep(0,dim(logdata_test1)[1])))
logdata_test11<-rbind(logdata[1,],logdata_test11)
logdata_test11<-logdata_test11[-1,]
pfw_prob <- predict(rf.well, newdata=logdata_test11, type="prob")
#pfw_prob <- predict(rf.well, newdata=logdata_test11)



###### No need to run the following chunk if accuracy is not needed
Accuracy_R<-function(ind){
  Train=logdata[-ind,]
  Test=logdata[ind,]
  rf.well<-randomForest(y~., data=Train, mtry=3, ntree=800, importance=TRUE)
  pred.rf<-predict(rf.well, newdata=Test)
  tab=table(pred.rf,Test$y)
  return((tab[1,1]+tab[2,2])/sum(tab))
}

a=matrix(c(0,0,0,0),nrow = 2)
for (i in 1:50) {
  set.seed(i)
  folds<-createFolds(factor(logdata$y), 4)
  temp=lapply(folds, Accuracy_R)
  ind=folds[[which.max(temp)]]
  Train<-logdata[-ind,]
  Test<-logdata[ind,]
  rf.well<-randomForest(y~., data=Train, mtry=3, ntree=800, importance=TRUE)
  pred.rf<-predict(rf.well, newdata=Test)
  tab=table(pred.rf,Test$y)
  a=a+as.matrix(prop.table(tab))
}

accuracy_rf=(a[1,1]+a[2,2])/50
accuracy_rf



# Final Output 
Pred_result<-cbind(testdata$Licence,testdata$UWI,pfw_prob[,2])
colnames(Pred_result)=c("Licence","UWI","Well.Center.Pass.Probability")


# pfw_prob <- as.data.frame(pfw_prob)
# Pred_result<-cbind(testdata$Licence,testdata$UWI,pfw_prob)
# colnames(Pred_result)=c("Licence","UWI","Phase2.Well.Center.Results")


View(Pred_result)

Pred_result_SK_ph2W_P<- Pred_result

Pred_result_SK_ph2W_P <- as.data.frame(Pred_result)

Pred_result_SK_ph2W1.1 <- as.data.frame(Pred_result_SK_ph2W_P)
for(i in 1:dim(Pred_result_SK_ph2W_P)[1]){
  if(Pred_result_SK_ph2W_P$Well.Center.Pass.Probability[i] >= 0.90){
    Pred_result_SK_ph2W1.1$Well.Center.Pass.Probability[i] <- "Pass"
  }
  else{
    Pred_result_SK_ph2W1.1$Well.Center.Pass.Probability[i] <- "Fail"
  }
}
View(Pred_result_SK_ph2W1.1)



# #Combine results from different province
# 
# 
# fullresults=read.csv("Phase 2 Well Centre Results Compare.csv",as.is=TRUE, header=TRUE)
# 
# loc <- which(grepl("E+",fullresults$Licence.Number))
# 
# for(i in 1:dim(fullresults)[1]){
#   if(!(grepl("[A-Za-z]", fullresults$Licence.Number[i]))){
#     fullresults$Licence.Number[i]=as.numeric(fullresults$Licence.Number[i])
#   }
# }
# 
# 
# for(i in 1:dim(AB_pred_result1.1)[1]){
#   loc = which(AB_pred_result1.1$Licence[i]==fullresults$License)
#   fullresults$Amber.s.predicted.results[loc]=AB_pred_result1.1$Well.Center.Pass.Probability[i]
# }
# 
# for(i in 1:dim(Pred_result_SK_ph2W1.1)[1]){
#   loc = which(Pred_result_SK_ph2W1.1$Licence[i]==fullresults$Licence)
#   fullresults$Amber.s.predicted.results[loc]=as.numeric(Pred_result_SK_ph2W1.1$Phase2.Well.Center.Results[i])-1
# }
# 
# for(i in 1:dim(Pred_result_SK_ph2W1.1)[1]){
#   loc = which(Pred_result_SK_ph2W1.1$Licence[i]==fullresults$Licence)
#   fullresults$Amber.s.predicted.results[loc]=Pred_result_SK_ph2W1.1$Well.Center.Pass.Probability[i]
# }
