#Phase 1 Results, Phase 2 Results and Remediated Volume prediction
#Author:Amber Huang(qian.huang@ucalgary.ca/qhuang@360eec.com)

################################## Test Data Loading #################################
library(reticulate)
library(dplyr)
library(readxl)
library(writexl)
library(stringi)

current_directory <- getwd()
python_folder_path <- file.path(current_directory,'routes','Model','Python file for Cartofact data extracting')
python_code_normalized_path <- normalizePath(python_folder_path)
setwd(python_code_normalized_path)
#print(python_code_normalized_path)
#setwd("/Users/anusa/Desktop/api/routes/Model/Python file for Cartofact data extracting")




###Obtain attributes from CARTOFACT.com using Python
python_path <- file.path(current_directory,'routes','Model','Python', 'Python312')
use_python(python_path)


#Select the attributes
attribute <- c('licence','uwi_formatted',
               'spud_date','cumulative_oil_production_m3',
               'cumulative_gas_production_e3m3','cumulative_water_production_m3',
               'cumulative_condensate_production_bbl','completion_interval_bottom',
               'prod_ip3_oil_bbld','prod_ip3_gas_mcfd',
               'ground_elevation','status_full',
               'llr_abandonment_area_name','surf_aband_date',
               'tv_depth','well_total_depth',
               'field_name','last_production_date',
               'prod_ip3_boe_boed','prod_mr3_wtr_bbld',
               'prod_mr3_oil_bbld')
table <- c('live_well_ab')
at_table <- as.data.frame(cbind(attribute,table))
attable_path <- file.path(current_directory,'routes','Model','Python file for Cartofact data extracting', "at_table.xlsx")
attable_file_normalized_path <- normalizePath(attable_path)
write_xlsx(at_table, attable_file_normalized_path)



#Obtain the attributes
library(reticulate)



# Specify the path to your Python file with spaces, escaping the spaces with a backslash
python_extracting_cartofact_path <- file.path(python_folder_path,'extracting cartofact attributes.py')
extracting_cartofact_normalized_path <- normalizePath(python_extracting_cartofact_path)
py_run_file(extracting_cartofact_normalized_path)


attribute_df_path <- file.path(python_folder_path, 'attribute_df.csv')
attribute_df_normalized_path <- normalizePath(attribute_df_path)
testdata <- read.csv(attribute_df_normalized_path)

# obtain attribute 'oil_in_place'
search_id <- c('field_name')
id <- unique(testdata$field_name)
id <- data.frame(id = id)

id_file_path <- file.path(python_folder_path,'id.xlsx')
id_file_normalized_path <- normalizePath(id_file_path)
write_xlsx(id,id_file_normalized_path)

attribute <- c('field_name','oil_in_place_e3m3')
table <- c('live_well_ab_st98_field_oil_reserve')
at_table <- as.data.frame(cbind(attribute,table,search_id))


write_xlsx(at_table,attable_file_normalized_path)
python_extracting_other_table_path <- file.path(python_folder_path,'extracting attributes other table.py')
python_extracting_other_table_normalized_path <- normalizePath(python_extracting_other_table_path)
py_run_file(python_extracting_other_table_normalized_path)
attribute_ot_path <- file.path(python_folder_path,'attribute_ot.csv')
attribute_ot_normalized_path <- normalizePath(attribute_ot_path)
oilinplace <- read.csv(attribute_ot_normalized_path)
testdata <- merge(testdata,oilinplace,by = 'field_name',all.x = TRUE)


#Table "well_ab_casing" ('Casing' only taken value of "SURFACE")
search_id <- c('uwi_formatted')
id <- unique(testdata$uwi_formatted)
id <- data.frame(id = id)
write_xlsx(id,id_file_normalized_path)

attribute <- c('uwi_formatted','casing_size','casing_code')
table <- c('live_well_ab_casing')
at_table <- as.data.frame(cbind(attribute,table,search_id))
write_xlsx(at_table,attable_file_normalized_path)
py_run_file(python_extracting_other_table_normalized_path)
casing <- read.csv(attribute_ot_normalized_path)
casing <- subset(casing, casing_code == 'SURFACE')
testdata <- merge(testdata, casing, by = 'uwi_formatted', all.x = TRUE)



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
colnames(testdata)[colnames(testdata) == "llr_abandonment_area_name"] <- "LLR.Abandonment.Area"
colnames(testdata)[colnames(testdata) == "surf_aband_date"] <- "Surface.Aband..Date"
colnames(testdata)[colnames(testdata) == "tv_depth"] <- "True.Vertical.Depth..m.TVD."
colnames(testdata)[colnames(testdata) == "well_total_depth"] <- "Total.Well.Depth..mKB."
colnames(testdata)[colnames(testdata) == "oil_in_place_e3m3"] <- "Oil.In.Place..e3m3."
colnames(testdata)[colnames(testdata) == "last_production_date"] <- "Last.Production.Date"
colnames(testdata)[colnames(testdata) == "prod_ip3_boe_boed"] <- "Initial.3Mth.Prod..BOE..BOE.d."
colnames(testdata)[colnames(testdata) == "prod_mr3_wtr_bbld"] <- "Last.3Mth.Prod..Water..BBL.d."
colnames(testdata)[colnames(testdata) == "prod_mr3_oil_bbld"] <- "Last.3Mth.Prod..Oil..BBL.d."
colnames(testdata)[colnames(testdata) == "casing_code"] <- "Casing"
colnames(testdata)[colnames(testdata) == "casing_size"] <- "Outside.Diameter..mm."


lics <- testdata$Licence
uwis <- testdata$UWI

# Add "Well.Type.Final" attributes
testdata['Well.Type.Final'] <- NA
for (i in 1:dim(testdata)[1]){
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
  a$Completion.Bottom..mKB. = max(b$Completion.Bottom..mKB.,na.rm = TRUE)
  a$Cum..Water.Production..m3. = sum(b$Cum..Water.Production..m3.,na.rm=TRUE)
  a$Cum..Oil.Production..m3. = sum(b$Cum..Oil.Production..m3.,na.rm=TRUE)
  a$Cum..Gas.Production..e3m3. = sum(b$Cum..Gas.Production..e3m3.,na.rm=TRUE)
  a$Cum..Condensate.Production..bbl. = sum(b$Cum..Condensate.Production..bbl.,na.rm=TRUE)
  a$Initial.3Mth.Prod..Gas..Mcf.d. = sum(b$Initial.3Mth.Prod..Gas..Mcf.d.,na.rm=TRUE)
  a$Initial.3Mth.Prod..Oil..BBL.d. = sum(b$Initial.3Mth.Prod..Oil..BBL.d.,na.rm=TRUE)
  a$Ground.Elevation..m.above.sea.level. = max(b$Ground.Elevation..m.above.sea.level.,na.rm=TRUE)
  a$Initial.3Mth.Prod..BOE..BOE.d. = sum(b$Initial.3Mth.Prod..BOE..BOE.d.,na.rm=TRUE)
  a$Last.3Mth.Prod..Oil..BBL.d. = sum(b$Last.3Mth.Prod..Oil..BBL.d.,na.rm=TRUE)
  a$Last.3Mth.Prod..Water..BBL.d. = sum(b$Last.3Mth.Prod..Water..BBL.d.,na.rm=TRUE)
  
  repuwi=rbind(repuwi,a)
}

#combine nonrep and repuwi to Sample
testdata=rbind(repuwi, nonrep)
testdata[testdata==-Inf] <- NA
testdata[testdata==Inf] <- NA

#replace NA with 0 for part of attributes
prod=c("Cum..Oil.Production..m3.","Cum..Water.Production..m3.",
       "Cum..Gas.Production..e3m3.","Cum..Condensate.Production..bbl.",
       "Initial.3Mth.Prod..Gas..Mcf.d.","Initial.3Mth.Prod..Oil..BBL.d.",
       "Initial.3Mth.Prod..BOE..BOE.d.","Last.3Mth.Prod..Oil..BBL.d.",
       "Last.3Mth.Prod..Water..BBL.d.")#If any attributes not applicable in CARTOFACT for the data, delete it here.
for (i in 1:length(prod)){
  testdata[prod[i]][is.na(testdata[prod[i]])] <- 0
}


lics <- testdata$Licence
uwis <- testdata$UWI


################ select variables for the model
cate<-testdata[,c("LLR.Abandonment.Area","Well.Type.Final")]

for (i in 1:length(cate)) {
  cate[,i]=as.factor(cate[,i]);
}

nume <- testdata[,c("Spud.Date","Last.Production.Date",
                    "Cum..Gas.Production..e3m3.","Cum..Water.Production..m3.",
                    "Cum..Oil.Production..m3.","Cum..Condensate.Production..bbl.",
                    "Completion.Bottom..mKB.","True.Vertical.Depth..m.TVD.",
                    "Total.Well.Depth..mKB.",
                    "Ground.Elevation..m.above.sea.level.",
                    "Initial.3Mth.Prod..Gas..Mcf.d.","Initial.3Mth.Prod..Oil..BBL.d.",
                    "Ground.Elevation..m.above.sea.level.","Oil.In.Place..e3m3.",
                    "Initial.3Mth.Prod..BOE..BOE.d.","Last.3Mth.Prod..Water..BBL.d.",
                    "Last.3Mth.Prod..Oil..BBL.d.")]

# combine True.Vertical.Depth and Total.Well.Depth
nume$True.Vertical.Depth..m.TVD.[is.na(nume$True.Vertical.Depth..m.TVD.)] <- 0
for (i in 1:dim(nume)[1]) {
  if (nume$True.Vertical.Depth..m.TVD.[i]==0){
    nume$True.Vertical.Depth..m.TVD.[i]=nume$Total.Well.Depth..mKB.[i]
  }
}
nume$Total.Well.Depth..mKB.=c()



#Full version of attributes for phase1 and phase2
# var_nume=c("Spud.Date","Cum..Gas.Production..e3m3.","Completion.Bottom..mKB.",
#            "Initial.3Mth.Prod..Gas..Mcf.d.","Cum..Oil.Production..m3.",
#            "True.Vertical.Depth..m.TVD.","Initial.3Mth.Prod..Oil..BBL.d.",
#            "Surface.Aband..Date","Ground.Elevation..m.above.sea.level.",
#            "Cum..Water.Production..m3.","Cum..Condensate.Production..bbl.",
#            "Outside.Diameter..mm.")# select all the numerical attributes will be used in model

var_nume=c("Spud.Date","Cum..Gas.Production..e3m3.",
           "Initial.3Mth.Prod..Gas..Mcf.d.","Cum..Oil.Production..m3.",
           "True.Vertical.Depth..m.TVD.","Initial.3Mth.Prod..Oil..BBL.d.",
           "Ground.Elevation..m.above.sea.level.",
           "Cum..Water.Production..m3.","Cum..Condensate.Production..bbl.")# select all the numerical attributes will be used in model

# #Full version of attributes for remediation volume
# var_nume=c("Spud.Date","Last.Production.Date",
#            "Cum..Oil.Production..m3.","Cum..Water.Production..m3.",
#            "Initial.3Mth.Prod..Oil..BBL.d.","Last.3Mth.Prod..Oil..BBL.d.",
#            "Last.3Mth.Prod..Water..BBL.d.","Initial.3Mth.Prod..BOE..BOE.d.",
#            "Outside.Diameter..mm.","Volume.Recovered.1",
#            "Oil.In.Place..e3m3.")

#For remediation volume
var_nume=c("Spud.Date",
           "Cum..Oil.Production..m3.","Cum..Water.Production..m3.",
           "Initial.3Mth.Prod..Oil..BBL.d.", "Last.3Mth.Prod..Oil..BBL.d.",
           "Last.3Mth.Prod..Water..BBL.d.","Initial.3Mth.Prod..BOE..BOE.d.",
           "Oil.In.Place..e3m3.")

lognume_test<-nume[,var_nume]




logcate_test <- cate[,c("LLR.Abandonment.Area","Well.Type.Final")]
logcate_test <- as.data.frame(logcate_test)
colnames(logcate_test) <- c("LLR.Abandonment.Area","Well.Type.Final")


logcate_test2 <- cate[,c("LLR.Abandonment.Area","Well.Type.Final")] 
logcate_test2 <- as.data.frame(logcate_test2)
colnames(logcate_test2) <- c("LLR.Abandonment.Area","Well.Type.Final")

# logcate_test <- cate[,c("LLR.Abandonment.Area")]
# logcate_test <- as.data.frame(logcate_test)
# colnames(logcate_test) <- c("LLR.Abandonment.Area")
# 
# 
# logcate_test2 <- cate[,c("LLR.Abandonment.Area")] 
# logcate_test2 <- as.data.frame(logcate_test2)
# colnames(logcate_test2) <- c("LLR.Abandonment.Area")










































###################################Phase1 Prediction###########################
#For this part run directly to line 598, then read the commment
#load data
excel_folder_path <- file.path(current_directory,'routes','Model','excel data files for R code models')
excel_folder_normalized_path <- normalizePath(excel_folder_path)
setwd(excel_folder_normalized_path)
Data=read.csv("Environmental Data Collection V1_Jul6_clean.csv", as.is=TRUE, header=TRUE)
Data <- as.data.frame(Data)
Sample=read.csv("Phase1 Attributes_MAIN.csv",as.is=TRUE, header=TRUE)
Sample0=read.csv("Phase1 Attributes_MAIN.csv",as.is=TRUE, header=TRUE)
Sample <- as.data.frame(Sample)


# Add "Well.Type.Final" attributes
Sample['Well.Type.Final'] <- NA
for (i in 1:dim(Sample)[1]){
  if (grepl("Oil", Sample$Full.Status[i])){
    Sample$Well.Type.Final[i]="oil"
  }
  else if (grepl("Water", Sample$Full.Status[i])){
    Sample$Well.Type.Final[i]="water"
  }
  else if (grepl("Gas", Sample$Full.Status[i])){
    Sample$Well.Type.Final[i]="gas"
  }
  else if ((Sample$Cum..Gas.Production..e3m3.[i]==0 || is.na(Sample$Cum..Gas.Production..e3m3.[i])) &
           (Sample$Cum..Water.Production..m3.[i]==0 || is.na(Sample$Cum..Water.Production..m3.[i])) &
           (Sample$Cum..Oil.Production..m3.[i]==0 || is.na(Sample$Cum..Oil.Production..m3.[i]))){
    Sample$Well.Type.Final[i]="did not produce"
  }
  else if(Sample$Cum..Oil.Production..m3.[i]!=0 & !(is.na(Sample$Cum..Oil.Production..m3.[i]))){
    Sample$Well.Type.Final[i]="oil"
  }
  else if((Sample$Cum..Oil.Production..m3.[i]==0 || is.na(Sample$Cum..Oil.Production..m3.[i])) &
          (Sample$Cum..Water.Production..m3.[i]!=0 & !(is.na(Sample$Cum..Water.Production..m3.[i])))){
    Sample$Well.Type.Final[i]="water"
  }
  else if((Sample$Cum..Oil.Production..m3.[i]==0 || is.na(Sample$Cum..Oil.Production..m3.[i])) &
          (Sample$Cum..Water.Production..m3.[i]==0 || is.na(Sample$Cum..Water.Production..m3.[i])) &
          (Sample$Cum..Gas.Production..e3m3.[i]!=0 & !(is.na(Sample$Cum..Gas.Production..e3m3.[i])))){
    Sample$Well.Type.Final[i]="gas"
  }
}



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
Data <- Data %>% distinct(UWI, Phase.1.ESA.Result, .keep_all = TRUE)
dup <- subset(Data,duplicated(UWI))

for (i in 1:dim(dup)[1]){
  loc=which(Data$UWI == dup$UWI[i])
  loc=loc[-which.max(Data$Source.Dates[loc])]
  Data=Data[-loc,]
}
dup <- subset(Data,duplicated(UWI))

#Set phase1 as "Fail" when phase1 is blank but have a phase2 results
Data$Phase.2.ESA.Result[Data$Phase.2.ESA.Result == "-"] <- ""
Data$Phase.1.ESA.Result[Data$Phase.1.ESA.Result == "-"] <- ""
for (i in 1:dim(Data)[1]){
  if (Data$Phase.1.ESA.Result[i]=="" & Data$Phase.2.ESA.Result[i]!=""){
    Data$Phase.1.ESA.Result[i]="Fail"
  }
}

Data<-Data[Data$Phase.1.ESA.Result!="",]

#write.csv(Data, "Data1.csv", row.names=FALSE)

############### Add licence to Data
Data$UWI <- stri_replace_all_fixed(Data$UWI, "​", "")
Data<-data.frame(Licence=Data$Licence.Number, UWI=Data$UWI, PF1=Data$Phase.1.ESA.Result,
                 date=Data$Source.Dates)
# Data<-data.frame(Licence=Data$Licence.Number, UWI=Data$UWI, PF=Data$Phase.2.ESA.Result, date=Data$Source.Dates)
# Data<-Data[Data$Phase.2.ESA.Result!="",]
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
gas=read.csv("Phase1 Attributes_GASFIELD.csv",as.is=TRUE, header=TRUE)
gas <- as.data.frame(gas)

common_col <- intersect(names(Sample),names(gas))
unique_col <- setdiff(names(gas),common_col)
Sample <- merge(Sample, gas[, c('UWI', unique_col)], by = 'UWI', all.x = TRUE)



#sheet CASING('Casing' only taken value of "SURFACE")
casing=read.csv("Phase1 Attributes_CASING.csv",as.is=TRUE, header=TRUE)
casing <- as.data.frame(casing)
casing <- casing[casing$Casing == 'SURFACE',]

common_col <- intersect(names(Sample),names(casing))
unique_col <- setdiff(names(casing),common_col)
Sample <- merge(Sample, casing[, c('UWI', unique_col)], by = 'UWI', all.x = TRUE)



#sheet CEMENTING(only 750 unique UWI, duplicate UWIs)
cement=read.csv("Phase1 Attributes_CEMENTING.csv",as.is=TRUE, header=TRUE)
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
incident=read.csv("Phase1 Attributes_INCIDENTS.csv",as.is=TRUE, header=TRUE)
incident <- as.data.frame(incident)

common_col <- intersect(names(Sample),names(incident))
unique_col <- setdiff(names(incident),common_col)
Sample <- merge(Sample, incident[, c('UWI', unique_col)], by = 'UWI', all.x = TRUE)

Sample$Volume.Recovered.1[is.na(Sample$Volume.Recovered.1)] <- 0
Sample$Volume.Released.1[is.na(Sample$Volume.Released.1)] <- 0



#sheet OIL FIELD
oil=read.csv("Phase1 Attributes_OILFIELD.csv")
oil <- as.data.frame(oil)
oil <- oil[,c("UWI","Cumulative.Oil.Production..e3m3.","Initial.Establish.Reserves.Oil.Enhanced..e3m3.","Initial.Establish.Reserves.Oil.Primary..e3m3.","Initial.Establish.Reserves.Oil.Total..e3m3.","Oil.In.Place..e3m3.","Remaining.Established.Oil.Reserves..e3m3.")]

common_col <- intersect(names(Sample),names(oil))
unique_col <- setdiff(names(oil),common_col)
Sample <- merge(Sample, oil[, c('UWI', unique_col)], by = 'UWI', all.x = TRUE)



#############################
#Remove rows of well with UWIs >= 2, nonrep
licence <- unique(Sample$Licence)
tab <- table(Sample$Licence)
tabname <- names(tab)
loc <- which(tab[as.character(licence)]>1)
loc <- names(loc)
nonrep <- Sample[!Sample$Licence %in% loc,]

#Build sub data frame of well with UWIs >= 2, repuwi
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
#repuwi=repuwi[,-89]#after add-in well type, delete this row
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


### add phase1 results to the dataframe Sample
Sample['PF1'] <- NA
for (i in 1:length(lics)){
  if (lics[i] %in% lic){
    loc <- which(sapply(lic,function(x) lics[i] %in% x))
    pf=Data[loc,"PF1"]
    if("Fail" %in% pf){
      Sample[i,"PF1"] <- "Fail"
    }
    else{
      Sample[i,"PF1"] <- "Pass"
    }
  }
}

################ select variables for the model
cate<-Sample[,c("Deepest.Form..Name","Full.Status","LLR.Abandonment.Area",
                "Lahee.Classification","Well.Trajectory","Projected.Formation",
                "Recovery.Mechanism","Geological.Formation","Producing.Formation",
                "Field","PSAC.Area.Code","Drilling.Contractor.Name",
                "Scheme.Sub.Type","EDCT","Type","Substance.Released.1","Well.Type.Final")]# add "Well.Type.Final" when obtain the data

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

# combine True.Vertical.Depth and Total.Well.Depth
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

ph1 <- Sample$PF1
ph1[ph1=="Fail"] <- 0
ph1[ph1=="Pass"] <- 1
ph1 <- as.numeric(ph1)

y=ph1


# var_nume=c("Spud.Date","Cum..Gas.Production..e3m3.","Completion.Bottom..mKB.",
#            "Initial.3Mth.Prod..Gas..Mcf.d.","Cum..Oil.Production..m3.",
#            "True.Vertical.Depth..m.TVD.","Initial.3Mth.Prod..Oil..BBL.d.",
#            "Surface.Aband..Date",
#            "Ground.Elevation..m.above.sea.level.","Cum..Water.Production..m3.",
#            "Cum..Condensate.Production..bbl.","Outside.Diameter..mm.")

lognume<-nume[,var_nume]

lognume_r <- dim(lognume)[1]

data_full <- rbind(lognume,lognume_test)
data_full_ph1 <- na.omit(data_full)
data_full <- as.data.frame(scale(data_full,center = T, scale = T))

lognume <- data_full[1:lognume_r,]
lognume_test1 <- data_full[(lognume_r+1):dim(data_full)[1],]
logdata_test1 <- as.data.frame(cbind(lognume_test1,logcate_test))


logcate<-cate[,c("LLR.Abandonment.Area","Well.Type.Final")] # add "Well.Type.Final" when obtain the data
logcate <- as.data.frame(logcate)
colnames(logcate) <- c("LLR.Abandonment.Area","Well.Type.Final")# add "Well.Type.Final" when obtain the data


logdata<-as.data.frame(cbind(lognume,y))
logdata<-as.data.frame(cbind(lognume,logcate,y))

logdata <- na.omit(logdata)
logdata$y=as.factor(logdata$y)


table(logdata$y)#Run this line and pause, used the output(0 is fail, 1 is pass) of this line to decide which of the following part to run
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



###### Please Note if Accuracy isn't Needed, do not run the following line. Directly go to "random forest"
logdata_logistic<-subset(logdata,(LLR.Abandonment.Area!="High Level"))#remove subset with fewer data points,check the well type subset when have it
######







###### random forest
library(randomForest)

rf.well.ph1<-randomForest(y~., data=logdata, mtry=3, ntree=800, nodesize=1,importance=TRUE) 

pred.rf<-predict(rf.well.ph1, newdata=logdata) 
# pred.rf<-predict(rf.well, newdata=logdata, type="prob") # if user is caring about the probabilities 
tab=table(pred.rf,logdata$y)

randomForest:::varImpPlot(rf.well.ph1)



### phase1 reuslts prediction
logdata_test11<-mutate(logdata_test1, y=as.factor(rep(0,dim(logdata_test1)[1])))
logdata_test11<-rbind(logdata[1,],logdata_test11)
logdata_test11<-logdata_test11[-1,]
ph1_prob <- predict(rf.well.ph1, newdata=logdata_test11, type="prob")


###### No need to run the following chunk if accuracy is not needed, directly go to Phase2 prediction 
Accuracy_R<-function(ind){
  Train=logdata[-ind,]
  Test=logdata[ind,]
  rf.well<-randomForest(y~., data=Train, mtry=3, ntree=800, importance=TRUE)
  pred.rf<-predict(rf.well, newdata=Test)
  tab=table(pred.rf,Test$y)
  return((tab[1,1]+tab[2,2])/sum(tab))
}
library(caret)
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

(a[1,1]+a[2,2])/50
######
































###################################Phase2 Prediction###########################
#For this part run directly to line 1112, then read the commment
#load data
#setwd("/Users/qianhuang/Desktop/360/model/model ph2 vs well center ")
setwd(excel_folder_normalized_path)
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
#Data <- Data %>% distinct(UWI, Phase.2.ESA.Result, .keep_all = TRUE)
dup <- subset(Data,duplicated(UWI))

for (i in 1:dim(dup)[1]){
  loc=which(Data$UWI == dup$UWI[i])
  loc=loc[-which.max(Data$Source.Dates[loc])]
  Data=Data[-loc,]
}
dup <- subset(Data,duplicated(UWI))
Data<-Data[Data$Phase.2.ESA.Result!="",]
Data<-Data[Data$Phase.2.ESA.Result!="-",]

#write.csv(Data, "Data3.csv", row.names=FALSE)

############### Add licence to Data
Data$UWI <- stri_replace_all_fixed(Data$UWI, "​", "")
Data<-data.frame(Licence=Data$Licence.Number, UWI=Data$UWI, PF=Data$Phase.2.ESA.Result,
                 PFW=Data$Phase.2.ESA.Well.Centre, date=Data$Source.Dates)
# Data<-data.frame(Licence=Data$Licence.Number, UWI=Data$UWI, PF=Data$Phase.2.ESA.Result, date=Data$Source.Dates)
# Data<-Data[Data$Phase.2.ESA.Result!="",]
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
#Remove rows of well with UWIs >= 2, nonrep
licence <- unique(Sample$Licence)
tab <- table(Sample$Licence)
tabname <- names(tab)
loc <- which(tab[as.character(licence)]>1)
loc <- names(loc)
nonrep <- Sample[!Sample$Licence %in% loc,]

#Build sub data frame of well with UWIs >= 2, repuwi
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

Sample['PF'] <- NA
for (i in 1:length(lics)){
  if (lics[i] %in% lic){
    loc <- which(sapply(lic,function(x) lics[i] %in% x))
    pf=Data[loc,"PF"]
    if("Fail" %in% pf){
      Sample[i,"PF"] <- "Fail"
    }
    else{
      Sample[i,"PF"] <- "Pass"
    }
  }
}



####################################################################################################
#Select variables for the model
cate<-Sample[,c("Deepest.Form..Name","Full.Status","LLR.Abandonment.Area",
                "Lahee.Classification","Well.Trajectory","Projected.Formation",
                "Recovery.Mechanism","Geological.Formation","Producing.Formation",
                "Field","PSAC.Area.Code","Drilling.Contractor.Name",
                "Scheme.Sub.Type","EDCT","Type","Substance.Released.1","Well.Type.Final")]

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

#write.csv(nume,"nume1.csv",row.names=FALSE)

ph2 <- Sample$PF
ph2[ph2=="Fail"] <- 0
ph2[ph2=="Pass"] <- 1
ph2 <- as.numeric(ph2)


########################choose Phase 2 Result or Well Center
y=ph2


### For ph2
# var_nume=c("Spud.Date","Cum..Gas.Production..e3m3.","Completion.Bottom..mKB.",
#            "Initial.3Mth.Prod..Gas..Mcf.d.","Cum..Oil.Production..m3.",
#            "Initial.3Mth.Prod..Oil..BBL.d.","Surface.Aband..Date","Ground.Elevation..m.above.sea.level.",
#            "Cum..Water.Production..m3.","Cum..Condensate.Production..bbl.","Outside.Diameter..mm.")


lognume<-nume[,var_nume]

lognume_r <- dim(lognume)[1]

data_full <- rbind(lognume,lognume_test)
data_full_ph2 <- na.omit(data_full)
data_full <- as.data.frame(scale(data_full,center = T, scale = T))

lognume <- data_full[1:lognume_r,]
lognume_test2 <- data_full[(lognume_r+1):dim(data_full)[1],]
logdata_test2<-as.data.frame(cbind(lognume_test2,logcate_test2))

logcate<-cate[,c("LLR.Abandonment.Area","Well.Type.Final")] # add "Well.Type.Final" when it becomes available in test data
logcate <- as.data.frame(logcate)
colnames(logcate) <- c("LLR.Abandonment.Area","Well.Type.Final")# add "Well.Type.Final" when obtain the data


#logdata<-as.data.frame(cbind(lognume,y))
logdata<-as.data.frame(cbind(lognume,logcate,y))

logdata <- na.omit(logdata)
logdata$y=as.factor(logdata$y)




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



###### Please Note if Accuracy isn't Needed, do not run the following line. Directly go to "random forest"
logdata_logistic<-subset(logdata,(LLR.Abandonment.Area!="High Level"))#remove subset with fewer data points,check the well type subset when have it
######











##########################################Random Forest################################################
library(randomForest)

rf.well.ph2<-randomForest(y~., data=logdata, mtry=3, ntree=800, nodesize=1,importance=TRUE) 
pred.rf<-predict(rf.well.ph2, newdata=logdata)
# pred.rf<-predict(rf.well, newdata=logdata, type="prob")
tab=table(pred.rf,logdata$y)
(tab[1,1]+tab[2,2])/sum(tab)

randomForest:::varImpPlot(rf.well.ph2)


# Prediction
logdata_test22<-mutate(logdata_test2, y=as.factor(rep(0,dim(logdata_test2)[1])))
logdata_test22<-rbind(logdata[1,],logdata_test22)
logdata_test22<-logdata_test22[-1,]
ph2_prob<-predict(rf.well.ph2, newdata=logdata_test22, type="prob")


###### No need to run the following chunk if accuracy is not needed, directly go to Phase2 prediction 
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
######








































################################ Remediation value prediction ###############
# For this chunk, run to the end
#load data
setwd(excel_folder_normalized_path)
Data=read.csv("Environmental Data Collection V1_Jul6_clean.csv", as.is=TRUE, header=TRUE)
Data <- as.data.frame(Data)
Sample=read.csv("DDP1 Dataset at Aug 10_MAIN.csv",as.is=TRUE, header=TRUE)
Sample <- as.data.frame(Sample)

#load combine new data(Leah) 
RCV_Leah=read.csv("Contamination Volume Data_Leah.csv",as.is=TRUE, header=TRUE)
# remove leading zeros for licence in Leah's 
remove_leading_zeros <- function(strings) {
  result <- sub("^0+", "", strings)
  return(result)
}
RCV_Leah$Licence <- remove_leading_zeros(RCV_Leah$Licence)




Sample1=read.csv("Remediation Volume Attributes Sep14_with Well Type_MAIN.csv",as.is=TRUE, header=TRUE)
Sample <- rbind(Sample,Sample1)
Sample0 <- Sample

gas=read.csv("DDP1 Dataset at Aug 10_GASFIELD.csv",as.is=TRUE, header=TRUE)
gas1=read.csv("Remediation Volume Attributes Sep14_with Well Type_GASFIELD.csv",as.is=TRUE, header=TRUE)
gas <- rbind(gas,gas1)

casing=read.csv("DDP1 Dataset at Aug 10_CASING.csv",as.is=TRUE, header=TRUE)
casing1=read.csv("Remediation Volume Attributes Sep14_with Well Type_CASING.csv",as.is=TRUE, header=TRUE)
casing <- rbind(casing,casing1)

cement=read.csv("DDP1 Dataset at Aug 10_CEMENTING.csv",as.is=TRUE, header=TRUE)
cement1=read.csv("Remediation Volume Attributes Sep14_with Well Type_CEMENTING.csv",as.is=TRUE, header=TRUE)
cement <- rbind(cement,cement1)

incident=read.csv("DDP1 Dataset at Aug 10_INCIDENTS.csv",as.is=TRUE, header=TRUE)
incident1=read.csv("Remediation Volume Attributes Sep14_with Well Type_INCIDENTS.csv",as.is=TRUE, header=TRUE)
incident <- rbind(incident,incident1)

oil=read.csv("DDP1 Dataset at Aug 10_OILFIELD.csv")
oil1=read.csv("Remediation Volume Attributes Sep14_with Well Type_OILFIELD.csv")
oil <- rbind(oil,oil1)

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
                 date=Data$Source.Dates,
                 CV=Data$Phase.2.ESA.Combined.Contaminated.Volume..m3.,
                 RCV=Data$Remediated.Combined.Volume..m3.)
# Data<-data.frame(Licence=Data$Licence.Number, UWI=Data$UWI, PF=Data$Phase.2.ESA.Result, date=Data$Source.Dates)

for(i in 1:dim(Data)[1]){
  if(Data$UWI[i] %in% Sample0$UWI){
    loc=which(Data$UWI[i]==Sample0$UWI)
    Data$Licence[i]=Sample0$Licence[loc]
  }
}

######## Remediated Volume
# Add Remediated Volume
for(i in 1:dim(Data)[1]){
  if(Data$RCV[i]==''){
    Data$RCV[i]=Data$CV[i]
  }
  else if(Data$RCV[i]=='-'){
    Data$RCV[i]=Data$CV[i]
  }
}


# Keep rows with RCV
Data <- subset(Data,subset=Data$RCV!='')
Data$RCV <- stri_replace_all_fixed(Data$RCV, ",", "")
Data$RCV <- as.numeric(Data$RCV)
#write.csv(Data, "Data3.csv", row.names=FALSE)


##################
lics <- Sample$Licence
lic <- Data$Licence
uwis <- Sample$UWI
uwi <- Data$UWI


#####################################Matching attributes from different sheet
#sheet GAS FIELD
gas <- as.data.frame(gas)

common_col <- intersect(names(Sample),names(gas))
unique_col <- setdiff(names(gas),common_col)
Sample <- merge(Sample, gas[, c('UWI', unique_col)], by = 'UWI', all.x = TRUE)



#sheet CASING('Casing' only taken value of "SURFACE")
casing <- as.data.frame(casing)
casing <- casing[casing$Casing == 'SURFACE',]

common_col <- intersect(names(Sample),names(casing))
unique_col <- setdiff(names(casing),common_col)
Sample <- merge(Sample, casing[, c('UWI', unique_col)], by = 'UWI', all.x = TRUE)

#sheet CEMENTING
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
incident <- as.data.frame(incident)

common_col <- intersect(names(Sample),names(incident))
unique_col <- setdiff(names(incident),common_col)
Sample <- merge(Sample, incident[, c('UWI', unique_col)], by = 'UWI', all.x = TRUE)

Sample$Volume.Recovered.1[is.na(Sample$Volume.Recovered.1)] <- 0
Sample$Volume.Released.1[is.na(Sample$Volume.Released.1)] <- 0

#sheet OIL FIELD
oil <- as.data.frame(oil)
oil <- oil[,c("UWI","Cumulative.Oil.Production..e3m3.","Initial.Establish.Reserves.Oil.Enhanced..e3m3.","Initial.Establish.Reserves.Oil.Primary..e3m3.","Initial.Establish.Reserves.Oil.Total..e3m3.","Oil.In.Place..e3m3.","Remaining.Established.Oil.Reserves..e3m3.")]

common_col <- intersect(names(Sample),names(oil))
unique_col <- setdiff(names(oil),common_col)
Sample <- merge(Sample, oil[, c('UWI', unique_col)], by = 'UWI', all.x = TRUE)


#############################
#Remove rows of well with UWIs >= 2, nonrep
licence <- unique(Sample$Licence)
tab <- table(Sample$Licence)
tabname <- names(tab)
loc <- which(tab[as.character(licence)]>1)
loc <- names(loc)
nonrep <- Sample[!Sample$Licence %in% loc,]

#Build sub data frame of well with UWIs >= 2, repuwi
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

######################Add remediation volume to dataframe
#Add RCV from Data file
Sample['RCV'] <- NA
for (i in 1:length(lics)){
  if (lics[i] %in% lic){
    loc <- which(sapply(lic,function(x) lics[i] %in% x))
    Sample[i,"RCV"] <- Data[loc,"RCV"]
  }
}

#Add RCV from Leah's file
for (i in 1:length(lics)){
  if(lics[i] %in% RCV_Leah$Licence){
    loc <- which(sapply(RCV_Leah$Licence, function(x) lics[i] %in% x))
    Sample[i,"RCV"] <- RCV_Leah[loc[1],"Volume.for.Model..m3."]
  }
}

# keep Sample data with RCV values
Sample <- subset(Sample,subset=Sample$RCV!='')



###### select attributes
cate<-Sample[,c("Deepest.Form..Name","Full.Status","LLR.Abandonment.Area",
                "Lahee.Classification","Well.Trajectory","Projected.Formation",
                "Recovery.Mechanism","Geological.Formation","Producing.Formation",
                "Field","PSAC.Area.Code","Drilling.Contractor.Name",
                "Scheme.Sub.Type","EDCT","Type","Well.Type.Final")]

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


################################################################################
### Parameter selection
y=as.numeric(Sample$RCV)

lognume<-nume[,var_nume]
lognume_r <- dim(lognume)[1]

data_full <- rbind(lognume,lognume_test)
data_full_ph2 <- na.omit(data_full)
data_full <- as.data.frame(scale(data_full,center = T, scale = T))

lognume <- data_full[1:lognume_r,]
lognume_test3 <- data_full[(lognume_r+1):dim(data_full)[1],]
logdata_test3 <- as.data.frame(cbind(lognume_test3,logcate_test))


logcate<-cate[,c("LLR.Abandonment.Area","Well.Type.Final")] 
logcate <- as.data.frame(logcate)
colnames(logcate) <- c("LLR.Abandonment.Area","Well.Type.Final")


logdata<-as.data.frame(cbind(lognume,y))
logdata<-as.data.frame(cbind(lognume,logcate,y))

logdata <- na.omit(logdata)


# CHECK the test data attributes
# tab_LLR=table(logdata$LLR.Abandonment.Area)
# level_LLR=levels(logdata$LLR.Abandonment.Area)
# if(sum(tab_LLR==0)>0){
# level_LLR=level_LLR[-which(tab_LLR==0)]
# }
# 
# tab_Well=table(logdata$Well.Type.Final)
# level_Well=levels(logdata$Well.Type.Final)
# if(sum(tab_Well==0)>0){
# level_Well=level_Well[-which(tab_Well==0)]
# }
# 
# ind_LLR=which(logdata_test3$LLR.Abandonment.Area%in%level_LLR)
# ind_Well=which(logdata_test3$Well.Type.Final%in%level_Well)
# 
# loc1=intersect(ind_LLR,ind_Well)
# 
# logdata_testFULL<-logdata_test3[loc1,]
# logdata_testLACK<-logdata_test3[-loc1,]
# logdata_testLACK[,c("LLR.Abandonment.Area","Well.Type.Final")]=c()

#testdata have "well.type.final" "did not produce"
#loc <- which(logdata_test3$Well.Type.Final=="did not produce")
#logdata_test3welltype <- logdata_test3[loc,]
#logdata_test3 <- logdata_test3[-loc,]



#testdata have "LLR", "High Level"
#loc <- which(logdata_test3$LLR.Abandonment.Area=="High Level")
#logdata_test3llr <- logdata_test3[loc,]
#logdata_test3 <- logdata_test3[-loc,]


#loc <- which(logdata$y==0)
#logdata <- logdata[-loc,]



# Fit the model by using GLM
glm.normal1<-glm(y~., data = logdata, family = gaussian(link="identity"))

# Remove the rows with y=0 (because gamma and inverse gaussian distributions cannot yield zeros)
logdata=logdata[-which(logdata$y==0),]
glm.gamma1<-glm(y~Spud.Date, data = logdata, family = Gamma(link="log"))
glm.gamma2<-glm(y~Spud.Date + 
                  Well.Type.Final + 
                  Initial.3Mth.Prod..BOE..BOE.d.+ 
                  LLR.Abandonment.Area +
                  Last.3Mth.Prod..Oil..BBL.d. + 
                  Initial.3Mth.Prod..Oil..BBL.d. +
                  Oil.In.Place..e3m3. +
                  Last.3Mth.Prod..Water..BBL.d., data = logdata, family = Gamma(link="log"))

# Example using 'glm' with a different optimization method (bfgs)
# glm.gamma2<-glm(y~., data = logdata, family = Gamma(link = "log"))

#glm.gamma2<-glm(y~., data = logdata, family = Gamma(link="inverse"))
glm.invgau<-glm(y~Spud.Date, data = logdata, family = inverse.gaussian(link="inverse"))

# Comparing their AIC
glm.normal1$aic
glm.gamma1$aic
glm.gamma2$aic
glm.invgau$aic




# Prediction
pred.normal1<-predict(glm.normal1, newdata=logdata_test3, type="response")
pred.gamma1<-predict(glm.gamma1, newdata=logdata_test3, type="response")
pred.gamma2<-predict(glm.gamma2, newdata=logdata_test3, type="response")
pred.invgau<-predict(glm.invgau, newdata=logdata_test3, type="response")





# Combine the prediction results
# logdata[,c("LLR.Abandonment.Area","Well.Type.Final")]=c()
# glm.normal12<-glm(y~., data=logdata, family = gaussian(link="identity"))
# pred.normal12<-predict(glm.normal12, newdata=logdata_testLACK, type="response")
# 
# #glm.gamma12<-glm(y~., data = logdata, family = Gamma(link="log"))
# glm.gamma12<-glm(y~Spud.Date, data = logdata, family = Gamma(link="log"))
# pred.gamma12<-predict(glm.gamma12, newdata=logdata_testLACK, type="response")
# 
# pred_normal=rep(0,dim(logdata_test3)[1])
# pred_normal[loc1]=pred.normal1
# pred_normal[-loc1]=pred.normal12
# 
# 
# pred_gamma=rep(0,dim(logdata_test3)[1])
# pred_gamma[loc1]=pred.gamma1
# pred_gamma[-loc1]=pred.gamma12
# 
# 
# logdata_test3_new<-cbind(logdata_test3, pred_normal, pred_gamma)




Pred_RV2 <- cbind(testdata$Licence, pred.normal1, pred.gamma1, pred.gamma2, pred.invgau)
Pred_RV2 <- as.data.frame(Pred_RV2)
colnames(Pred_RV2)=c("Licence", "Remediated Volume (Normal)","Remediated Volume (Gamma1)",
                     "Remediated Volume (Gamma2)","Remediated Volume (InversGaussian)")
# Pred_RV1 <- read.csv("results.csv")
# Pred_RV2 <- read.csv("results2.csv")




### Final Output 
#ph1 ph2
pred_result <- as.data.frame(cbind(testdata$Licence,ph1_prob[,2],ph2_prob[,2]))
colnames(pred_result)=c("Licence","ph1_pass_prob", "ph2_pass_prob")

setwd("/Users/qianhuang/Desktop/360/model/test data")
fullresults=read.csv("results.csv",as.is=TRUE, header=TRUE)

fullresults['ph1_pass_prob'] <- NA
fullresults['ph2_pass_prob'] <- NA

for(i in 1:dim(pred_result)[1]){
  loc = which(pred_result$Licence[i]==fullresults$Licence)
  fullresults$ph1_pass_prob[loc]=pred_result$ph1_pass_prob[i]
  fullresults$ph2_pass_prob[loc]=pred_result$ph2_pass_prob[i]
}

write.csv(fullresults,"Remediated Volume Prediction ph1ph2results.csv",row.names = FALSE)
fullresults <- read.csv("Remediated Volume Prediction ph1ph2results.csv", as.is=TRUE, header=TRUE)

pred_result2 <- as.data.frame(cbind(testdata$Licence,ph1_prob[,2],ph2_prob[,2]))
colnames(pred_result2)=c("Licence","ph1_pass_prob", "ph2_pass_prob")

for(i in 1:dim(pred_result2)[1]){
  loc = which(pred_result2$Licence[i]==fullresults$Licence)
  if(is.na(fullresults$ph1_pass_prob[loc])){
    fullresults$ph1_pass_prob[loc]=pred_result2$ph1_pass_prob[i]
  }
  if(is.na(fullresults$ph2_pass_prob[loc])){
    fullresults$ph2_pass_prob[loc]=pred_result2$ph2_pass_prob[i]
  }
}




#remediated volume
fullresults=read.csv("Remediation Volume Prediction.csv",as.is=TRUE, header=TRUE)

fullresults['normal'] <- NA
fullresults['gamma1'] <- NA
fullresults['gamma2'] <- NA
fullresults['inversegaussian'] <- NA
for(i in 1:dim(Pred_RV2)[1]){
  loc = which(Pred_RV2$Licence[i]==fullresults$Licence)
  fullresults$normal[loc]=Pred_RV2$`Remediated Volume (Normal)`[i]
  fullresults$gamma1[loc]=Pred_RV2$`Remediated Volume (Gamma1)`[i]
  fullresults$gamma2[loc]=Pred_RV2$`Remediated Volume (Gamma2)`[i]
  fullresults$inversegaussian[loc]=Pred_RV2$`Remediated Volume (InversGaussian)`[i]
}

for(i in 1:dim(Pred_RV1)[1]){
  loc = which(Pred_RV1$Licence[i]==fullresults$Licence)
  if(!(is.na(Pred_RV1$pred_normal[i]))){
    fullresults$pred_normal[loc]=Pred_RV1$pred_normal[i]
  }
  if(!(is.na(Pred_RV1$pred_gamma[i]))){
    fullresults$pred_gamma[loc]=Pred_RV1$pred_gamma[i]
  }
}

write.csv(fullresults,"Master Wells_ARC_Q3_Rem Licences.csv",row.names = FALSE)



Pred_result<-cbind(testdata$UWI,ph1_prob[,2], ph2_prob[,2], pred.normal1, pred.gamma2)

colnames(fullresults)=c("UWI","Phase 1 Pass Probability", "Phase 2 Pass Probability", "Predicted Remediation Vol (Gaussian identity-link)", 
                        "Predicted Remediation Vol (Gamma inv-link)")
