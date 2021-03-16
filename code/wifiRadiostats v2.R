library(drake)
library(lubridate)
library(tidyverse)
library(reshape)

raw_data  <- read_csv('F:/Dashboarding/SharePoint/CommScope/XWiFi XM Offload - og1600Logs/15 min/2-arrisRouterWiFiRadioStats/OG1600.WiFiRadioStats.2020_06_13.csv', 
                      col_types = cols(`2.4_RadioACKFailed` = col_number(), 
                                       `2.4_RadioConnectionExceed` = col_number(), 
                                       `2.4_RadioMgtRxOk` = col_number(), 
                                       `2.4_RadioMgtTxFailed` = col_number(), 
                                       `2.4_RadioMgtTxOk` = col_number(), 
                                       `2.4_RadioRxDropped` = col_number(), 
                                       `2.4_RadioRxFailed` = col_number(), 
                                       `2.4_RadioRxOffer` = col_number(), 
                                       `2.4_RadioRxOfferBytes` = col_number(), 
                                       `2.4_RadioRxOk` = col_number(), `2.4_RadioRxOkBytes` = col_number(), 
                                       `2.4_RadioTxDropped` = col_number(), 
                                       `2.4_RadioTxErrorRate` = col_number(), 
                                       `2.4_RadioTxFailed` = col_number(), 
                                       `2.4_RadioTxOffer` = col_number(), 
                                       `2.4_RadioTxOfferBytes` = col_number(), 
                                       `2.4_RadioTxOk` = col_number(), `2.4_RadioTxOkBytes` = col_number(), 
                                       `5G_RadioACKFailed` = col_number(), 
                                       `5G_RadioConnectionExceed` = col_number(), 
                                       `5G_RadioMgtRxOk` = col_number(), 
                                       `5G_RadioMgtTxFailed` = col_number(), 
                                       `5G_RadioMgtTxOk` = col_number(), 
                                       `5G_RadioRxDropped` = col_number(), 
                                       `5G_RadioRxFailed` = col_number(), 
                                       `5G_RadioRxOffer` = col_number(), 
                                       `5G_RadioRxOfferBytes` = col_number(), 
                                       `5G_RadioRxOk` = col_number(), `5G_RadioRxOkBytes` = col_number(), 
                                       `5G_RadioTxDropped` = col_number(), 
                                       `5G_RadioTxErrorRate` = col_number(), 
                                       `5G_RadioTxFailed` = col_number(), 
                                       `5G_RadioTxOffer` = col_number(), 
                                       `5G_RadioTxOfferBytes` = col_number(), 
                                       `5G_RadioTxOk` = col_number(), `5G_RadioTxOkBytes` = col_number(), 
                                       Timestamp = col_datetime(format = "%d/%m/%Y %H:%M:%S")))

#creating separate df's for 2.4 & 5 Ghz spectrum
df                                          <- raw_data %>% select(Timestamp, 
                                                                   Current_IP, 
                                                                   Current_MAC, 
                                                                   Current_Location, 
                                                                   `2.4_RadioRxbyrate`, 
                                                                   `2.4_RadioTxbyrate`,
                                                                   `5G_RadioRxbyrate`, 
                                                                   `5G_RadioTxbyrate`)

df                                          <- df %>% filter(`2.4_RadioRxbyrate` != "na")
df1                                         <- df %>% separate(`2.4_RadioRxbyrate`, c(paste0("2.4_Rx_mcs", 0:23)), sep = ",")
df1                                         <- df1 %>% separate(`2.4_RadioTxbyrate`, c(paste0("2.4_Tx_mcs", 0:23)), sep = ",")
df1                                         <- df1 %>% separate(`5G_RadioRxbyrate`, c(paste0("5G_Rx_mcs", 0:9)), sep = ",")
df1                                         <- df1 %>%  separate(`5G_RadioTxbyrate`, c(paste0("5G_Tx_mcs", 0:9)), sep = ",")

#Regex pattern
pattern = '.*\\STATS:\\.*'

df1$`2.4_Rx_mcs0`                           <- as.numeric(gsub(pattern, "", df1$`2.4_Rx_mcs0`, perl=TRUE))
df1$`2.4_Rx_mcs8`                           <- as.numeric(gsub(pattern, "", df1$`2.4_Rx_mcs8`, perl=TRUE))
df1$`2.4_Rx_mcs16`                          <- as.numeric(gsub(pattern, "", df1$`2.4_Rx_mcs16`, perl=TRUE))

df1$`2.4_Tx_mcs0`                           <- as.numeric(gsub(pattern, "", df1$`2.4_Tx_mcs0`, perl=TRUE))
df1$`2.4_Tx_mcs8`                           <- as.numeric(gsub(pattern, "", df1$`2.4_Tx_mcs8`, perl=TRUE))
df1$`2.4_Tx_mcs16`                          <- as.numeric(gsub(pattern, "", df1$`2.4_Tx_mcs16`, perl=TRUE))


df1$`5G_Rx_mcs0`                            <- as.numeric(gsub(pattern, "", df1$`5G_Rx_mcs0`, perl=TRUE))
df1$`5G_Rx_mcs5`                            <- as.numeric(gsub(pattern, "", df1$`5G_Rx_mcs5`, perl=TRUE))

df1$`5G_Tx_mcs0`                            <- as.numeric(gsub(pattern, "", df1$`5G_Tx_mcs0`, perl=TRUE))
df1$`5G_Tx_mcs5`                            <- as.numeric(gsub(pattern, "", df1$`5G_Tx_mcs5`, perl=TRUE))


#some values in 5G_Tx_mcs9 are NA's - total such values are
sum(is.na(df1$`5G_Tx_mcs9`)) # result = 192

#casting NA's on 5G_Tx_mcs9 to 0
df1$`5G_Tx_mcs9`[is.na(df1$`5G_Tx_mcs9`)]   <- 0

#casting to numeric
df1[,c(5:72)]                               <- sapply(df1[, c(5:72)], as.numeric)

# adding date columns
df1                                         <- df1 %>% mutate(obs_day = date(Timestamp), 
                                                              obs_hour = hour(Timestamp))

# start database processing here
# summarise
list_of_cols                                <- colnames(df1[, c(5:72)])
df2_by_hour                                 <- df1 %>% group_by(Current_MAC, Current_Location, obs_day, obs_hour) %>% summarise_at(list_of_cols, sum)
df2_by_day                                  <- df1 %>% group_by(Current_MAC, Current_Location, obs_day) %>% summarise_at(list_of_cols, sum)

#gather
# setting index as the location column
# rownames(df2)                             <- df2$Current_Location

# melting data
library(reshape)
df_melted                                   <- df2_by_day[-c(2)]
df_melted                                   <- as.data.frame(df_melted)
df_melted                                   <- melt(df_melted, id=c("Current_MAC", "obs_day"))

pattern                                     <- "\\d\\.?\\w?\\_\\w+\\_\\mcs(\\d+)" # perl=TRUE
df_melted                                   <- df_melted %>% mutate(sort_order = str_match(variable, pattern)[,2]) # creating sort order

pattern1                                    <- "\\d\\.?\\w?\\_(\\w+)\\_\\mcs\\d+" # perl=TRUE
df_melted                                   <- df_melted %>% mutate(transmission_type = str_match(variable, pattern1)[,2]) # creating sort order

pattern2                                    <- "(\\d\\.?\\w?)\\_\\w+\\_\\mcs\\d+" # perl=TRUE
df_melted                                   <- df_melted %>% mutate(radio_type = str_match(variable, pattern2)[,2]) # creating sort order

#creating FACT tables
# df_FACT                                     <- df %>% group_by(Current_MAC, Current_Location) %>% tally()
# df_LOCATION                                 <- unique(df_FACT$Current_Location)


# writing CSVs
# write.csv(df2_by_day, './data/pickles/mcs_06_15_2020.csv')
write.csv(df_melted, './data/melted/mcs_06_13_2020_melted.csv')
# write.csv(df_FACT, './data/pickles/mcs_06_15_2020_FACT.csv')
# write.csv(df_LOCATION, './data/pickles/mcs_06_15_2020_LOCATION.csv')

# colnames(df_melted)

# #plotting
# library(plotly)
# 
# fig                                       <- plot_ly(
#   x = df2$Current_Location,
#   y = df2$Rx_mcs0,
#   name = "mcs0",
#   type = "bar"
# )
# 
# fig                                       <- fig %>%
#   add_trace(y = ~df2$Rx_mcs1, type = 'bar', name='mcs1') %>%
#   add_trace(y = ~df2$Rx_mcs2, type = 'bar', name='mcs2') %>%
#   add_trace(y = ~df2$Rx_mcs3, type = 'bar', name='mcs3') %>%
#   add_trace(y = ~df2$Rx_mcs4, type = 'bar', name='mcs4') %>%
#   add_trace(y = ~df2$Rx_mcs5, type = 'bar', name='mcs5') %>%
#   add_trace(y = ~df2$Rx_mcs6, type = 'bar', name='mcs6') %>%
#   add_trace(y = ~df2$Rx_mcs7, type = 'bar', name='mcs7') %>%
#   add_trace(y = ~df2$Rx_mcs8, type = 'bar', name='mcs8') %>%
#   add_trace(y = ~df2$Rx_mcs9, type = 'bar', name='mcs9') %>%
#   add_trace(y = ~df2$Rx_mcs10, type = 'bar', name='mcs10') %>%
#   add_trace(y = ~df2$Rx_mcs11, type = 'bar', name='mcs11') %>%
#   add_trace(y = ~df2$Rx_mcs12, type = 'bar', name='mcs12') %>%
#   add_trace(y = ~df2$Rx_mcs13, type = 'bar', name='mcs13') %>%
#   add_trace(y = ~df2$Rx_mcs14, type = 'bar', name='mcs14') %>%
#   add_trace(y = ~df2$Rx_mcs15, type = 'bar', name='mcs15') %>%
#   add_trace(y = ~df2$Rx_mcs16, type = 'bar', name='mcs16') %>%
#   add_trace(y = ~df2$Rx_mcs17, type = 'bar', name='mcs17') %>%
#   add_trace(y = ~df2$Rx_mcs18, type = 'bar', name='mcs18') %>%
#   add_trace(y = ~df2$Rx_mcs19, type = 'bar', name='mcs19') %>%
#   add_trace(y = ~df2$Rx_mcs20, type = 'bar', name='mcs20') %>%
#   add_trace(y = ~df2$Rx_mcs21, type = 'bar', name='mcs21') %>%
#   add_trace(y = ~df2$Rx_mcs22, type = 'bar', name='mcs22') %>%
#   add_trace(y = ~df2$Rx_mcs23, type = 'bar', name='mcs23') %>%
#   layout(yaxis = list(title = 'sum of MCS'), barmode = 'group')
# 
# 
# fig
# 
# updatemenus                               <- list(
#   list(
#     active = -1,
#     type= 'buttons',
#     buttons = list(
#       list(
#         label = "High",
#         method = "update",
#         args = list(list(visible = c(FALSE, TRUE)),
#                     list(title = "Apple High",
#                          annotations = list(c(), high_annotations)))),
#       list(
#         label = "Low",
#         method = "update",
#         args = list(list(visible = c(TRUE, FALSE)),
#                     list(title = "Apple Low",
#                          annotations = list(low_annotations, c() )))),
#       list(
#         label = "Both",
#         method = "update",
#         args = list(list(visible = c(TRUE, TRUE)),
#                     list(title = "Apple",
#                          annotations = list(low_annotations, high_annotations)))),
#       list(
#         label = "Reset",
#         method = "update",
#         args = list(list(visible = c(TRUE, TRUE)),
#                     list(title = "Apple",
#                          annotations = list(c(), c())))))
#   )
# )
# 




# 
# df5G                                      <- raw_data %>% select(Timestamp, Current_IP, Current_MAC, Current_Location, `5G_RadioRxbyrate`, `5G_RadioTxbyrate`)
# df5G                                      <- df5G %>% filter(`5G_RadioRxbyrate` != "na")
