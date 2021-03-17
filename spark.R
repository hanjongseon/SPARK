library(sparklyr)
library(dplyr)
library(dbplyr)
library(data.table)

conf = spark_config()
conf$spark.executor.memory = "40G"
conf$`sparklyr.shell.driver-memory` = '40G'
conf$sparkr.memory.fraction = 0.9
conf$spark.rapids.sql.concurrentGpuTasks = 4
conf$spark.sql.shuffle.partitions.local = 10
conf$spark.rapids.memory.gpu.pooling.enabled = 'true'
conf$spark.rapids.memory.gpu.allocFraction = 0.9
sc = spark_connect(master='local', config=conf, version = '3.0')
spark_web(sc)

compy = function(x) {
  compute(x,spark_table_name(x))
}

Total_POP2 = spark_read_parquet(sc, path = "d:/work/Total_POP.parquet", name = "Total_POP2", overwrite = T)
Total_POP2

tbl_cache(sc, "Total_POP2")
Total_POP2

Total_POP2 = 
  Total_POP2 %>% mutate(
    NEW_BLD = "",
    NEW_DONG = "",
    RESULT = ""
  ) %>% compy()

Total_POP2

sdf_nrow(Total_POP2)

src_tbls(sc)
Total_POP2 %>% colnames()

Total_POP2 %>% show_query()

Total_POP2 = 
  Total_POP2 %>% 
  mutate(
    NEW_BLD = BLD_NAME,
    NEW_DONG = DONG_NAME,
    REFER_DONG = DONG_NAME,
    RESULT = ""
  ) %>% compy()

Total_POP2 %>% show_query()

#Total_POP[is.na(Total_POP)] = ""
Total_POP2 = na.replace(Total_POP2, "")

Total_POP2 %>% 
  filter(rlike(NEW_DONG, "\\\\(.*(길|로).*[0-9]\\\\)")) 

A = Sys.time()
Total_POP2 = 
  Total_POP2 %>% 
  #filter(rlike(NEW_DONG, "\\\\(.*(길|로).*[0-9]\\\\)")) %>% 
  mutate(
    NEW_DONG = ifelse(rlike(NEW_DONG, "\\\\(.*(길|로).*[0-9]\\\\)"), regexp_replace(NEW_DONG,"\\\\(.*(길|로).*[0-9]\\\\)", ""), NEW_DONG)
  ) %>% compy()
Sys.time()-A

Total_POP2


Total_POP2
Total_POP2 %>% show_query()



Total_POP2 %>%
  filter(
    rlike(NEW_DONG, "^[0-9]+동$|^[A-Z]동$|^(가|나|다|라|마|바|사|아|자|차|카|타|파|하)동$")
  ) %>%
  select(NEW_DONG) 


A = Sys.time()
Total_POP2 = 
  Total_POP2 %>% 
  mutate(
    NEW_DONG = ifelse(rlike(NEW_DONG, "^[0-9]+동$|^[A-Z]동$|^(가|나|다|라|마|바|사|아|자|차|카|타|파|하)동$"), regexp_replace(NEW_DONG,"동$", ""), NEW_DONG)
  ) %>% compy()
Sys.time()-A


Total_POP2 %>% 
  filter(rlike(NEW_DONG, "^제[0-9]+동$|^제[A-Z]동$|^제(가|나|다|라|마|바|사|아|자|차|카|타|파|하)동$")) %>% 
  sdf_nrow()


Total_POP2 = 
  Total_POP2 %>% 
  mutate(
    NEW_DONG = ifelse(rlike(NEW_DONG, "^제[0-9]+동$|^제[A-Z]동$|^제(가|나|다|라|마|바|사|아|자|차|카|타|파|하)동$"),
                      regexp_replace(NEW_DONG, "(^제|동$)", ""),
                      NEW_DONG)
  ) %>% compy()

#Total_POP2 = Total_POP2 %>% compy()
Total_POP2 %>% show_query()

# Total_POP2 %>% 
#   select(NEW_DONG)%>% 
#   spark_apply(
#     function(e) ifelse(rlike(e, "^제[0-9]+동$|^제[A-Z]동$|^제(가|나|다|라|마|바|사|아|자|차|카|타|파|하)동$"),
#                        regexp_replace(e, "(^제|동$)", ""),
#                        e), names = 'RESULT'
#   )


# Total_POP2 %>% 
#   filter(rlike(NEW_DONG," 제?[0-9]+동$")) %>% 
#   select(NEW_DONG)

A = Sys.time()
Total_POP2 = 
  Total_POP2 %>% left_join(
    Total_POP2 %>% 
      filter(rlike(NEW_DONG, "^[가-힇]{4,}제[0-9A-Z]동")) %>% 
      select(NEW_DONG) %>% 
      mutate(
        NEW_DONG = regexp_replace(NEW_DONG, "동$", "")
      ) %>% 
      mutate(
        TEMP_SPLIT = split(NEW_DONG, "제")
      ) %>% 
      sdf_separate_column("TEMP_SPLIT", into=c("TEMP_BLD", "TEMP_DONG")) %>% 
      select(NEW_DONG, TEMP_BLD, TEMP_DONG) %>% distinct()) %>% 
  mutate(
    NEW_DONG = ifelse(!is.na(TEMP_DONG), TEMP_DONG, NEW_DONG),
    NEW_BLD = ifelse(!is.na(TEMP_DONG), TEMP_BLD, NEW_BLD)
  ) %>% 
  select(-TEMP_BLD, -TEMP_DONG) %>% compy()
Sys.time()-A

Total_POP2 #%>% sdf_nrow()
Total_POP2 %>% head()

Total_POP2 %>% 
  filter(
    rlike(NEW_DONG, ".*차$")
  )# %>% sdf_nrow()

Total_POP2 %>% 
  filter(
    rlike(NEW_DONG, ".*차$")
  ) %>% select(
    NEW_BLD, NEW_DONG
  ) %>% 
  mutate(
    TEMP_BLD = NEW_DONG,
    TEMP_DONG = ""
  ) %>% distinct() %>% 
  select(TEMP_BLD)

Total_POP2 = 
  Total_POP2 %>% 
  filter(
    rlike(NEW_DONG, ".*차$")
  ) %>% select(
    NEW_BLD, NEW_DONG
  ) %>% 
  mutate(
    TEMP_BLD = NEW_DONG,
    TEMP_DONG = ""
  ) %>% distinct() %>% 
  right_join(Total_POP2) %>% 
  mutate(
    NEW_DONG = ifelse(!is.na(TEMP_DONG), TEMP_DONG, NEW_DONG),
    NEW_BLD = ifelse(!is.na(TEMP_DONG), TEMP_BLD, NEW_BLD)
  ) %>% select(-TEMP_BLD, -TEMP_DONG) %>% compy()


Total_POP2 %>% 
  filter(
    rlike(NEW_DONG, ".*(주택$|빌라$)")
  ) %>% select(NEW_DONG) %>% 
  filter(
    rlike(NEW_DONG, '빌라$')
  )

# sdf_nrow(Total_POP2)
# Total_POP2 %>% show_query()
# Total_POP2 %>% compy()


Total_POP2 = 
  Total_POP2 %>% 
  filter(
    rlike(NEW_DONG, ".*(주택|빌라)$")
  ) %>% select(
    NEW_BLD, NEW_DONG
  ) %>% 
  mutate(
    TEMP_BLD = paste(NEW_BLD, NEW_DONG),
    TEMP_DONG = ""
  ) %>% distinct() %>% 
  right_join(Total_POP2) %>% 
  mutate(
    NEW_DONG = ifelse(!is.na(TEMP_DONG), TEMP_DONG, NEW_DONG),
    NEW_BLD = ifelse(!is.na(TEMP_DONG), TEMP_BLD, NEW_BLD)
  ) %>% select(-TEMP_BLD, -TEMP_DONG) %>% compy()

Total_POP2 #%>% head()

Total_POP2 %>% 
  filter(
    rlike(NEW_DONG, "^(에이|비|비이|씨|시|디|이|에프|지|쥐|에이치|아이|제이|케이|엘|엠|엔|오|피|큐|알|에스|티|유|브이|더블유|엑스|와이|제트)동$")
  ) %>% select(NEW_DONG)


Total_POP2 = 
  Total_POP2 %>% 
  mutate(
    NEW_DONG=ifelse(rlike(NEW_DONG, "^(에이|비|비이|씨|시|디|이|에프|지|쥐|에이치|아이|제이|케이|엘|엠|엔|오|피|큐|알|에스|티|유|브이|더블유|엑스|와이|제트)동$")
                    , regexp_replace(NEW_DONG, "동$", ""), NEW_DONG)
  ) %>% compy()

Total_POP2 = 
  Total_POP2 %>% 
  # filter(
  #   rlike(NEW_DONG, "^(에이|비|비이|씨|시|디|이|에프|지|쥐|에이치|아이|제이|케이|엘|엠|엔|오|피|큐|알|에스|티|유|브이|더블유|엑스|와이|제트)$")
  # ) %>% 
  mutate(
    NEW_DONG = case_when(
      rlike(NEW_DONG, '^에이$')~"A",
      rlike(NEW_DONG, '^비$')~"B",
      rlike(NEW_DONG, '^비이$')~"B",
      rlike(NEW_DONG, '^씨$')~"C",
      rlike(NEW_DONG, '^시$')~"C",
      rlike(NEW_DONG, '^디$')~"D",
      rlike(NEW_DONG, '^이$')~"E",
      rlike(NEW_DONG, '^에프$')~"F",
      rlike(NEW_DONG, '^쥐$')~"G",
      rlike(NEW_DONG, '^지$')~"G",
      rlike(NEW_DONG, '^에이치$')~"H",
      rlike(NEW_DONG, '^아이$')~"I",
      rlike(NEW_DONG, '^제이$')~"J",
      rlike(NEW_DONG, '^케이$')~"K",
      rlike(NEW_DONG, '^엘$')~"L",
      rlike(NEW_DONG, '^엠$')~"M",
      rlike(NEW_DONG, '^엔$')~"N",
      rlike(NEW_DONG, '^오$')~"O",
      rlike(NEW_DONG, '^피$')~"P",
      rlike(NEW_DONG, '^큐$')~"Q",
      rlike(NEW_DONG, '^알$')~"R",
      rlike(NEW_DONG, '^에스$')~"S",
      rlike(NEW_DONG, '^티$')~"T",
      rlike(NEW_DONG, '^유$')~"U",
      rlike(NEW_DONG, '^브이$')~"V",
      rlike(NEW_DONG, '^더블유$')~"W",
      rlike(NEW_DONG, '^엑스$')~"X",
      rlike(NEW_DONG, '^와이$')~"Y",
      rlike(NEW_DONG, '^제트$')~"Z",
      TRUE ~ NEW_DONG
    )
  ) %>% compy()#%>% select(NEW_DONG) %>% distinct()
#엠|엔|오|피|큐|알|에스|티|유|브이|더블유|엑스|와이|제트
# 
# case_when(
#   RESULT!='Comp' & rlike(NEW_DONG, "^[0-9]+동$|^[A-Z]동$|^(가|나|다|라|마|바|사|아|자|차|카|타|파|하)동$") ~ 'Comp'#,
#   #RESULT=='Comp' & rlike(NEW_DONG, "^[0-9]+동$|^[A-Z]동$|^(가|나|다|라|마|바|사|아|자|차|카|타|파|하)동$") ~ NEW_DONG
# )


Total_POP2 %>% 
  filter(REFER_DONG!=NEW_DONG) %>% 
  select(NEW_DONG, REFER_DONG)


# Total_POP2

Total_POP2 %>% sdf_nrow()

Total_POP2 = 
  Total_POP2 %>% 
  filter(
    rlike(NEW_DONG,"[가-힇]+[0-9A-Z]$")
  ) %>% 
  mutate(
    TEMP_DONG = regexp_extract(NEW_DONG, "[0-9A-Z]$", 0),
    TEMP_BLD = regexp_replace(NEW_DONG, "[0-9A-Z]$", "")
  ) %>% 
  select(
    NEW_DONG, TEMP_DONG, TEMP_BLD
  ) %>% distinct() %>% 
  right_join(Total_POP2) %>% 
  mutate(
    NEW_DONG = ifelse(!is.na(TEMP_DONG), TEMP_DONG, NEW_DONG),
    NEW_BLD = ifelse(!is.na(TEMP_BLD), TEMP_BLD, NEW_BLD)
  ) %>% 
  select(
    -TEMP_DONG, -TEMP_BLD
  ) %>% compy()

Total_POP2 %>% head()

Total_POP2 = Total_POP2 %>% 
  filter(
    rlike(NEW_DONG, "[가-힇]+[0-9A-Z]+동$")
  ) %>% 
  mutate(
    TEMP_DONG = regexp_extract(NEW_DONG, '[0-9A-Z]+동$', 0),
    TEMP_BLD = regexp_replace(NEW_DONG, '[0-9A-Z]+동$', '')
  ) %>% 
  select(NEW_DONG, TEMP_DONG, TEMP_BLD) %>% 
  distinct() %>% 
  right_join(Total_POP2) %>% 
  mutate(
    NEW_DONG = ifelse(!is.na(TEMP_DONG), TEMP_DONG, NEW_DONG),
    NEW_BLD = ifelse(!is.na(TEMP_BLD), TEMP_BLD, NEW_BLD)
  ) %>%  select(
    -TEMP_DONG, -TEMP_BLD
  ) %>% compy()

Total_POP2 %>% show_query()
Total_POP2 %>% head()

spark_write_csv(Total_POP2, "d:/work/Total_POP2_csv",delimiter = '|')
library(data.table)
dfs = lapply(paste0('d:/work/Total_POP2_csv/', dir("d:/work/Total_POP2_csv", pattern = '.csv')), function(x) {fread(x, encoding='UTF-8')})
unlink("d:/work/Total_POP2_csv",recursive = TRUE)
dir
dfs_frame = bind_rows(dfs)
dfs_frame

Total_POP2 = 
  Total_POP2 %>% 
  filter(
    rlike(NEW_DONG, "(.*빌$|.*빌딩$|.*빌리지$)")
  ) %>% select(
    NEW_DONG
  ) %>% 
  mutate(
    TEMP_DONG = "",
    TEMP_BLD = NEW_DONG
  ) %>% distinct() %>% 
  right_join(Total_POP2) %>% 
  mutate(
    NEW_DONG = ifelse(!is.na(TEMP_DONG), TEMP_DONG, NEW_DONG),
    NEW_BLD = ifelse(!is.na(TEMP_BLD), TEMP_BLD, NEW_BLD)
  ) %>% 
  select(
    -TEMP_DONG, -TEMP_BLD
  ) %>% compy()

Total_POP2 = 
  Total_POP2 %>% 
  filter(
    rlike(NEW_DONG, "[가-힇]\\\\(.*동\\\\)")
  ) %>% select(
    NEW_BLD, NEW_DONG
  ) %>% 
  mutate(
    TEMP_DONG = regexp_replace(NEW_DONG, "(.*\\\\(|동\\\\)$)", ""),
    TEMP_BLD = regexp_replace(NEW_DONG, "\\\\(.*동\\\\)$", "")
  ) %>% distinct() %>% 
  right_join(Total_POP2) %>% 
  mutate(
    NEW_BLD = ifelse(!is.na(TEMP_BLD), TEMP_BLD, NEW_BLD),
    NEW_DONG = ifelse(!is.na(TEMP_DONG), TEMP_DONG, NEW_DONG)
  ) %>% 
  select(-TEMP_DONG, -TEMP_BLD) %>% compy()

Total_POP2 %>% 
  select(
    NEW_DONG
  ) %>% distinct()

Total_POP2 %>% 
  filter(
    rlike(NEW_DONG, "^\\\\([가-힇]동\\\\)$")
  ) %>% select(NEW_DONG) %>% 
  distinct()

Total_POP2 = 
  Total_POP2 %>% 
  mutate(
    NEW_DONG = case_when(
      rlike(NEW_DONG, "^\\\\(비동\\\\)$")~ "B",
      rlike(NEW_DONG, "^\\\\(나동\\\\)$")~ "나",
      rlike(NEW_DONG, "^\\\\(가동\\\\)$")~ "가",
      TRUE ~NEW_DONG
    )
  ) %>% compy()

Total_POP2 %>% 
  filter(
    rlike(NEW_DONG, "^\\\\(명품하우스\\\\)")
  ) %>% select(NEW_BLD, NEW_DONG)

Total_POP2 = 
  Total_POP2 %>% 
  mutate(
    NEW_DONG = case_when(
      rlike(NEW_DONG, "^\\\\(명품하우스\\\\)")~ "",
      TRUE ~NEW_DONG
    )
  ) %>% compy()

Total_POP2 = 
  Total_POP2 %>% 
  filter(
    rlike(NEW_DONG, "[가-힇][^동](에이|비|비이|씨|시|디|이|에프|지|쥐|에이치|아이|제이|케이|엘|엠|엔|오|피|큐|알|에스|티|유|브이|더블유|엑스|와이|제트)동$")
  ) %>% 
  select(
    NEW_BLD, NEW_DONG
  ) %>% distinct() %>% 
  mutate(
    TEMP_BLD = regexp_replace(NEW_DONG, "(에이|비|비이|씨|시|디|이|에프|지|쥐|에이치|아이|제이|케이|엘|엠|엔|오|피|큐|알|에스|티|유|브이|더블유|엑스|와이|제트)동$", ""),
    TEMP_DONG = regexp_extract(NEW_DONG, "(에이|비|비이|씨|시|디|이|에프|지|쥐|에이치|아이|제이|케이|엘|엠|엔|오|피|큐|알|에스|티|유|브이|더블유|엑스|와이|제트)동$")
  ) %>% 
  mutate(
    TEMP_DONG = case_when(
      rlike(TEMP_DONG, '^에이$')~"A",
      rlike(TEMP_DONG, '^비$')~"B",
      rlike(TEMP_DONG, '^비이$')~"B",
      rlike(TEMP_DONG, '^씨$')~"C",
      rlike(TEMP_DONG, '^시$')~"C",
      rlike(TEMP_DONG, '^디$')~"D",
      rlike(TEMP_DONG, '^이$')~"E",
      rlike(TEMP_DONG, '^에프$')~"F",
      rlike(TEMP_DONG, '^쥐$')~"G",
      rlike(TEMP_DONG, '^지$')~"G",
      rlike(TEMP_DONG, '^에이치$')~"H",
      rlike(TEMP_DONG, '^아이$')~"I",
      rlike(TEMP_DONG, '^제이$')~"J",
      rlike(TEMP_DONG, '^케이$')~"K",
      rlike(TEMP_DONG, '^엘$')~"L",
      rlike(TEMP_DONG, '^엠$')~"M",
      rlike(TEMP_DONG, '^엔$')~"N",
      rlike(TEMP_DONG, '^오$')~"O",
      rlike(TEMP_DONG, '^피$')~"P",
      rlike(TEMP_DONG, '^큐$')~"Q",
      rlike(TEMP_DONG, '^알$')~"R",
      rlike(TEMP_DONG, '^에스$')~"S",
      rlike(TEMP_DONG, '^티$')~"T",
      rlike(TEMP_DONG, '^유$')~"U",
      rlike(TEMP_DONG, '^브이$')~"V",
      rlike(TEMP_DONG, '^더블유$')~"W",
      rlike(TEMP_DONG, '^엑스$')~"X",
      rlike(TEMP_DONG, '^와이$')~"Y",
      rlike(TEMP_DONG, '^제트$')~"Z",
      TRUE ~ TEMP_DONG
    )
  ) %>% right_join(Total_POP2) %>% 
  mutate(
    NEW_BLD = ifelse(!is.na(TEMP_BLD), TEMP_BLD, NEW_BLD),
    NEW_DONG = ifelse(!is.na(TEMP_DONG), TEMP_DONG, NEW_DONG)
  ) %>% 
  select(
    -TEMP_DONG, -TEMP_BLD
  ) %>% compy()

Total_POP2

Total_POP = readRDS("D:\\work\\POP20200618.rds") %>% setDT()

A = Sys.time()
Total_POP = 
  Total_POP %>% 
  filter(
    grepl("[가-힇][^동](에이|비|비이|씨|시|디|이|에프|지|쥐|에이치|아이|제이|케이|엘|엠|엔|오|피|큐|알|에스|티|유|브이|더블유|엑스|와이|제트)동$",DONG_NAME)
  ) %>% 
  select(
    BLD_NAME, DONG_NAME
  ) %>% distinct() %>% 
  mutate(
    TEMP_BLD = str_replace(DONG_NAME, "(에이|비|비이|씨|시|디|이|에프|지|쥐|에이치|아이|제이|케이|엘|엠|엔|오|피|큐|알|에스|티|유|브이|더블유|엑스|와이|제트)동$", ""),
    TEMP_DONG = str_extract(DONG_NAME, "(에이|비|비이|씨|시|디|이|에프|지|쥐|에이치|아이|제이|케이|엘|엠|엔|오|피|큐|알|에스|티|유|브이|더블유|엑스|와이|제트)동$") %>% str_remove_all("동$")
  ) %>% 
  mutate(
    TEMP_DONG = case_when(
      str_detect(TEMP_DONG, '^에이$')~"A",
      str_detect(TEMP_DONG, '^비$')~"B",
      str_detect(TEMP_DONG, '^비이$')~"B",
      str_detect(TEMP_DONG, '^씨$')~"C",
      str_detect(TEMP_DONG, '^시$')~"C",
      str_detect(TEMP_DONG, '^디$')~"D",
      str_detect(TEMP_DONG, '^이$')~"E",
      str_detect(TEMP_DONG, '^에프$')~"F",
      str_detect(TEMP_DONG, '^쥐$')~"G",
      str_detect(TEMP_DONG, '^지$')~"G",
      str_detect(TEMP_DONG, '^에이치$')~"H",
      str_detect(TEMP_DONG, '^아이$')~"I",
      str_detect(TEMP_DONG, '^제이$')~"J",
      str_detect(TEMP_DONG, '^케이$')~"K",
      str_detect(TEMP_DONG, '^엘$')~"L",
      str_detect(TEMP_DONG, '^엠$')~"M",
      str_detect(TEMP_DONG, '^엔$')~"N",
      str_detect(TEMP_DONG, '^오$')~"O",
      str_detect(TEMP_DONG, '^피$')~"P",
      str_detect(TEMP_DONG, '^큐$')~"Q",
      str_detect(TEMP_DONG, '^알$')~"R",
      str_detect(TEMP_DONG, '^에스$')~"S",
      str_detect(TEMP_DONG, '^티$')~"T",
      str_detect(TEMP_DONG, '^유$')~"U",
      str_detect(TEMP_DONG, '^브이$')~"V",
      str_detect(TEMP_DONG, '^더블유$')~"W",
      str_detect(TEMP_DONG, '^엑스$')~"X",
      str_detect(TEMP_DONG, '^와이$')~"Y",
      str_detect(TEMP_DONG, '^제트$')~"Z",
      TRUE ~ TEMP_DONG
    )
  ) %>% right_join(Total_POP) %>% 
  mutate(
    BLD_NAME = ifelse(!is.na(TEMP_BLD), TEMP_BLD, BLD_NAME),
    DONG_NAME = ifelse(!is.na(TEMP_DONG), TEMP_DONG, DONG_NAME)
  ) %>% 
  select(
    -TEMP_DONG, -TEMP_BLD
  )
Sys.time()-A
#














#

"2020-07-03 12:45:06 KST"

Total_POP2 = 
  Total_POP2 %>% 
  mutate(
    GROUP_FILTER = ifelse(((RESULT !='Comp') & (rlike(NEW_DONG, "^제[0-9]+동$|^제[A-Z]동$|^제(가|나|다|라|마|바|사|아|자|차|카|타|파|하)동$"))),1,0)) %>% 
  mutate(
    RESULT = ifelse(GROUP_FILTER ==1, "Comp", RESULT),
    NEW_DONG = ifelse(GROUP_FILTER ==1, regexp_replace(NEW_DONG, "^제", ""), NEW_DONG)
  ) %>% 
  select(-GROUP_FILTER) %>% compute('total_pop3')

sdf_nrow(Total_POP2 %>% filter(RESULT != "Comp"))


Total_POP2 = 
  Total_POP2 %>% 
  mutate(
    GROUP_FILTER = ifelse(((RESULT !='Comp') & (rlike(NEW_DONG, "^[가-힇]{4,} 제?[0-9]+동$"))),1,0)) %>% 
  mutate(RESULT = ifelse(GROUP_FILTER ==1, 'Comp', RESULT),
         NEW_DONG_TEMP = ifelse(GROUP_FILTER ==1, split(NEW_DONG, " "), split(NEW_DONG, " "))) %>%
  sdf_separate_column("NEW_DONG_TEMP", into = c('NEW_BLD2', 'NEW_DONG2')) %>%
  mutate(
    NEW_BLD = ifelse(GROUP_FILTER ==1, NEW_BLD2, ""),
    NEW_DONG = ifelse(GROUP_FILTER == 1, NEW_DONG2, NEW_DONG)
  ) %>% select(-GROUP_FILTER, -NEW_DONG2, -NEW_BLD2, -NEW_DONG_TEMP)

sdf_nrow(Total_POP2 %>% filter(RESULT != "Comp"))

"2020-07-03 13:20:26 KST"

Total_POP2 = 
  Total_POP2 %>% 
  mutate(
    GROUP_FILTER = ifelse(((RESULT !='Comp') & (rlike(NEW_DONG, "^[가-힇]{4,} [0-9]+동$"))),1,0)) %>% 
  mutate(RESULT = ifelse(GROUP_FILTER ==1, 'Comp', RESULT),
         NEW_DONG_TEMP = ifelse(GROUP_FILTER ==1, split(NEW_DONG, " "), split(NEW_DONG, " "))) %>%
  sdf_separate_column("NEW_DONG_TEMP", into = c('NEW_BLD2', 'NEW_DONG2')) %>%
  mutate(
    NEW_BLD = ifelse(GROUP_FILTER ==1, NEW_BLD2, ""),
    NEW_DONG = ifelse(GROUP_FILTER == 1, NEW_DONG2, NEW_DONG)
  ) %>% select(-GROUP_FILTER, -NEW_DONG2, -NEW_BLD2, -NEW_DONG_TEMP)

Total_POP2 %>% 
  filter(RESULT != 'Comp') %>% sdf_nrow()

spark_write_parquet(Total_POP2, 'temp.parquet', mode='overwrite')
Total_POP2 = spark_read_parquet(sc, path='temp.parquet', name='total_pop3', overwrite = TRUE)

Total_POP2 = 
  Total_POP2 %>% 
  mutate(
    GROUP_FILTER = ifelse(((RESULT !='Comp') & (rlike(NEW_DONG, "^[가-힇]{4,} 제?[A-Z]+동$"))),1,0)) %>% 
  mutate(RESULT = ifelse(GROUP_FILTER ==1, 'Comp', RESULT),
         NEW_DONG_TEMP = ifelse(GROUP_FILTER ==1, split(NEW_DONG, " "), split(NEW_DONG, " "))) %>%
  sdf_separate_column("NEW_DONG_TEMP", into = c('NEW_BLD2', 'NEW_DONG2')) %>%
  mutate(
    NEW_BLD = ifelse(GROUP_FILTER ==1, NEW_BLD2, ""),
    NEW_DONG = ifelse(GROUP_FILTER == 1, NEW_DONG2, NEW_DONG)
  ) %>% select(-GROUP_FILTER, -NEW_DONG2, -NEW_BLD2, -NEW_DONG_TEMP)

spark_write_parquet(Total_POP2, 'temp.parquet', mode='overwrite')
Total_POP2 = spark_read_parquet(sc, path='temp.parquet', name='total_pop3', overwrite = TRUE)

Total_POP2 %>% 
  filter(RESULT != 'Comp') %>% sdf_nrow()


Total_POP2 = 
  Total_POP2 %>% 
  mutate(
    GROUP_FILTER = ifelse(((RESULT !='Comp') & (rlike(NEW_DONG, "^[가-힇]{4,} [A-Z]+동$"))),1,0)) %>% 
  mutate(RESULT = ifelse(GROUP_FILTER ==1, 'Comp', RESULT),
         NEW_DONG_TEMP = ifelse(GROUP_FILTER ==1, split(NEW_DONG, " "), split(NEW_DONG, " "))) %>%
  sdf_separate_column("NEW_DONG_TEMP", into = c('NEW_BLD2', 'NEW_DONG2')) %>%
  mutate(
    NEW_BLD = ifelse(GROUP_FILTER ==1, NEW_BLD2, ""),
    NEW_DONG = ifelse(GROUP_FILTER == 1, NEW_DONG2, NEW_DONG)
  ) %>% select(-GROUP_FILTER, -NEW_DONG2, -NEW_BLD2, -NEW_DONG_TEMP) %>% compute()

Total_POP2 %>% 
  filter(RESULT != 'Comp') %>% sdf_nrow()

Total_POP3 = Total_POP2 %>% collect()


Total_POP2 = 
  Total_POP2 %>% dplyr::mutate(
    RESULT = case_when(
      RESULT!='Comp' & rlike(NEW_DONG, "^[0-9]+동$|^[A-Z]동$|^(가|나|다|라|마|바|사|아|자|차|카|타|파|하)동$") ~ 'Comp'#,
      #RESULT=='Comp' & rlike(NEW_DONG, "^[0-9]+동$|^[A-Z]동$|^(가|나|다|라|마|바|사|아|자|차|카|타|파|하)동$") ~ NEW_DONG
    )
  ) %>% compute('tempdb')

dbplyr::sql_render(Total_POP2)


Total_POP %>% dplyr::filter(
  grepl("\\(.*(길|로).*[0-9]\\)", DONG_NAME)
) %>% dplyr::mutate(
  DONGS = gsub("\\(.*(길|로).*[0-9]\\)", "", DONG_NAME)
) %>% dplyr::select(
  DONGS
) %>% nrow()


#하이브에서 특수문자는 대괄호를 한번 더 넣어줘야 함.

Total_POP2 %>% dplyr::filter(
  rlike(DONG_NAME,"[\\(].*(길|로).*[0-9][\\)]")
) %>% dplyr::mutate(
  DONGS = regexp_replace(DONG_NAME, "[\\(].*(길|로).*[0-9][\\)]", "")
) %>% dplyr::select(
  DONGS
) %>% sdf_nrow()


Total_POP2 %>% 
  mutate(
    TEST_DONG = case_when(
      RESULT!='Comp' & rlike(NEW_DONG,"^[가-힇]{4,} 제?[0-9]+동$") ~split(NEW_DONG, " ")
    )
  ) %>% filter(!is.na(TEST_DONG)) %>% 
  select(TEST_DONG) %>% 
  mutate(EXP = explode(TEST_DONG))


Total_POP2 %>% 
  filter(rlike(NEW_DONG,"^[가-힇]{4,} 제?[0-9]+동$"))


tms = copy_to(sc, data.frame(A = c("아파트 가동", "아파트")), 'test_copy', overwrite = TRUE)




# spark://DESKTOP-NSKBCK8.localdomain:7077
