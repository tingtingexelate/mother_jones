setwd('~/mojo_data/')

files <- list.files(pattern ='tsv')

library(data.table)
dat <- list()

for (f in seq(length(files))){
  dat[[f]] <- data.frame(fread(files[f]))
}
names(dat) <- unlist(sapply(files, function(x) substring(x, 1, nchar(x)-4)))

dat_summary <- function(df){
  
  summary_results <- do.call('rbind', lapply(1:ncol(df), function(x){
    
    return(data.frame(total_records = length(df[,x]),
                      total_unique_records = length(unique(df[,x])),
                      total_empty_records = sum(df[,x] == ""),
                      total_NA_records = sum(is.na(df[,x])),
                      total_zeros = sum(df[,x] ==0)))}))  
  
  
  
  rownames(summary_results) <- colnames(df)
  return(summary_results)
}

list_summary <- lapply(dat, function(x) dat_summary(x))
df_summary <- data.frame(do.call('rbind',list_summary))
df_summary$tablename <- sapply(rownames(df_summary), function(x) unlist(strsplit(x,'\\.'))[1])
df_summary$colname <- sapply(rownames(df_summary), function(x) unlist(strsplit(x,'\\.'))[2])
write.csv(df_summary[c(6,7,1:5)], file ='list_summary.tsv',sep='\t',quote = F)

"wp_postmeta"           "wp_posts"              "wp_taxonomy"           "wp_termmeta"           "wp_term_relationships" "wp_terms" "wp_users"             "wp_users"

##################### wp_posts #####################

wp_posts <- dat[['wp_posts']]
wp_postmeta <- dat[['wp_postmeta']]
wp_posts$yyyymmdd <- as.Date(wp_posts$post_date)
library(sqldf)

head(wp_posts[,1:4])

nposts_by_author <- sqldf("select post_author, count(*), count(distinct ID) cnt from wp_posts 
                           group by 1
                           order by 3 desc")

nposts_by_author$pct <- nposts_by_author$cnt/sum(nposts_by_author$cnt)

summary(nposts_by_author[3:nrow(nposts_by_author), 'cnt'])

min(as.Date(wp_posts$post_date[wp_posts$post_date != '']))

nposts_by_time <- sqldf("select post_date, count(*), count(distinct ID) cnt from wp_posts 
                         group by 1
                         order by 3 desc")
library(ggplot2)
nposts_by_time$pct <- nposts_by_time$cnt/sum(nposts_by_time$cnt)
nposts_by_time$pdate <- as.Date(nposts_by_time$post_date)
nposts_by_time$pyear <- format(nposts_by_time$pdate,'%Y%m')
nposts_by_time$pmonth <- format(nposts_by_time$pdate,'%m')
nposts_by_time$pyearmonth <- format(nposts_by_time$pdate,'%Y%m')

require(dplyr)
nposts_by_time <- nposts_by_time[!is.na(nposts_by_time$pdate),]

cnt_by_date <- unlist(lapply(split(nposts_by_time, nposts_by_time$pdate), function(x) sum(x$cnt)))
cnt_by_date <- data.frame(year = names(cnt_by_date),
                          cnt = as.numeric(cnt_by_date))

cnt_by_year <- unlist(lapply(split(nposts_by_time, nposts_by_time$pyear), function(x) sum(x$cnt)))
cnt_by_year <- data.frame(year = names(cnt_by_year),
                          cnt = as.numeric(cnt_by_year))

cnt_by_month <- unlist(lapply(split(nposts_by_time, nposts_by_time$pmonth), function(x) sum(x$cnt)))
cnt_by_month <- data.frame(year = names(cnt_by_month),
                          cnt = as.numeric(cnt_by_month))


cnt_by_yearmonth <- unlist(lapply(split(nposts_by_time, nposts_by_time$pyearmonth), function(x) sum(x$cnt)))
cnt_by_yearmonth <- data.frame(yymm = names(cnt_by_yearmonth),
                               cnt = as.numeric(cnt_by_yearmonth),stringsAsFactors = F)
cnt_by_yearmonth$yymm <- as.Date(paste0(cnt_by_yearmonth$yymm,"01"), "%Y%m%d") 


ggplot(data = cnt_by_yearmonth, aes(x = yymm, y = cnt, group = 1))+
  geom_point() +
  geom_line() +
  scale_x_date(date_breaks = "3 years", date_labels = "%Y-%m")+
  labs(x = "Year Month", y = "# posts", 
       title = "Number of posts by year and month")


ggplot(data = cnt_by_yearmonth[-c(1,292),], aes(x = yymm, y = cnt, group = 1))+
  geom_point() +
  geom_line() +
  scale_x_date(date_breaks = "3 years", date_labels = "%Y-%m")+
  labs(x = "Year Month", y = "# posts", 
       title = "Number of posts by year and month without outliers")

http://www.sthda.com/english/articles/32-r-graphics-essentials/128-plot-time-series-data-using-ggplot/
  
  
wp_posts <- dat[['wp_posts']]
wp_postmeta <- dat[['wp_postmeta']]
wp_posts$yyyymmdd <- as.Date(wp_posts$post_date)  

wp_posts_empty <- wp_posts[wp_posts$post_date == '',]
wp_posts_nonempty <- wp_posts[wp_posts$post_date != '',]

wp_posts_700101 <- wp_posts_nonempty[format(wp_posts_nonempty$yyyymmdd,'%Y%m%d') == '19700101',]
sqldf("select post_status,post_type,  count(distinct ID) from wp_posts_700101 group by 1,2 order by 1,2")

post_status           post_type count(distinct ID)
1       draft   jp_sitemap_master                  1
2     publish       nav_menu_item                 17
3     publish       redirect_rule                  1
4     publish vip-legacy-redirect              24667

wp_posts_170628 <- wp_posts_nonempty[format(wp_posts_nonempty$yyyymmdd,'%Y%m%d') == '20170628',]
sqldf("select post_status,post_type,  count(distinct ID) from wp_posts_170628 group by 1,2 order by 1,2")

post_status           post_type count(distinct ID)
1       draft        guest-author               2241
2       draft          jp_sitemap                  5
3       draft                post                  1
4     publish            feedback                  1
5     publish        guest-author                  1
6     publish                post                 22
7     publish       redirect_rule                  1
8     publish vip-legacy-redirect              21840  



t<- sqldf("select post_status, post_type,  count(distinct ID) cnt_ID
       from wp_posts group by 1,2 order by 1,2")


wp_posts <- dat[['wp_posts']]
wp_postmeta <- dat[['wp_postmeta']]
wp_posts$pdate<- as.Date(wp_posts$post_date)
wp_posts$pyearmonth <- format(wp_posts$pdate,'%Y%m')


nposts_by_time <- sqldf("select post_type, pyearmonth, count(distinct ID) cnt from wp_posts 
                        where post_status = 'publish' and post_type in ('vip-legacy-redirect','post')
                        group by 1,2
                        order by 1,2 desc")
library(ggplot2)

nposts_by_time$yymm <- as.Date(paste0(nposts_by_time$pyearmonth,"01"), "%Y%m%d") 


ggplot(data = nposts_by_time, aes(x = yymm, y = cnt, group = 1))+
  geom_point(aes(color = post_type), size = 1) + 
  scale_x_date(date_breaks = "4 years", date_labels = "%Y-%m")+
  labs(x = "Year Month", y = "# posts", 
       title = "Number of posts by month")

nposts_by_time <- sqldf("select post_type, pyearmonth, count(distinct ID) cnt from wp_posts 
                        where post_status = 'publish' and post_type in ('post')
                        group by 1,2
                        order by 1,2 desc")
library(ggplot2)

nposts_by_time$yymm <- as.Date(paste0(nposts_by_time$pyearmonth,"01"), "%Y%m%d") 


ggplot(data = nposts_by_time, aes(x = yymm, y = cnt, group = 1))+
  geom_point() + 
  geom_line(aes(color = post_type), size = 1) + 
  scale_x_date(date_breaks = "5 years", date_labels = "%Y-%m")+
  labs(x = "Year Month", y = "# posts", 
       title = "Number of posts by month")





wp_posts_publish_post <- wp_posts[wp_posts$post_status == 'publish' & wp_posts$post_type %in% c('post'),]

dim(wp_posts_publish_post)
# 71502    25


nposts_by_author <- sqldf("select post_author, count(distinct ID) cnt, min(post_date) min_date,max(post_date) max_date from wp_posts_publish_post 
                           group by 1
                           order by 2 desc")

nposts_by_author$pct <- nposts_by_author$cnt/sum(nposts_by_author$cnt)

dim(nposts_by_author)

221 authors 41.2% of them only post once; but author 0 posted 58791 (82.2%) also whats author 1??

nposts_by_author$datediff <- as.Date(unlist(sapply(nposts_by_author$max_date,function(x) unlist(strsplit(x,' '))[1])), '%Y-%m-%d') - as.Date(unlist(sapply(nposts_by_author$min_date,function(x) unlist(strsplit(x,' '))[1])), '%Y-%m-%d')

summary(nposts_by_author[, 'cnt'])

Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
1.0     1.0     2.0   323.5    23.0 58790.0 

summary(nposts_by_author[!nposts_by_author$post_author %in% c(0,128096,130286), 'cnt'])

Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
1.00    1.00    2.00   28.23   21.75  623.00 

summary(as.numeric(nposts_by_author$datediff))
Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
0.0     0.0   123.0   648.4   495.0 15000.0 

summary(as.numeric(nposts_by_author[nposts_by_author$post_author !=0 & nposts_by_author$cnt>1,]$datediff))
Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
0.0   189.0   454.0   996.6  1051.0 15000.0

library(sqldf)
nauthors_bymonth <- sqldf("select  pyear, count(distinct post_author) cnt from wp_posts_publish_post 
                           group by 1
                           order by 1 ")



wp_posts <- dat[['wp_posts']]
wp_posts_publish_post <- wp_posts[wp_posts$post_status == 'publish' & wp_posts$post_type %in% c('post'),]

wp_postmeta <- dat[['wp_postmeta']]
wp_term_relationships <- dat[['wp_term_relationships']]
wp_taxonomy <- dat[['wp_taxonomy']]
wp_terms <- dat[['wp_terms']]
wp_posts$yyyymmdd <- as.Date(wp_posts$post_date)  
wp_users <- dat[['wp_users']]


wp_terms_combo <- merge(wp_taxonomy, wp_terms, by.x = 'term_id', by.y = 'term_id')
wp_terms_combo <- merge(wp_terms_combo, wp_term_relationships, by.x = 'term_taxonomy_id', by.y = 'term_taxonomy_id')


wp_posts_terms <- merge(wp_posts,wp_terms_combo, by.x = 'ID', by.y = 'object_id')

library(sqldf)
nposts_by_terms <- sqldf("select name, count(distinct ID) cnt from wp_posts_terms 
                            where term_group  = 1
                            group by 1
                            order by 2 desc")
nposts_by_terms$pct <- nposts_by_terms$cnt/sum(nposts_by_terms$cnt)
wordcloud(words = nposts_by_terms$name, freq = nposts_by_terms$cnt, min.freq = 1 ,
          max.words=100, random.order=FALSE, rot.per=0.35, 
          colors=brewer.pal(8, "Dark2"), main ='10k posts')

# load packages
library(RCurl)
library(XML)
library(parallel)
wp_posts_publish_post$post_content_txt <- unlist(mclapply(wp_posts_publish_post$post_content, function(x) {
                                                              
                                                              if (x!= "") {
                                                              doc = htmlParse(x, asText=TRUE)
                                                              plain.text <- xpathSApply(doc, "//p", xmlValue)
                                                              plain.text <- plain.text[plain.text!='']
                                                              plain.text <- gsub('\\"','',plain.text)
                                                              # pattern <- "\\s]+))?)+\\s*|\\s*)/?>"
                                                              # plain.text <- gsub(pattern, "\\1", plain.text)
                                                              plain.text <- paste0(plain.text[2:length(plain.text)], collapse = ' ')
                                                              library(stringr)
                                                              plain.text <- str_replace_all(plain.text, "[[:punct:]]", " ")
                                                              plain.text <- gsub("[^[:alnum:][:blank:]+?&/\\-]", "", plain.text)
                                                              plain.text <- gsub("000", "", plain.text)
                                                              plain.text <- gsub("\t", "", plain.text)
                                                              plain.text <- gsub("+", "", plain.text)
                                                              plain.text <- tolower(plain.text)
                                                              }else{
                                                                
                                                                plain.text <- ''
                                                              }
                                                              return(plain.text)
                                                            },mc.cores = 3))



library(stopwords)
stopwords_list <- stopwords(language = "en")


wp_posts_publish_post$num_char <- sapply(wp_posts_publish_post$post_content_txt, function(x) nchar(x))
words_cnt <- mclapply(wp_posts_publish_post$post_content_txt, function(t) {
  
  t <- unlist(strsplit(t,' '))
  # # remove numbers
  # t  <- gsub('[[:digit:]]+', '',t)
  t <- t[!t %in%  c('+','0','+0',letters,1:10)]
  t <- tolower(t)
  return(data.frame(num_words = length(t),
                    num_unique_words = length(unique(t)),
                    num_unique_nonstopwords = length(unique(t[!t%in%stopwords_list]))))
  
}, mc.cores = 3)
words_cnt <- do.call('rbind',words_cnt)

wp_posts_publish_post$num_words  <- words_cnt$num_words
wp_posts_publish_post$num_unique_words  <- words_cnt$num_unique_words
wp_posts_publish_post$num_unique_nonstopwords  <- words_cnt$num_unique_nonstopwords

summary(wp_posts_publish_post$num_words[wp_posts_publish_post$num_words >0 ])
summary(wp_posts_publish_post$num_unique_nonstopwords[wp_posts_publish_post$num_unique_nonstopwords>0])
summary(wp_posts_publish_post$num_unique_words[wp_posts_publish_post$num_unique_words>0])

> summary(wp_posts_publish_post$num_words[wp_posts_publish_post$num_words >0 ])
Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
1.0   162.0   346.0   593.1   666.0 37700.0 

> summary(wp_posts_publish_post$num_unique_words[wp_posts_publish_post$num_unique_words>0])
Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
1.0   102.0   186.0   251.3   311.0  5302.0 
> summary(wp_posts_publish_post$num_unique_nonstopwords[wp_posts_publish_post$num_unique_nonstopwords>0])
Min. 1st Qu.  Median    Mean 3rd Qu.    Max. 
1.0    74.0   142.0   206.8   252.0  5189.0 


all_words <- mclapply(wp_posts_publish_post$post_content_txt, function(t) {
  
  t <- unlist(strsplit(t,' '))
  # # remove numbers
  # t  <- gsub('[[:digit:]]+', '',t)
  t <- t[t!= '']
  t <- tolower(t)
  t <- t[!t %in% c(stopwords_list,c('+','0','+0',letters,as.character(1:10)))]
  return(t)
}, mc.cores = 3)

# all_words_freq <- mclapply(all_words, function(x){ return(data.frame(words = names(table(x)),
#                                                         freq = as.numeric(table(x))))}, mc.cores = 3)
# 
# all_words_freq <- do.call('rbind',all_words_freq)

all_words_sample <- Reduce(c,all_words[sample(1:length(all_words),10000)])

all_words_sample_freq <- data.frame(words = names(table(all_words_sample)),
                                    freq =as.numeric(table(all_words_sample)))

library(wordcloud)


all_words_sample_freq <- all_words_sample_freq[!all_words_sample_freq$words %in% c('s','t','can','one','just',c('+','0','+0',letters,1:10)),]
wordcloud(words = all_words_sample_freq$words, freq = all_words_sample_freq$freq, min.freq = 10 ,
          max.words=100, random.order=FALSE, rot.per=0.35, 
          colors=brewer.pal(8, "Dark2"), main ='10k posts')

wp_posts_publish_post$pyear <- format(as.Date(wp_posts_publish_post$post_date),'%Y')

years <- list(obama1 = c(2008:2011),
              obama2 = c(2012:2015),
              trump = c(2016:2018))

for (i in seq(length(years))){
  
  print(names(years)[i])
  all_words_sample <- Reduce(c,all_words[wp_posts_publish_post$pyear%in% years[[i]]][sample(1:sum(wp_posts_publish_post$pyear%in% years[[i]]),10000)])
  all_words_sample_freq <- data.frame(words = names(table(all_words_sample)),
                                      freq =as.numeric(table(all_words_sample)))
  all_words_sample_freq <- all_words_sample_freq[!all_words_sample_freq$words %in% c('s','t','can','one','just',c('+','0','+0',letters,1:10)),]
  wordcloud(words = all_words_sample_freq$words, freq = all_words_sample_freq$freq, min.freq = 10 ,
            max.words=100, random.order=FALSE, rot.per=0.35, 
            colors=brewer.pal(8, "Dark2"), main ='10k posts')
}



