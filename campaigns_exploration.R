setwd('~/mojo/email click data - oct 1-31/')

# before installing uaparserjs
# sudo yum install epel-release
# sudo yum install v8-devel
# in R, install.packages("uaparserjs")
# challenge:
# user agent data all lower cased, user parser can't work properly. a workaround is to captalize each word


library(uaparserjs)
library(data.table)

files <- list.files('.', pattern ='csv')

dat <- lapply(files, function(x) data.frame(fread(x, sep=',',head=T)))
names(dat) <- files

append_attrs <- function(df){
  
  # this function will append extra information to the original data, including
  # parse Url to get the site
  # parse timestamp to get the date
  # parse user agent data to get browser and os
  # flag records that are bad, internal, image or subscription 
   
  
  # parse Url to get the site
  df$domain <- sapply(df$Url, function(x) {
    
    domains = unlist(strsplit(x,'/'))
    domains = domains[domains != '']
    return(domains[2])
  })

  # get the date in YYYY-MM-DD format 
  df$date <- sapply(df$Recorded.On, function(x) substring(x,1, 10))
  ua <- do.call('rbind',lapply(df$Browser, function(x) {
    
    temp <- ua_parse(gsub("(^|[[:space:]])([[:alpha:]])", "\\1\\U\\2", gsub('\\(','\\( ',x), perl=TRUE))
    browser <- temp$ua.family
    os      <- temp$os.family
    return(data.frame(browser =browser, os =os ))
  }))

  df$browser <- ua$browser
  df$os      <- ua$os
  
  df$is_bad_records <- df$Url %in% c('Url','mailto:recharge@motherjones.com')
  df$is_internal_records <- grepl('li.motherjones.com', df$Url)
  df$is_image_records   <-  grepl('img src=', df$Url)
  df$is_donation_DON_records <- grepl('secure.motherjones.com', df$Url) & grepl('pub_code=DON', df$Url)
  df$is_donation_SDN_records <- grepl('secure.motherjones.com', df$Url) & grepl('pub_code=SDN', df$Url)
  df$is_subscription_records <- grepl('secure.motherjones.com', df$Url) & grepl('pub_code=MJM', df$Url)
  
  
  return(df)
  
}


###################### append extra info to the dat ####################
# this process will append extra information to the original data, including
# parse Url to get the site
# parse timestamp to get the date
# parse user agent data to get browser and os
# flag records that are bad, internal, image or subscription 
########################################################################
library(parallel)
appended_dat <- mclapply(dat, function(x) append_attrs(x), mc.cores = 6)



######################## Look through campaigns to get aggregated info ##### ##### #####
#### this return a list of summaries, including:
## high-level counts of urls, emails, etc
## breakdown counts like # urls by email, # urls by browser, # urls by date, etc
##################################################################################### #####
summary_results <- vector(length(dat), mode = 'list')

library(sqldf)

for (i in 1: length(dat)){
  print(' ############################################')
  print(paste0('=====', names(appended_dat)[i],'======='))
  df <- appended_dat[[i]]
  df$Browser <- NULL
  df$weekdays <- weekdays(as.Date(df$date))
  

  summary_results[[i]]$ovarall_summary <- data.frame(nclicks = nrow(df),
                                                     nariticle_clicks = nrow(df[rowSums(df[, c('is_bad_records','is_internal_records','is_image_records','is_donation_DON_records','is_donation_SDN_records','is_subscription_records')])==0,]),
                                                     nurls   = length(unique(df$Url)),
                                                     nbad_urls = length(unique(df$Url[df$is_bad_records])),
                                                     ninternal_urls = length(unique(df$Url[df$is_internal_records])),
                                                     nimage_urls = length(unique(df$Url[df$is_image_records])),
                                                     ndon_urls = length(unique(df$Url[df$is_donation_DON_records])),
                                                     nsdn_urls = length(unique(df$Url[df$is_donation_SDN_records])),
                                                     nsub_urls = length(unique(df$Url[df$is_subscription_records])),
                                                     nemail   = length(unique(df$Email)),
                                                     ndonors  = length(unique(df$Email[df$is_donation_DON_records | df$is_donation_SDN_records])),
                                                     nsubscriber  = length(unique(df$Email[df$is_subscription_records])),
                                                     nbrower = length(unique(df$browser)),
                                                     nos = length(unique(df$os)),
                                                     ndomain = length(unique(df$domain)),
                                                     ndates = length(unique(df$date)))
  
  donors <- unique(df$Email[df$is_donation_DON_records | df$is_donation_SDN_records])
  
  summary_results[[i]]$donors <- donors
  
  df <- df[rowSums(df[, c('is_bad_records',
                          'is_internal_records',
                          'is_image_records',
                          'is_donation_DON_records',
                          'is_donation_SDN_records',
                          'is_subscription_records')])==0,]
  
  df_donors_only <- df[df$Email %in% donors,]

  print(paste0(' ---- Break Down by url usage ------'))
    url_cnts <- sqldf("select Email, count(Url) n_urls, count(distinct url) n_unique_urls from df 
                       group by Email")
  
  summary_results[[i]]$url_count        <- url_cnts
  summary_results[[i]]$url_distribution <- summary(url_cnts$n_urls)
  summary_results[[i]]$url_unique_distribution <- summary(url_cnts$n_unique_urls)
  
  donor_url_cnts <- sqldf("select Email, count(Url) n_urls, count(distinct url) n_unique_urls from df_donors_only 
                       group by Email")
  
  summary_results[[i]]$donor_url_cnts        <- donor_url_cnts
  summary_results[[i]]$donor_url_distribution <- summary(donor_url_cnts$n_urls)
  summary_results[[i]]$donor_url_unique_distribution <- summary(donor_url_cnts$n_unique_urls)
  
  email_cnts <- sqldf("select Url,  count(distinct Email) n_unique_Email from df 
                       group by Url
                       order by 2 desc")
  
  summary_results[[i]]$email_cnts        <- email_cnts
  summary_results[[i]]$email_unique_distribution <- summary(email_cnts$n_unique_Email)
  
  
  
  print(paste0(' ---- Break Down by domain ------'))
  domain_cnts <- sqldf(" select domain, count(distinct Url) n_urls from df 
                        group by domain
                        order by 2 desc;")
  summary_results[[i]]$domain_count        <- domain_cnts

  
  print(paste0(' ---- Break Down by date ------'))
  date_cnts <- sqldf(" select date, count(distinct Url) n_urls from df 
                        group by date
                        order by 1;")

  summary_results[[i]]$date_count <- date_cnts
  
  weekday_cnts <- sqldf(" select weekdays, count(distinct Url) n_urls from df 
                        group by weekdays
                        order by 1;")
  summary_results[[i]]$weekday_cnts <- weekday_cnts
  print(paste0(' ---- Break Down by family ------'))
  browser_cnts <- sqldf("  select browser, count(distinct Url) n_urls from df 
                           group by browser
                           order by 2 desc;")
}

names(summary_results) <- gsub('_data_results.csv','',names(dat))


###################### ########################### ########################### ########################### #####
############################### Use aggregated info to get presentable summaries #################################
###################### ########################### ########################### ########################### ########
## overall counts
overall_counts <- as.data.frame(do.call('rbind',lapply(summary_results, function(x) x$ovarall_summary)))
write.csv(overall_counts, file ='overall_counts.csv',quote=F)

##  nurls / email summary
nurls_by_email <- as.data.frame(do.call('rbind',lapply(summary_results, function(x) x$url_unique_distribution)))
write.csv(nurls_by_email, file ='nurls_by_email.csv',quote=F)


nurls_donor_by_email <- as.data.frame(do.call('rbind',lapply(summary_results, function(x) x$donor_url_unique_distribution)))
write.csv(nurls_donor_by_email, file ='nurls_donor_by_email.csv',quote=F)


##  email / url summary
nemail_by_url <- as.data.frame(do.call('rbind',lapply(summary_results, function(x) x$email_unique_distribution)))

domain_summary <- lapply(summary_results, function(x) {
  
  temp <-  x$domain_count
  temp$cumpct <- cumsum(temp$n_urls/sum(temp$n_urls))
  
  mojo     <- temp[temp$domain =='www.motherjones.com','n_urls']
  others   <- sum(temp[temp$domain !='www.motherjones.com','n_urls'])
  top90pct <- paste0(temp[temp$cumpct<=0.9,'domain'], collapse = '||')
  
  
  return(data.frame(mojo  = mojo/sum(temp$n_urls),
                    other = others/sum(temp$n_urls),
                    num_sites = nrow(temp),
                    top90pct = top90pct))
  
  })
write.csv(do.call('rbind',domain_summary), file ='domain_summary.csv',quote=F)


weekday_cnts <- as.data.frame(do.call('cbind',lapply(summary_results, function(x) x$weekday_cnts)))
write.csv(nurls_by_email, file ='nurls_by_email.csv',quote=F)


###################### ########################### ########################### ########################### #####
###############################  Combine all the campaigns into one and do an overall summary ###################
###################### ########################### ########################### ########################### #######

packed_dat <- do.call('rbind',appended_dat)
packed_dat <- packed_dat[rowSums(df[, c('is_bad_records',
                                         'is_internal_records',
                                         'is_image_records',
                                         'is_donation_DON_records',
                                         'is_donation_SDN_records',
                                         'is_subscription_records')])==0,]
packed_dat$Browser <- NULL
packed_dat_doners <- packed_dat[packed_dat$Email %in% Reduce(c,sapply(summary_results, function(x) x$donors)),]
packed_dat_doners$Browser <- NULL




print(paste0(' ---- Break Down by url usage ------'))
url_cnts <- sqldf("select Email, count(Url) n_urls, count(distinct url) n_unique_urls from packed_dat 
                    group by Email")

url_unique_distribution <- summary(url_cnts$n_unique_urls)

donor_url_cnts <- sqldf("select Email, count(Url) n_urls, count(distinct url) n_unique_urls from packed_dat_doners 
                       group by Email")
donor_url_unique_distribution <- summary(donor_url_cnts$n_unique_urls)


