## Import packages

library(tseries) # Time series package
library('ggplot2')
library('forecast')

summary(prod_master_111)

prod_master_111 <- subset(prod_master_111, select = -c(week_end,week_st))
prod_master_111$DATE = as.Date(paste(2001, prod_master_111$WEEK, 1, sep="-"), "%Y-%U-%u")


ggplot(prod_master_111, aes(DATE, UNITS)) + geom_line() + scale_x_date('week')  + ylab("Weekly Sales") +
  xlab("") +

prod_hunt <- prod_master_111[prod_master_111$L5 == 'HUNTS',]
prod_other <- prod_master_111[prod_master_111$L5 != 'HUNTS',]
  
sales_ts = ts(prod_hunt[, c('DOLLARS')])
prod_hunt$clean_sales = tsclean(sales_ts)

ggplot() +
  geom_line(data = prod_hunt, aes(x = DATE, y = clean_sales)) + ylab('Weekly sales')

prod_hunt$cnt_ma = ma(prod_hunt$clean_sales, order=7) # using the clean count with no outliers
daily_data$cnt_ma30 = ma(daily_data$clean_cnt, order=30)

count_ma = ts(na.omit(prod_hunt$cnt_ma), frequency=30)
decomp = stl(count_ma, s.window="periodic")
deseasonal_cnt <- seasadj(decomp)
plot(decomp)



