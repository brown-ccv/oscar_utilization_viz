library(ggplot2)

setwd("~/projects/oscar_utilization_viz/")

dat <- read.csv("data/daily_active_users.csv")
dat$day <- as.Date(dat$day)


ggplot(dat, aes(x = day, y = n_users)) +
  geom_line(colour = "purple") +
  stat_smooth() +
  ggtitle("Oscar Daily-Active Users") +
  xlab("Date") +
  ylab("Number of Users Running Jobs on Oscar")
  