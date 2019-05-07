/*
SAS Project
Brand Selected: HUNTS (Rank 6 by Market Share)
Group 9: 
Nitansh , 
Namratha , 
Spandhan 
Tuesday Batch
*/

/*Importing Data Files*/

DATA work.groc;
INFILE 'E:/Namratha/spagsauc/spagsauc/spagsauc_groc_1114_1165' DLM=' ' FIRSTOBS=2;
INPUT IRI_KEY WEEK SY GE VEND ITEM UNITS DOLLARS F $ D PR; 
RUN;

DATA work.drug;
INFILE 'E:/Namratha/spagsauc/spagsauc/spagsauc_drug_1114_1165' DLM=' ' FIRSTOBS=2;
INPUT IRI_KEY WEEK SY GE VEND ITEM UNITS DOLLARS F $ D PR; 
RUN;

DATA work.del_store;
INFILE 'H:/Delivery_Stores' DLM=' ' FIRSTOBS=2;
INPUT IRI_KEY OU $ EST_ACV  Market_Name $ Open Clsd MskdName $; 
RUN;

/**/
PROC SORT DATA=work.groc;
by SY GE VEND ITEM;
RUN;

PROC SORT DATA=work.drug;
by SY GE VEND ITEM;
RUN;
data work.groc_drug;
set work.groc work.drug;
run;
/*Merging All Store-Product level files*/
proc sql;
create table work.brand_txn as 
select  spag.IRI_KEY as IRI_KEY,
spag.WEEK as WEEK,
spag.SY as SY,
spag.GE as GE,
spag.VEND as VEND,
spag.ITEM as ITEM,
spag.UNITS as UNITS,
spag.DOLLARS as DOLLARS,
spag.F as F,
spag.D as D,
spag.PR as PR,
prod.L4 as L4,
prod.L5 as L5,
prod.VOL_EQ as VOL_EQ,
prod.PRODUCT_TYPE as PRODUCT_TYPE,
prod.FLAVOR_SCENT as FLAVOR_SCENT,
prod.ADDITIVES as ADDITIVES,
prod.TYPE_OF_ITALIAN_SCE as TYPE_OF_ITALIAN_SCE,
prod.STYLE as STYLE,
prod.CONSISTENCY as CONSISTENCY,
prod.HEAT_LEVEL as HEAT_LEVEL 
from work.groc_drug as spag left join work.prod as prod 
on input(prod.SY,2.)= spag.SY and	input(prod.GE, 2.)=spag.GE and input(prod.VEND, 7.) = spag.VEND and	input(prod.ITEM, 7.)= spag.ITEM;
quit;

proc sql;
create table brand_txn1 as 
select 
brand.IRI_KEY as IRI_KEY,
brand.WEEK as WEEK,
brand.SY as SY,
brand.GE as GE,
brand.VEND as VEND,
brand.ITEM as ITEM,
brand.UNITS as UNITS,
brand.DOLLARS as DOLLARS,
brand.F as F,
brand.D as D,
brand.PR as PR,
brand.L4 as L4,
brand.L5 as L5,
brand.VOL_EQ as VOL_EQ,
brand.PRODUCT_TYPE as PRODUCT_TYPE,
brand.FLAVOR_SCENT as FLAVOR_SCENT,
brand.ADDITIVES as ADDITIVES,
brand.TYPE_OF_ITALIAN_SCE as TYPE_OF_ITALIAN_SCE,
brand.STYLE as STYLE,
brand.CONSISTENCY as CONSISTENCY,
brand.HEAT_LEVEL as HEAT_LEVEL,
week.Calendar_week_starting_on as week_st,
week.Calendar_week_ending_on as week_end 
from work.brand_txn as brand left join work.week as week 
on brand.WEEK = week.IRI_WEEK;
quit;

proc print data = work.brand_txn2(obs=30);where week_end is not missing ;run;

/*Master Product - Store Txn data file (6million+ rows)*/
proc sql;
create table brand_txn2 as 
select  
brand.IRI_KEY as IRI_KEY,
brand.WEEK as WEEK,
brand.SY as SY,
brand.GE as GE,
brand.VEND as VEND,
brand.ITEM as ITEM,
brand.UNITS as UNITS,
brand.DOLLARS as DOLLARS,
brand.F as F,
brand.D as D,
brand.PR as PR,
brand.L4 as L4,
brand.L5 as L5,
brand.VOL_EQ as VOL_EQ,
brand.PRODUCT_TYPE as PRODUCT_TYPE,
brand.FLAVOR_SCENT as FLAVOR_SCENT,
brand.ADDITIVES as ADDITIVES,
brand.TYPE_OF_ITALIAN_SCE as TYPE_OF_ITALIAN_SCE,
brand.STYLE as STYLE,
brand.CONSISTENCY as CONSISTENCY,
brand.HEAT_LEVEL as HEAT_LEVEL,
brand.week_st as week_st,
brand.week_end as week_end,
lu.OU as OU,
lu.EST_ACV as EST_ACV,
lu.Market_Name as Market_Name,
lu.Open as Open,
lu.Clsd as Clsd,
lu.MskdName as MskdName 
from work.brand_txn1 as brand left join work.del_store as lu 
on brand.IRI_KEY = lu.IRI_KEY;
quit;

proc sql ;
create table q1 as 
select  L5 as  brand, sum(dollars) as sales_dollar 
from work.brand_txn
group by brand 
order by sales_dollar desc;
quit;
proc print data = work.q1;run;
/*Master Product - Store Txn data file (TOP 20 brands - 5million+ rows)*/
DATA work.brand_txn3;
SET work.brand_txn2;
where L5 IN ('PREGO','CLASSICO','RAGU OLD WORLD STYLE','RAGU','FIVE BROTHERS','HUNTS','PRIVATE LABEL','RAGU CHUNKY GARDEN STYLE','BARILLA','FRANCESCO RINALDI','PREGO CHUNKY GARDEN','NEWMANS OWN','RAGU HEARTY','RAGU CHEESE CREATIONS','DEL MONTE','RAGU ROBUSTO','PROGRESSO','HEALTHY CHOICE','AUNT MILLIES','CLASSICO CREATIONS');
RUN;
/*Master Product - Store Txn data file (TOP 10 brands - 4million+ rows)*/
DATA work.brand_txn4;
SET work.brand_txn3;
where L5 IN ('PREGO','CLASSICO','RAGU OLD WORLD STYLE','RAGU','FIVE BROTHERS','HUNTS','PRIVATE LABEL','RAGU CHUNKY GARDEN STYLE','BARILLA','FRANCESCO RINALDI');
RUN;
/*Master Product - Store Txn data file (TOP 6 brands - 3million+ rows)*/
DATA work.brand_txn5;
SET work.brand_txn4;
where L5 IN ('PREGO','CLASSICO','RAGU OLD WORLD STYLE','RAGU','FIVE BROTHERS','HUNTS');
RUN;
/*A*/
Data work.top6_master;
set work.brand_txn5;
run;

/*Brand != HUNTS - Distinc flavours*/

proc sql;
create table work.a1 as
Select a.IRI_KEY as IRI_KEY, count(distinct a.FLAVOR_SCENT) AS other_cat
 from work.top6_master as a 
 where L5 <> 'HUNTS'
 group by IRI_KEY;
quit; 
/*Brand = HUNTS - Distinc flavours*/
proc sql;
create table work.b1 as
Select a.IRI_KEY as IRI_KEY, count(distinct a.FLAVOR_SCENT) AS hunts_cat
 from work.top6_master as a 
 where L5 = 'HUNTS'
 group by IRI_KEY;
quit; 
/*StoreID + OtherBrands vs HUNTS - Distinc flavours*/
/*B*/
proc sql;
create table work.store_brand_subcat as
select a1.IRI_KEY as IRI_KEY,a1.other_cat as other_cat, b1.hunts_cat as hunts_cat from 
 a1 left Join b1 on a1.IRI_KEY = b1.IRI_KEY; 
Quit; 
/*Brand = HUNTS - Weekly Sales parameters*/
proc sql;
create table work.c as
select distinct WEEK, sum(UNITS)as hunts_usales,sum(DOLLARS) as hunts_sales,sum(VOL_EQ) as hunts_vsales
from work.top6_master where L5 = 'HUNTS' group by WEEK; quit;
/*Top 6 - Weekly Sales parameters*/
proc sql;
create table work.d as
select distinct WEEK, sum(UNITS)as other_usales,sum(DOLLARS) as other_sales,sum(VOL_EQ) as other_vsales
from work.top6_master  group by WEEK; quit;
/*Top6 vs  HUNTS - Weekly Sales parameters - Unit Price Ratio Created*/
proc sql;
create table work.weekly_sales as
select d.WEEK, c.hunts_usales as hunts_usales , d.other_usales as other_usales,  (c.hunts_sales*d.other_vsales)/(d.other_sales*c.hunts_vsales) as hunts_uprice_i 
from work.c as c join work.d  as d on c.WEEK = d.WEEK; quit;
/*Top 6 vs  HUNTS - Weekly Sales parameters - Unit Price Ratio Created - last 5 weeks Hunts market share(units) adeed*/
/*C*/
proc sql;
create table work.weekly_sales2 as
select a.WEEK as WEEK, a.hunts_usales as hunts_usales , a.other_usales as other_usales, sum(b.hunts_usales)/sum(b.other_usales) as hunts_usales_ratio, a.hunts_uprice_i as hunts_uprice_i
from work.weekly_sales as a left join work.weekly_sales as b on 
a.WEEK >= b.WEEK and  a.WEEK < (b.WEEK +5)
group by a.WEEK, a.hunts_uprice_i,a.hunts_usales,a.other_usales;quit;
/*proc print data = work.weekly_sales2;run;*/

/*Weekly Store-wise Promotions Dummy variables creation(currently on upc level)*/
proc sql; create table w_st_promo as
select WEEK, IRI_KEY, 
(case when F IN ('A+','B+','C','D') and L5 = 'HUNTS' then 1 else 0 end) as f_h,
(case when F IN ('A+','B+','C','D') and L5 <> 'HUNTS' then 1 else 0 end) as f_o,
(case when D >0 and L5 = 'HUNTS' then 1 else 0 end) as d_h,
(case when D >0 and L5 <> 'HUNTS' then 1 else 0 end) as d_o,
(case when PR >0 and L5 = 'HUNTS' then 1 else 0 end) as pr_h,
(case when PR >0 and L5 = 'HUNTS' then 1 else 0 end) as pr_o
from work.top6_master; quit;
/*Weekly Store-wise Promotions Dummy variables creation(on week+store level) (hunt and other brands)*/
proc sql; create table work.w_st_promo1 as
select WEEK, IRI_KEY, 
max(f_h) as f_h, max(f_o) as f_o, max(d_h) as d_h, max(d_o) as d_o, max(pr_h) as pr_h, max(pr_o) as pr_o
from work.w_st_promo
group by WEEK, IRI_KEY; quit;
/*Weekly Store-wise Promotions Dummy variables creation(on week+store level) (hunt and other brands)+ (Hunts last week)*/
/*D*/
proc sql; create table work.w_st_promo2 as
select a.WEEK as WEEK, a.IRI_KEY as IRI_KEY,
a.f_h as f_h, a.f_o as f_o, 
a.d_h as d_h, a.d_o as d_o, 
a.pr_h as pr_h, a.pr_o as pr_o,
b.f_h as f_hlw, 
b.d_h as d_hlw, 
b.pr_h as pr_hlw
from work.w_st_promo1 as a left join work.w_st_promo1 as b
on a.WEEK = (b.WEEK +1)
and a.IRI_KEY = b.IRI_KEY;quit;
/*Weekly Store-wise Promotions Dummy variables creation(on week+store level) (hunt and other brands)+ (Hunts last week)
  Setting previous week dummy variable to zero for 1st week of the year*/
proc sql;
update work.w_st_promo2
      set pr_hlw=0 
         where pr_hlw is NULL;
		 proc sql;
update work.w_st_promo2
      set d_hlw=0 
         where d_hlw is NULL;
		 proc sql;
update work.w_st_promo2
      set f_hlw=0 
         where f_hlw is NULL;

/*proc print data = work.w_st_promo2 (obs = 20);run;*/

/*Ignore:
		 proc sql; create table work.store52 as
select IRI_KEY, count(WEEK) as week from work.w_st_promo2 group by IRI_KEY having week = 52;quit*/;

/*Creating Final Table for Panel Data : A+B+C+D*/
proc sql; create table  work.panel_ta as
select a.IRI_KEY as IRI_KEY, a.WEEK as WEEK, sum(a.UNITS) as units, sum(a.DOLLARS) as dollars, 
f_h as f_h, f_o as f_o, f_hlw as f_hlw, d_h as d_h, d_o as d_o, d_hlw as d_hlw, pr_h as pr_h, pr_o as pr_o, pr_hlw  as pr_hlw from work.top6_master as a
left join work.w_st_promo2 as b
on a.IRI_KEY = b.IRI_KEY
and a.WEEK = b.WEEK
where  a.L5 = 'HUNTS'
and a.IRI_KEY NOT in (select distinct IRI_KEY from work.store522)/*this filter is based on 2nd query ran afterwards)*/
group by a.IRI_KEY,a.WEEK,f_h, f_o, f_hlw, d_h, d_o, d_hlw, pr_h, pr_o, pr_hlw;quit;
proc sql; create table  work.panel_table as
select a.IRI_KEY as IRI_KEY, a.WEEK as WEEK, units as units, dollars as dollars, 
f_h as f_h, f_o as f_o, f_hlw as f_hlw, d_h as d_h, d_o as d_o, d_hlw as d_hlw, pr_h as pr_h, pr_o as pr_o, pr_hlw  as pr_hlw, 
hunts_cat as hunts_cat, other_cat as other_cat,
hunts_usales_ratio as hunts_usales_ratio, hunts_uprice_i as hunts_uprice_i
from work.panel_ta as a
left join work.store_brand_subcat as b
on a.IRI_KEY = b.IRI_KEY
left join work.weekly_sales2 as c
on a.WEEK = c.WEEK;quit;

/*Checking stores with <52 weeks data for making Panel data balanced*/
proc sql; create table work.store522 as
select IRI_KEY, count(WEEK) as week from work.panel_ta group by IRI_KEY having week < 52;quit

/*proc sql (obs = 10);
select * from work.w_st_promo2  where week = 1115;quit*/;
proc sql;
select WEEK,FLAVOR_SCENT, sum(DOLLARS) as dollar, sum(UNITS)as units from work.top6_master where
L5 = 'HUNTS' group by WEEK,FLAVOR_SCENT ; quit;


/*RFM Analysis*/libname proj 'E:/nitansh';

PROC SQL;
CREATE TABLE proj.panel_RFM AS (
SELECT PANID, MAX(WEEK) AS Recency_value, COUNT(WEEK) AS Frequency_value, SUM(DOLLARS) AS Monetary_value
FROM proj.customer_info 
GROUP BY PANID having Frequency_value > 0);


proc sort data= proj.panel_RFM out= proj.panel_RFM_data;
by DESCENDING Recency_value;
run;


proc rank DATA=proj.panel_RFM_data out= proj.Panel_RFM_Rank groups=3 ties=low;
var Recency_value;
ranks Recency_Rank;
run;

proc rank DATA= proj.Panel_RFM_Rank out=proj.Panel_RFM_FreqRank groups=3 ties=low;
var Frequency_value;
ranks Frequency_Rank;
run;

proc rank data = proj.Panel_RFM_FreqRank out = proj.panel_RFM_MonetaryRank groups=3 ties=low;
var Monetary_value;
ranks Monetary_Rank;
run;

data proj.Rank_RFM;
set proj.Panel_RFM_MonetaryRank;
Recency_Rank+1;
Frequency_Rank+1;
Monetary_Rank+1;
RFM_Value=cats(of Recency_Rank Frequency_Rank Monetary_Rank)+0;
run;

data proj.RFM_weighted;
set proj.Rank_RFM;
a=0.5;
b=0.3;
c=0.2;
WeightedRFM= (b * Recency_Rank)+ (c * Frequency_Rank) + (a * Monetary_Rank);
Cluster_Number=round(WeightedRFM,1);
if Cluster_Number=3 then Cluster_Name="Clust_3";
if Cluster_Number=2 then Cluster_Name="Clust_4";
if Cluster_Number=1 then Cluster_Name="Clust_5";
run;

proc print data=proj.RFM_weighted (obs=10);
run;


/*******************MDC*************************/
----------------------------------------------------------------------------;

DATA project1.PANEL_GROCERY;
INFILE 'C:\SRK\spagsauc_PANEL_GR_1114_1165.dat' FIRSTOBS=2 DLM='09'x;
LENGTH COLUPC $13.;
INPUT PANID WEEK $ UNITS OUTLET $ DOLLARS IRI_KEY $ COLUPC $;
RUN;

DATA project1.PANEL_GROCERY;
SET project1.PANEL_GROCERY;
IF LENGTH(COLUPC)=11 THEN COLUPC="0"||COLUPC;
RUN;

DATA project1.PANEL_GROCERY(DROP = OUTLET);
SET project1.PANEL_GROCERY(RENAME = (UNITS = P_UNITS DOLLARS = P_DOLLARS));
RUN;

PROC PRINT DATA=PANEL_GROCERY(OBS=20);RUN;

/*** IMPORTING GROCERY SCANNER DATA ***/

DATA project1.GROCERY;
INFILE 'C:\SRK\spagsauc_groc_1114_1165' FIRSTOBS=2;
INPUT IRI_KEY $ 1-7 WEEK $ 9-12 SY $ 14-15 GE $ 17-18 VEND $ 20-24 ITEM $ 26-30 UNITS 32-36 DOLLARS 38-45 
F $ 47-50 D $ 52-52 PR $ 54-54;
RUN;

DATA project1.GROCERY;
SET project1.GROCERY;
length colupc $14;
colupc=PUT(GE,Z1.)||PUT(VEND,Z5.)||PUT(ITEM,Z5.);
RUN;

DATA project1.GROCERY (DROP = SY GE VEND ITEM);
SET project1.GROCERY;
RUN;

PROC PRINT DATA=project1.GROCERY(OBS=20);RUN;

/*** JOINING GROCERY STORE AND PANEL DATA ***/

PROC SQL;
CREATE TABLE project1.PS_GROCERY AS
SELECT A.COLUPC,A.IRI_KEY,A.WEEK,B.PANID,B.P_UNITS,B.P_DOLLARS,A.UNITS,A.DOLLARS,A.F,A.D FROM
(SELECT * FROM project1.GROCERY) AS A 
INNER JOIN
(SELECT * FROM project1.PANEL_GROCERY) AS B
ON A.IRI_KEY=B.IRI_KEY AND A.WEEK = B.WEEK AND A.COLUPC = B.COLUPC;
QUIT;

proc import out=project1.spag datafile="H:\spag\prod_sauce.xls" dbms=EXCEL REPLACE;
getnames=YES; mixed=YES; scantext=YES; run;

PROC SQL;
CREATE TABLE project1.BRAND_SPAG AS
SELECT * FROM project1.spag WHERE L5 like '%HUNTS%' or L5 like '%PREGO%' or L5 like '%CLASSICO%' or  L5 like '%FIVE BROTHERS%' or  L5 like '%RAGU%';
QUIT;

DATA project1.BRAND_SPAG;
SET project1.BRAND_SPAG;
IF LENGTH(ITEM)=2 THEN ITEM="000"||TRIM(ITEM);
IF LENGTH(ITEM)=3 THEN ITEM="00"||TRIM(ITEM);
IF LENGTH(ITEM)=4 THEN ITEM="0"||TRIM(ITEM);
IF LENGTH(VEND)=2 THEN VEND="000"||TRIM(VEND);
IF LENGTH(VEND)=3 THEN VEND="00"||TRIM(VEND);
IF LENGTH(VEND)=4 THEN VEND="0"||TRIM(VEND);
SY=TRIM(SY);
GE=TRIM(GE);
VEND=TRIM(VEND);
ITEM=TRIM(ITEM);
COLUPC=SY||GE||VEND||ITEM;
RUN;

DATA project1.BRAND_SPAG;
SET project1.BRAND_SPAG;
COLUPC=COMPRESS(COLUPC);
RUN;

DATA project1.BRAND_SPAG (KEEP = L4 L5 COLUPC VOL_EQ);
SET project1.BRAND_SPAG;
RUN;

PROC PRINT DATA=project1.BRAND_SPAG(OBS=20);
RUN;

/*** JOINING "SAUCE" & "GROCERY STORE/PANEL DATA" ***/

PROC SQL;
CREATE TABLE project1.MDCFINAL AS
SELECT * FROM
(SELECT * FROM project1.BRAND_SPAG) AS A 
INNER JOIN
(SELECT * FROM project1.PS_GROCERY) AS B
ON A.COLUPC=B.COLUPC;
QUIT;

PROC PRINT DATA=project1.MDCFINAL(OBS=20);RUN;

PROC SQL;
CREATE TABLE project1.BRANDS_3 AS
SELECT COUNT(DISTINCT(L5)) AS C_L5,IRI_KEY,WEEK FROM project1.MDCFINAL GROUP BY IRI_KEY,WEEK HAVING C_L5 = 3;
QUIT;

PROC CONTENTS DATA=project1.BRANDS_3;RUN;

PROC SQL;
CREATE TABLE project1.GROCERY_FINAL AS
SELECT * FROM
(SELECT * FROM project1.BRANDS_3) AS A 
INNER JOIN
(SELECT * FROM project1.MDCFINAL) AS B
ON A.WEEK=B.WEEK AND A.IRI_KEY = B.IRI_KEY;
QUIT;

DATA project1.GROCERY_FINAL (DROP = C_L4 P_UNITS P_DOLLARS);
SET project1.GROCERY_FINAL;
RUN;

PROC PRINT DATA=project1.GROCERY_FINAL(OBS=20);RUN;

/*********** CALCULATING PRICE DISPLAY FEATURE AND PROMOTION ************/

DATA project1.GROCERY_FINAL;
SET project1.GROCERY_FINAL;
TOT_VOL = VOL_EQ*100*UNITS;
PRICE_PER_ML = DOLLARS/TOT_VOL;
RUN;

DATA project1.GROCERY_FINAL;
SET project1.GROCERY_FINAL;
IF F='NONE' THEN F_1=0;
ELSE F_1=1;
IF D='0' THEN D_1=0;
ELSE D_1=1;
IF PR='0' THEN PR_1=0;
ELSE PR_1=1;
RUN;

PROC PRINT DATA=project1.GROCERY_FINAL(OBS=20);
RUN;

PROC CONTENTS DATA=GROCERY_FINAL; RUN;

PROC SQL;
CREATE TABLE project1.HUNTS_MDC AS
SELECT * FROM project1.GROCERY_FINAL WHERE L5 LIKE '%HUNTS%';
QUIT;

PROC SQL;
CREATE TABLE project1.HUNTS_GROUPED_MDC AS
SELECT IRI_KEY,WEEK,COLUPC,SUM(UNITS) AS TOT_UNITS, SUM(DOLLARS) AS TOT_DOLLARS, SUM(F_1) AS TOT_F, 
SUM(D_1) AS TOT_D, SUM(PR_1) AS TOT_PR, AVG(PRICE_PER_ML) AS AVG_PPM, SUM(TOT_VOL) AS TOT_ML FROM project1.HUNTS_MDC
GROUP BY IRI_KEY,WEEK,COLUPC;
QUIT;

PROC SQL;
CREATE TABLE project1.HUNTS_WEEKLY_MDC AS
SELECT IRI_KEY,WEEK,SUM(UNITS) AS TOT_IRI_WEEK_UNITS, SUM(TOT_VOL) AS TOT_IRI_WEEK_ML
FROM project1.HUNTS_MDC
GROUP BY IRI_KEY,WEEK;
QUIT;

PROC SQL;
CREATE TABLE project1.HUNTS_GROUPED_MDC_1 AS
SELECT A.*, B.* FROM 
(SELECT * FROM project1.HUNTS_GROUPED_MDC) AS A
LEFT JOIN
(SELECT * FROM project1.HUNTS_WEEKLY_MDC) AS B
ON A.IRI_KEY=B.IRI_KEY AND A.WEEK=B.WEEK;
QUIT;

DATA project1.HUNTS_GROUPED_MDC_1;
SET project1.HUNTS_GROUPED_MDC_1;
W_PRICE = AVG_PPM*(TOT_ML/TOT_IRI_WEEK_ML);
IF TOT_F=0 THEN W_FEATURE=0;
ELSE W_FEATURE=1*(TOT_ML/TOT_IRI_WEEK_ML);
IF TOT_D=0 THEN W_DISPLAY=0;
ELSE W_DISPLAY=1*(TOT_ML/TOT_IRI_WEEK_ML);
IF TOT_PR=0 THEN W_PROMOTION=0;
ELSE W_PROMOTION=1*(TOT_ML/TOT_IRI_WEEK_ML);
RUN;

PROC SQL;
CREATE TABLE project1.HUNTS_MDC_FINAL AS
SELECT IRI_KEY,WEEK,SUM(TOT_UNITS) AS HU_TOT_UNITS, SUM(TOT_DOLLARS) AS HU_TOT_DOLLARS,SUM(W_PRICE) AS HU_W_PRICE,
SUM(W_FEATURE) AS HU_W_FEATURE, SUM(W_DISPLAY) AS HU_W_DISPLAY, SUM(W_PROMOTION) AS HU_W_PROMOTION, SUM(TOT_ML) AS HU_SUM_ML
FROM project1.HUNTS_GROUPED_MDC_1 
GROUP BY IRI_KEY,WEEK;
QUIT;

PROC PRINT DATA=HUNTS_MDC_FINAL(OBS=20);
RUN;


