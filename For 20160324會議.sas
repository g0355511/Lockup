﻿/*  新任務  */

libname bon 'D:\Documents\For 邦茹\';

/*  計算「淨」買賣張數、金額  */
data a01;
   set bon.big3_raw2;
   N_ALL_bs=N_ALL_b-N_ALL_s;
   N_fg_bs=N_fg_b-N_fg_s;
   N_dl_bs=N_dl_b-N_dl_s;
   N_tt_bs=N_tt_b-N_tt_s;
   D_ALL_bs=D_ALL_b-D_ALL_s;
   D_fg_bs=D_fg_b-D_fg_s;
   D_dl_bs=D_dl_b-D_dl_s;
   D_tt_bs=D_tt_b-D_tt_s;
run;

proc sql;
   create table q05 as
   select *
         ,mdy_vol-mdy_unlock1 as dif1    /*  計算交易量資料時間，與首次解除集保日之間的時間差距  */
         ,mdy_vol-mdy_unlock2 as dif2    /*  計算交易量資料時間，與完全解除集保日之間的時間差距  */
   from bon.sample_unlock a left outer join a01 b
   on a.stk=b.stk
   order by stk, date;
quit;   

data tmp3;
   set q05;
   by stk mdy_vol;
   if first.stk then day=1;  /*  注意：籌碼資料溯及興櫃階段  */
   else day=day+1;
   retain day;
run;

/*  Macro, 作出距事件日的日期  */

libname bon 'D:\Documents\For 邦茹\';

%macro day(y);   /*  目標：計算距事件日的「交易」天數  */

/*  和宣告日期合併，取事件日當天，或之後最近的一天的資料  */
proc sql;
   create table q050 as
   select stk
         ,&y
         ,day as base                             /*  存下事件日（或之後最近一天）的流水號  */
         ,mdy_vol-&y as dif1                    
         ,min(mdy_vol-&y) as dif_min            
   from tmp3               
   where (mdy_vol-&y >=0)    /*  從tmp3中，取出解除日（首度或最終）之後的資料  */              
   group by stk
  /*  having mdy_vol-&y = min(mdy_vol-&y )   */  /*  僅留事件日或距事件日最近的資料  */
   having mdy_vol-&y = 0
   order by stk, mdy_vol;
quit;  
   
proc sql;
   create table q051 as
   select a.stk
         ,a.mdy_vol
         ,b.&y                             
         ,a.mdy_vol-b.&y as dif1
         ,a.day-b.base as day1              /*  計算距事件日的「交易」天數  */
         ,a.*
   from tmp3 a right outer join q050 b      /*  合併籌碼檔與前一個檔（取得base流水號） */   
   on a.stk=b.stk
   group by a.stk
   order by b.stk, a.mdy_vol;
quit;   
   
%mend;

%day(mdy_unlock1);   /*  產出：q051，下面要用  */


/*  計算平均數(-26,-6)期間  */
data tmp31;
   set q051;
   where (-26<=day1<-6);    /*  取[-26,-6]期間資料  */
run;

/*  取平均數  */
ods html close;
proc means data=tmp31;
   var N_fg_bs  N_tt_bs  N_dl_bs  N_ALL_bs
       D_fg_bs  D_tt_bs  D_dl_bs  D_ALL_bs;   
   by stk;
   output out=tmp33 (drop = _TYPE_ _FREQ_)  mean (N_fg_bs  N_tt_bs  N_dl_bs  N_ALL_bs
                                                  D_fg_bs  D_tt_bs  D_dl_bs  D_ALL_bs)
                                                 =MN_fg_bs  MN_tt_bs  MN_dl_bs  MN_ALL_bs
                                                  MD_fg_bs  MD_tt_bs  MD_dl_bs  MD_ALL_bs ; 
quit;   

/*  計算「異常」交易張數、金額  */
proc sql;
   create table tmp341 as
   select a.*
         ,b.*
         ,(N_fg_bs  - MN_fg_bs  )/ (MN_fg_bs  + 1 ) as  AN_fg_bs 
         ,(N_tt_bs  - MN_tt_bs  )/ (MN_tt_bs  + 1 ) as  AN_tt_bs 
         ,(N_dl_bs  - MN_dl_bs  )/ (MN_dl_bs  + 1 ) as  AN_dl_bs 
         ,(N_ALL_bs - MN_ALL_bs )/ (MN_ALL_bs + 1 ) as AN_ALL_bs
         ,(D_fg_bs  - MD_fg_bs  )/ (MD_fg_bs  + 1 ) as  AD_fg_bs 
         ,(D_tt_bs  - MD_tt_bs  )/ (MD_tt_bs  + 1 ) as  AD_tt_bs 
         ,(D_dl_bs  - MD_dl_bs  )/ (MD_dl_bs  + 1 ) as  AD_dl_bs 
         ,(D_ALL_bs - MD_ALL_bs )/ (MD_ALL_bs + 1 ) as AD_ALL_bs
   from q051 a left outer join tmp33 b
   on a.stk=b.stk
   order by stk, day1;
quit;


/*  將有異常的交易資料，存出成永久檔  */
data bon.big3_ab_vol2;
   set tmp341;
run;   


%Macro aturn(a,b);   /*  目標：計算「累積」交易量（含：異常累積）  */

ods html close;

/*  計算「累積」交易量（含：異常累積）  */

proc sql;
   create table tmp&a&b as
   select stk
         ,name
         ,sum(N_ALL_bs) as  CN_ALL_bs 
         ,sum(N_fg_bs ) as  CN_fg_bs  
         ,sum(N_dl_bs ) as  CN_dl_bs  
         ,sum(N_tt_bs ) as  CN_tt_bs  
         ,sum(D_ALL_bs) as  CD_ALL_bs 
         ,sum(D_fg_bs ) as  CD_fg_bs  
         ,sum(D_dl_bs ) as  CD_dl_bs  
         ,sum(D_tt_bs ) as  CD_tt_bs  
         ,sum(AN_ALL_bs) as CAN_ALL_bs 
         ,sum(AN_fg_bs ) as CAN_fg_bs  
         ,sum(AN_dl_bs ) as CAN_dl_bs  
         ,sum(AN_tt_bs ) as CAN_tt_bs  
         ,sum(AD_ALL_bs) as CAD_ALL_bs 
         ,sum(AD_fg_bs ) as CAD_fg_bs  
         ,sum(AD_dl_bs ) as CAD_dl_bs  
         ,sum(AD_tt_bs ) as CAD_tt_bs  
         ,"key&a&b" as key
         
   from bon.big3_ab_vol2
   where (&a <=day1<= &b)
   group by stk
   order by stk;
quit;

proc sort data=tmp&a&b nodup;
   by _all_;
quit;               
  
%mend;


data list;
   input y $  1-10; 
   datalines;
CN_ALL_bs  
CN_fg_bs   
CN_dl_bs   
CN_tt_bs   
CD_ALL_bs  
CD_fg_bs   
CD_dl_bs   
CD_tt_bs   
CAN_ALL_bs 
CAN_fg_bs  
CAN_dl_bs  
CAN_tt_bs  
CAD_ALL_bs 
CAD_fg_bs  
CAD_dl_bs  
CAD_tt_bs  
;   
run;
data list;
   set list;
   no=_N_;
run;   


%macro comp(a,b,c,d);

%aturn(&a,&b);  /*  (0,2)期間，三大法人的累積（異常）交易量（金額）  */
%aturn(&c,&d);
data tmp;
   set tmp&a&b tmp&c&d ;
run;

data q6;   run;

%do i=1 %to 16;   /*  16  */
data list01;
   set list;
   where no=&i;
   call symput('y',y);
run;   

ods output  ClassLevels=q1 NObs=q2 ModelANOVA=q3 Means=q4;
proc glm data = tmp ;
  class key;
  model &y=key;
  means key /deponly;
quit;


data q1;
   set q1;
   keep Values;
data q2;
   set q2;
   where Label='使用的觀測值數目';
   keep N;

data q3;
   set q3;
   where HypothesisType=3;
   keep FValue ProbF;

%Macro star(file);
   data &file;
      set &file;
           if ProbF=. then S_F='   ';
      else if ProbF<0.01  then S_F='***';
      else if ProbF<0.05  then S_F='**';
      else if ProbF<0.1   then S_F='*';
      drop ProbF;
   run;         
%mend;

%star(q3);

   
data q41 q42;
   set q4;
   no=_N_;
   if no=1 then output q41;
   else if no=2 then output q42; 
   keep key N Mean_&y;
run;

data q41; set q41; rename N=N1 Mean_&y=Mean1 key=key1; 
data q42; set q42; rename N=N2 Mean_&y=Mean2 key=key2; 
run;

data q5;
   merge q1-q3 q41-q42;
   YYY="&y";
run; 
proc datasets lib=work memtype=data;
   modify q5; 
     attrib _all_ label=' '; 
     attrib _all_ format=;
quit;

proc sql;
   create table q51 as
   select YYY, Values, N, Fvalue, S_F, key1, N1, Mean1, key2, N2, Mean2 
   from q5;
quit;   

data q6;
   set q6 q51;
run;




%end;
%mend;

    **********>>>*********  第一個結果   ;
%comp(0,2,3,5);
%comp(0,3,4,7);
%comp(1,2,3,4);
%comp(1,3,4,6);
%comp(1,4,5,8);

