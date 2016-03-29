/*  新任務  */

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




/*  任務二：觀察事件前後，每天，三大法人的異常交易量的比較（差異）  */

%Macro agogo(a,b,y);   /*  目標：計算「累積」交易量（含：異常累積）  */

ods html close;

/*  計算「累積」交易量（含：異常累積）  */

data c00;         
   set bon.big3_ab_vol2;
   where &a <=day1<= &b;
run;

data c01;
   set c00;
   id="fg";
   rename AN_fg_bs=AN AD_fg_bs=AD;
   keep stk AN_fg_bs AD_fg_bs id day1;
run;   

data c02;
   set c00;
   id="tt";
   rename AN_tt_bs=AN AD_tt_bs=AD;
   keep stk AN_tt_bs AD_tt_bs id day1;
run;   

data c03;
   set c00;
   id="dl";
   rename AN_dl_bs=AN AD_dl_bs=AD;
   keep stk AN_dl_bs AD_dl_bs id day1;
run;   

data c1;
   set c01 c02 c03;
run;   

data q6;  run;

%do i=1 %to 1;

ods output  ClassLevels=q1 NObs=q2 ModelANOVA=q3 Means=q4;
proc glm data = c1 ;
   where day1=&i;
   class id;
   by day1;
   model &y=id;
   means id /deponly;
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

   
data q41 q42 q43;
   set q4;
   no=_N_;
   if no=1 then output q41;
   else if no=2 then output q42; 
   else if no=3 then output q43; 
   keep id N Mean_&y;
run;

data q41; set q41; rename N=N1 Mean_&y=Mean1 id=key1; 
data q42; set q42; rename N=N2 Mean_&y=Mean2 id=key2; 
data q43; set q43; rename N=N3 Mean_&y=Mean3 id=key3; 
run;

data q5;
   merge q1-q3 q41-q43;
   YYY="&y";
   day1="&y";
run; 
proc datasets lib=work memtype=data;
   modify q5; 
     attrib _all_ label=' '; 
     attrib _all_ format=;
quit;

proc sql;
   create table q51 as
   select YYY, day1, Values, N, Fvalue, S_F, key1, N1, Mean1, key2, N2, Mean2, key3, N3, Mean3   
   from q5;
quit;   

data q6;
   set q6 q51;
run;

%end;
%mend;

%agogo(-15,15,AD);


















************************;
*  整合行專案           ;
*    先計算報酬         ;
*    在計算交易量       ;
************************;
/*
bon.ar_car_20_20 from
D:\Documents\For 邦茹\16_5_分析_分群敘述統計表_AR_CAR_仿.sas
*/

libname bon 'D:\Documents\For 邦茹\';

%Macro eleven(a,b);   /*  目標：計算累積異常交易量  */

proc sql;
   create table a01 as
   select stk
         ,name
         ,sum(ar) as CAR_P&a._&b
/*         ,(exp(sum(ret_ln)/100)-1)*100 as hpr_P&a._&b   */
/*         ,(exp(sum(ret_mkt)/100)-1)*100 as hpr_mkt      */
/*         ,calculated hpr_P&a._&b - calculated hpr_mkt as hpr_adj_P&a._&b   */
   from bon.ar_car_20_20
   where &a<=day1<=&b         
   group by stk
   order by stk;
quit;
proc sort data=a01 nodup out=a02;
   by _all_;
quit;    


%mend;

%eleven(1,6);  

*  產業代碼   ;
data fin;
   set bon.fin;
   if stk='3369' then do; ind='M2300';  ind_name='電子零組件';  end;
   ind2=substr(ind,2,2);
   keep stk ind ind2 ind_name; 
run;

/*  結合檔案  */
data d01;
   merge bon.sample_unlock a02(in=w1) tmp342(in=w2) bon.IPO(keep=stk u_mkt) bon.vc(keep=stk vc) fin(keep=stk ind ind2 ind_name);
   by stk;
run;

data d02;
   set d01;
   IPO_year=int(d_IPO/10000);
run;   

/*  註：
   1. a02： 
   2. tmp342：計算「累積」交易量（含：異常累積）  
   3. fin: IPO前後的財務指標   
*/

/*  敘述統計--起  */


ods html close;
data p1;
   input vb $  1-11  @;
   datalines;
IPO_unlock1 
vc          
CAR_P1_6       
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
data p1;
   set p1;
   no=_N_;
run;

%Macro goa;

data p0;   run;

%do i=1 %to 1;    /* 19  */
data p2;
   set p1;
   if no=&i;
   call symput('vb', vb);
run;
proc means data=d02 n mean min q1 median q3 max std;    /*  data=q0來自  'D:\Lai\處理極端值20150717.sas'  */
   var &vb;                                            /*  data=tmp:沒去邊; data=q0:有去邊  */
   class ind_name;
   output out=p3
          n     =n
          mean  =mean
          std   =std   
          t     =T
          prt   =prt     
          min   =min   
          q1    =q1
          median=median               
          q3    =q3    
          max   =max;                       
quit;
data p3;
   set p3;
   vb="&vb";
data p0;
   set p0 p3;
run;
%end;
%mend;
   
%goa;

data p0;
   set p0;
   if prt=. then P='   ';
   else if prt<0.01  then P='***';
   else if prt<0.05  then P='**';
   else if prt<0.1   then P='*';
run;

proc sql;
   create table p4 as
   select vb
          ,ind_name
          ,n	
          ,mean	
          ,std	
          ,T
          ,P
          ,min	
          ,q1	
          ,median	
          ,q3	
          ,max

   from p0  (drop=_type_ _FREQ_)
   where vb ne '';
quit;   

/*  將label的值清除掉  */
proc datasets lib=work memtype=data;
   modify p4; 
     attrib _all_ label=' '; 
     attrib _all_ format=;
quit;  

proc sort data=d02(keep= ind_name ind ind2) out=d03 nodup;
   by ind;
quit;   
proc sort data=d03 nodup out=d04;
by _all_;
quit; 
 

proc sql;
   create table d05 as
   select a.ind_name
         ,b.ind
         ,b.ind2
         ,a.*
   from p4 a left outer join d04 b
   on a.ind_name=b.ind_name
   order by ind2, ind;
quit;   
            
   

/*  敘述統計--迄  */




