*************************************************************************************************************
* Title            : Con-Round macro                                 
* Description      : Con-Round time series data. 
*                    The input dataset can be multi periodicity (monthly, quarterly and annual)
*                  
* Parameters       : ts_value - the column name on the input dataset where the values are held.
*                    Round    - The number of decimal places to round.(eg. 0,1,2....)
* Globals accessed : Input       - input dataset name
*                    Output      - output dataset name
*                    Result      - Name of the output dataset column to hold the results. 
* Macros called    : error_check - test the success of any sas step.
*                    subset_by_periodicity - subset the input data by periodicity.
*                    quarterly_con - conround quarterly data 
*                    aggregate - aggregate time series data
*                    validate_ds - validate time series data prior to aggregation.
*                    val
* Author           : Krunch           
* Date written     : Nov 2005       
*     
* Change history   : 
*__________________________________________________________________________________________________________________________________
* 				Date:			UserId:			Description:	
*___________________________________________________________________________________________________________________________________
*  Amend1		28/11/06		gagorr			addition of the 'rows_to_tweak' variable to the keep statement, to make sure
*												that it is available for futher calculations
*  Amend2		28/11/06		gagorr			change the way in which allocated_value is calculated 
*												(to make it consistent with function specification)
*  Amend3		19/11/07		krunch			DML enhancements.
*  Amend4		11/01/08		gagorr/krunch	Ref.no. defect 30944. Extra validation implemented to ensure that the function's second parameter 
*												"precision" is restricted to the non-negative integers exlusively. Please see function spec 
*												for more details.
*  Amend5(abc)	30/09/08		krunch			Defect 25230 -  Monthly data processing was failing due to ___pdicity being out of step on interim datasets.
*  Amend6		02/03/16		smithj			Introducing DML for check on 2nd parameter (rounding precision). Using validate types to check this during 'save' phase. 
*************************************************************************************************************;
%macro conround(ts_value, round);
  options validvarname=any;
  %put %sysfunc(ifc(&putflag=1,ERROR-------- In Macro &sysmacroname ... %sysfunc(time(),time11.2) -------,)) ;;
  %let first_valid=1;

  %global numobs;

  %put ts_value= &ts_value;
  %put round= &round;
  %put input= &input;
  %put output= &output;
  %put result= &result;
  %put round= 10**-&round;

  /* Amend6 - using validate types for 'save' action. DML moved further down (after validate types). */

/* Amend4: Regardless of execmode.*/
/*%if ^%index(&round,___tempvar) %then */
/*	 The second parameter is NOT a ___tempvar timeseries name, so do the next check.*/
/*	%if ^%is_a_non_negative_integer(&round) %then %do;*/
/*		%let saserror = The precision argument for the CONROUND function is not a non-negative integer. Please adjust.;*/
/*		%let stopsas = 1;*/
/*		%error_check*/
/*		 Note: If execmode=1 no DML message exists for this problem yet.*/
/*	%end;*/
/* End of Amend4.*/

/************************************************************************
 Prepare function.  Create lists to call macro 'prepare_function'.
************************************************************************/  
  %let this=&sysmacroname ;
  %let write_flag=0;
  %if %symexist(get_dim_only) %then
    %do;
      %if &get_dim_only=1 %then
        %let write_flag=1;
    %end;

/* Set up Macro lists.*/ 
  %let write_list=&ts_value;
  %let write_list = %superq(write_list);

/* Create list of input parameter(TS only) names.*/ 
  %let var_list=ts_value;
  %let var_list = %superq(var_list);

/* Create list of input parameter(TS only) values.*/ 
  %if %index(%upcase(&round),___TEMPVAR)>0 %then
    %let ts_list=&ts_value~&round;
  %else
    %let ts_list=&ts_value;
  %let ts_list = %superq(ts_list);

/* Create list of all input parameter values.*/ 
  %let param_list=&ts_value~&round;
  %let param_list = %superq(param_list);

/* Create list of all input parameter types.*/ 
  /* Amend6 - new type for positive integer used */
  %let type_list=anyvar~posint;
  %let type_list = %superq(type_list);

/* Validate and evaluate parameters.*/
  %prepare_function;
  %error_check; 

/* Amend6 - if validate types does not catch this during save use DML here */

%if &execmode %then
	%if ^%index(&round,___tempvar) %then 
		/*The second parameter is NOT a ___tempvar timeseries name, so do the next check.*/
		%if ^%is_a_non_negative_integer(&round) %then %do;
			%let message_code = SAS17040;
			%let stopsas = 1;
			%error_check
		%end;

/* Check return codes and flags.*/
  %if &write_flag or &return_code or ^&execmode %then 
    %return;

  %put ts_list= &ts_list;
  %put ts_value=&ts_value;

/************************************************************************
 End of Prepare function.  
************************************************************************/

/* Amend3: DML function input validation.*/
%dml_stage_validation(diag_stage=FUNCTION_INPUT);
%if ^&has_some_values %then %return; /* Return to call_multi_per.*/



/* Get the names of all the other dimensions on input table.*/
  proc contents data=&input nodetails noprint out=contents(keep=name);run;
  %error_check;  
  proc sql noprint;
    select quote(trim(name))||'n' 
       into :by_vars_con separated by ' '
      from contents
        where upcase(name) NE %upcase("&ts_value")
          and upcase(name) NE 'TS_PERIOD';
/*          and upcase(name) NOT contains '___TEMPVAR';*/
  quit;
  %error_check; 
  %put by_vars_con = &by_vars_con;

/* Split up the input data by periodicity first. Each subset will then be conrounded individually.*/
/*%subset_by_periodicity(&input)*/
  %add_pdicity(&input) ;
  %error_check; 

/* Save these subsets under a new name, as 'subset_by_periodicity' will be called again within the
   'aggregate' macro and overwrite current outputs.*/
  %let quarterly_data = 0;
  %let monthly_data = 0;
  
  %if (&___q_obs GT 0) %then
    %do;
      %let quarterly_data = 1;
      %let obs_qtr = &___q_obs;
      data qtr_subset;
        set &input._q;
      run;
      %error_check; 
    %end;

  %if (&___m_obs GT 0) %then 
    %do;
      %let monthly_data = 1;
      %let obs_mon = &___m_obs;
      data mon_subset;
        set &input._m;
      run;
      %error_check; 
    %end;

/* Store any annual data in the output dataset. Nothing else to do with it.
   Will probably have no rows but at least the 'output' dataset now exists.*/
  %if %index(%upcase(&round),___TEMPVAR)>0 %then
    %merge_these(&input._a(in=in_flag) &round,&input._a,&other_dim,stmts=if in_flag) ;

  data output;
    set &input._a;
    "&ts_value"n = round("&ts_value"n, 10**-&round);
  run;
  %error_check;

  %if (&___a_obs GT 0) %then 
   /* Set the first-valid flag .*/
    %let first_valid=0;

/********************************************************************************
 Store the Global values for 'output' and 'result' as these will be overwritten
 in 'quarterly_con' macro when the 'aggregate' macro is called. 
 They can then be replaced when needed later.
*********************************************************************************/
  %let real_input = &input;
  %let real_output = &output;
  %let real_result = &result;

/**************************************************************************** 
 ConRound the Quarterly data.
*****************************************************************************/
%if (&quarterly_data) %then
  %do;
    /*************************************************
     Validate the data before conrounding.
    **************************************************/
    %validate_ds(qtr_subset, Q);
    %error_check;

    /* Store any 'bad_pile' data from above in the output dataset. This data will not be conrounded.*/
    %if %sysfunc(exist(bad_pile)) %then
      %do;
        data output;
          set  output 
               bad_pile;
          "&ts_value"n = round("&ts_value"n, 10**-&round);
        run;
        %error_check;
      %end;

  /* See if there are any rows left in the 'qtr_subset' dataset after validation.*/
    proc sql noprint;
      select count(ts_period)
        into :rows
          from qtr_subset;
    quit;
    %error_check;
    %put rows= &rows;  

    %if (&rows>0) %then
      %do;
       /* ConRound the Quarterly data.*/
        %quarterly_con;
        %error_check;

       /* Store the quarterly conrounded output in the output dataset.*/
        data output;
          set output
              qtr_subset_output;
        run;
        %error_check; 

        %let first_valid=0;
      %end;/* rows if test.*/

    %else 
      %do;
       /* No Quarterly data passed validation. Set saserror and leave stopsas=0.*/
        %let saserror= WARNING: All 'Quarterly' data failed 'CONROUND' validation. No 'Quarterly' data will be processed.;
        %error_check; 
      %end;

  %end;/* quarterly_data if test.*/

/**************************************************************************** 
 ConRound the Monthly data.
*****************************************************************************/
  %if (&monthly_data) %then
    %do;
    /*************************************************
     Validate the monthly data before conrounding.
    **************************************************/
      %validate_ds(mon_subset, M);
      %error_check;

     /* Store any 'bad_pile' data from above in the output dataset. This data will not be conrounded.*/
      %if %sysfunc(exist(bad_pile)) %then 
        %do;
          data output;
            set  output 
                 bad_pile;
            "&ts_value"n = round("&ts_value"n, 10**-&round);
          run;
          %error_check;
        %end;

      /* See if there are any rows left in the 'mon_subset' dataset after validation.*/
      proc sql noprint;
         select count(ts_period)
         into :rows
         from mon_subset;
      quit;
      %error_check;
      %put rows= &rows;

      %if (&rows>0) %then 
        %do;
     /* Add a label to the mon_subset dataset to show the 'aggregate' macro that subset/validate has been done.*/
          data mon_subset(label="conround" );
             set mon_subset;
          run ;
          %error_check; 

       /************************************************************************************************************************* 
        ConRound the Monthly data. This first bit takes the monthly data and aggregates it up to quarterly data. This quarterly 
        output can then be put through the same conround process as real quarterly data(above). 
        The ouput from this step will then be used to conround the input monthly data. Follow that??
       **************************************************************************************************************************/

      /* Re-Set the globals that will be used by 'aggregate' to fool it into doing what we want.*/
      %let result = &ts_value;
      %let input = mon_subset;
      %let output = qtr_subset; 
      /* Monthly time series need to be aggregated to Quarterly.*/
      %aggregate(&ts_value, Q, total);
      %error_check;

	/* Amend5a - Remove the ___pdicity variable as it does not truly represent the data at this point. Aggregate will replace it next.*/
	data qtr_subset(drop=___pdicity);
	set qtr_subset;
	run ;
	%error_check; 

      /* ConRound the Quarterly data.*/
      %quarterly_con;
      %error_check;

      /* Is &round a number or a dimension name?*/
      %if %index(%upcase(&round),___TEMPVAR)>0 %then
	      /* Get the output dimension from the value function on the mon_subset table.*/
          %merge_these(mon_subset &round,mon_subset,&other_dim) ;

       /* Round the input monthly series values and
          Create a quarterly_period column on 'mon_subset'. This will be used to merge the discrepancy values next.
        I know this next bit looks strange, but it's the best way of creating a quarterly(text) date from a monthly(text) date.*/ 
      
      data mon_subset(keep=ts_period "&ts_value"n quarterly_period rounded_value &by_vars_con_q);
        if _n_=1 then 
          do;
            rx1=prxparse("s/JAN|FEB|MAR/Q1/");
            rx2=prxparse("s/APR|MAY|JUN/Q2/");
            rx3=prxparse("s/JUL|AUG|SEP/Q3/");
            rx4=prxparse("s/OCT|NOV|DEC/Q4/");
          end;
        retain rx1;
        retain rx2;
        retain rx3;
        retain rx4;
  
        numch=0;
        set mon_subset;
        quarterly_period = compress(upcase(ts_period));
        rounded_value = round("&ts_value"n, 10**-&round);

        call prxchange(rx1,-1,quarterly_period,quarterly_period,reslen,trunc,numch);
        if (numch=0) then call prxchange(rx2,-1,quarterly_period,quarterly_period,reslen,trunc,numch);
        if (numch=0) then call prxchange(rx3,-1,quarterly_period,quarterly_period,reslen,trunc,numch);
        if (numch=0) then call prxchange(rx4,-1,quarterly_period,quarterly_period,reslen,trunc,numch);
        quarterly_period = substr(quarterly_period,1,6);
      run;
       /* You MUST do this bit, or SAS will fail.*/
      data _null_;
        call prxfree(rx1);
        call prxfree(rx2);
        call prxfree(rx3);
        call prxfree(rx4);
      run;

       /* Create a temporary dataset holding the rounded input monthly series values and aggregate them. This output 
          will then be tagged on to the 'qtr_subset_output' dataset creating the 'sum_rounded_months' column.*/
      data temp1(rename=(rounded_value="&ts_value"n)label="conround");
      set  mon_subset(drop = "&ts_value"n quarterly_period);
	  run;

      /* Re-Set the globals that will be used by 'aggregate' to fool it into doing what we want.*/
      %let input = temp1;
      %let output = temp1_agg; 
      %aggregate(&ts_value, Q, total);
      %error_check;



      /* Is &round a number or a dimension name?*/
      %if %index(%upcase(&round),___TEMPVAR)>0 %then 
          /* Get the output from the value function on the aggregated table.*/
          %merge_these(temp1_agg &round,temp1_agg,&other_dim) ;

	  /* Amend5b -  ___pdicity variable cannot be used in the following sorts as it does not truly represent the data at this point. 
		  			So 'other_dim' is more appropriate as we should only be dealing with a single periodicity of data.*/ 
/*      proc sort data=qtr_subset_output; by ts_period &by_vars_con_q ; run;*/
/*      proc sort data=temp1_agg; by ts_period &by_vars_con_q ; run;*/

	  proc sort data=qtr_subset_output; by ts_period &other_dim ; run;
      proc sort data=temp1_agg; by ts_period &other_dim; run;
      %error_check;

     /* Merge the data creating the 'sum_rounded_months' and 'rounded_value' column.
        Also rename ts_period to quarterly_period to match 'mon_subset' table.*/
       data qtr_subset_output(rename=(ts_period=quarterly_period));
       merge  qtr_subset_output 
              temp1_agg(rename=("&ts_value"n=sum_rounded_months));
       by ts_period /*&by_vars_con_q*/ &other_dim /* Amend5c. - See 5b for details.*/;
       rounded_value = round("&ts_value"n,10**-&round);
       quarterly_discrepancy= rounded_value - sum_rounded_months;

         /*Calculate the number(n) of rows in the group to adjust.*/
         if not missing(quarterly_discrepancy) then 
            rows_to_tweak= abs(quarterly_discrepancy)/(10**-&round) ;
      
       run;
       %error_check;

       /*Sort aggregate datasets for merge.*/
       proc sort data=qtr_subset_output; by &by_vars_con_q quarterly_period; run;
       proc sort data=mon_subset; by &by_vars_con_q quarterly_period; run;
       %error_check; 
	   
       /******************************************************************************** 
      Add the 'quarterly_discrepancy' column from 'qtr_subset_output' to 'mon_subset'.
        Calculate distortion values.
       *********************************************************************************/
       data mon_subset;
		 /*Amend1 - the variable 'rows_to_tweak' was added to the list of variables in the keep statement below,
		 				as it is used by the  datasteps with SAmend2 (please see below)*/
       merge qtr_subset_output(in=b keep = &by_vars_con_q quarterly_period quarterly_discrepancy rows_to_tweak) 
             mon_subset;
         if not missing(quarterly_discrepancy) then do;
            if (quarterly_discrepancy GT 0) then 
             distortion = abs(((rounded_value + ((10**-&round) / 2))/ "&ts_value"n)-1);
          else 
               distortion = abs(1-((rounded_value - ((10**-&round) / 2))/ "&ts_value"n));
            end;
       by &by_vars_con_q quarterly_period ;
       run;
       %error_check;

	   

       /**************************************************************************************************
        Create a sas date column 'sas_period' from the character date column .
       ***************************************************************************************************/
       %sasdate_ds(mon_subset, ts_period);
       %error_check; 

     /*Sort dataset for rank.*/
     proc sort data=mon_subset; by &by_vars_con_q quarterly_period; run;
       %error_check; 

       /**************************************************************************** 
        Rank the distortion values within their groups.
       *****************************************************************************/ 
       proc rank data=mon_subset out= mon_subset_ranked(rename=(distortion=distortion_order)) ties=low descending;
       by &by_vars_con_q quarterly_period;
       var distortion sas_period;
     ranks distortion_rank sas_period_rank; 
       run;
       %error_check;

    /**************************************************************************** 
     Create 'My_Rank'which is a combination of the two ranks created above.
    *****************************************************************************/ 
    data mon_subset_ranked_2(drop=distortion_rank_char sas_period_rank_char);
    set mon_subset_ranked;
      if missing(distortion_order) then do;
         distortion_rank_char=' ';
         sas_period_rank_char=' ';
       end;
      else do;
         distortion_rank_char=left(put(distortion_rank, best32.));
       sas_period_rank_char=left(put(sas_period_rank, best32.));
       end;

    my_rank= input(distortion_rank_char !! sas_period_rank_char,best32.);
    run;
    %error_check; 

    proc sort data=mon_subset_ranked_2 out=mon_subset_ranked_3;
    by &by_vars_con_q quarterly_period decending distortion_rank my_rank;
    run;
    %error_check; 

    /****************************************************************************************** 
     Calculate allocated monthly values and then the final Constrained rounded monthly value.
    *******************************************************************************************/
    data mon_subset_output (keep= &by_vars_con_q ts_period conrounded_value
                           rename=(conrounded_value = "&ts_value"n));
  set mon_subset_ranked_3;
    by &by_vars_con_q quarterly_period ;
       retain count;
       if first.quarterly_period then 
        count = 0;
     count=count+1;
       allocated_value=0;

		/*******************************************************************************************
		Amend2 - change the way in which allocated_value is calculated; to make it consistent with
					function specification, which says:
				 	"Add the PRECISION to the rounded values of the first n quarters in the list, where 
				 	n is the discrepancy divided by the precision
	   ********************************************************************************************/
	    if not missing(quarterly_discrepancy) and count le int(rows_to_tweak) then 
	      do;
	        if quarterly_discrepancy gt 0 then allocated_value= (10**-&round)/*quarterly_discrepancy*/;
			 	else allocated_value= (-1)*(10**-&round);
	      end;
/*
       if not missing(quarterly_discrepancy) and quarterly_discrepancy NE 0 then do;
        if count LE int(rows_to_tweak) then 
           allocated_value=quarterly_discrepancy;
        end;
*/     
       conrounded_value = rounded_value + allocated_value;
    run;
    %error_check; 
      
       /* Store the monthly conrounded output in the output dataset.*/
       data output;
     set  output
           mon_subset_output;
     run;
       %error_check; 

       %let first_valid=0;
       %end;/* rows if test.*/

    %else %do;
        /* No Monthly data passed validation. Set saserror and leave stopsas=0.*/
        %let saserror= WARNING: All 'Monthly' data failed 'CONROUND' validation. No 'Monthly' data will be processed.;
      %error_check; 
        %end;

    %end;/* monthly_data if test.*/

/* Deliver the results.*/
data &real_output(rename=("&ts_value"n="&real_result"n));
set output; 
run;
%error_check; 

/*Re-set the globals.*/
%let input = &real_input;
%let output = &real_output;
%let result = &real_result;

/* Check if there is any output.*/
%if (&first_valid) %then %do;
    /* No output produced. Set saserror and leave stop=0.*/
   %let saserror= WARNING: The 'CONROUND' function has produced ZERO rows in the output.;
   %error_check;
   %end;

%goto exit;
%exit:
%mend conround;


/*%conround(ts_value, 0);*/
           


