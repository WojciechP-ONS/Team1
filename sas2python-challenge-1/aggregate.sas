************************************************************************************************************************************************
* Title					:	AGGREGATE macro                                 
* Description			:	Aggregates a time series to a specified periodicity.
*
* Parameters			:	ts_value		-	The column name on the input dataset where the values are held.
*								periodicity	-	Periodicity to aggregate to.
*								basis			-	The obervation basis of the time series.
* Globals accessed	:	Input			-	input dataset name
*								Output		-	output dataset name
*								Results		-	name of the output dataset column to hold the results.
* Macros called		:	error_check
*								subset_by_periodicity
*								val
*								validate_ds
*								sasdate_ds
* Author					:	Krunch
* Date written			:	Oct 2005
*
* Change history		:
*________________________________________________________________________________________________________________________________________________
* 				Date:		UserId:				Description:	
*________________________________________________________________________________________________________________________________________________
* Amend1		17/01/06	phil mason			Applied upcase() to periodicity, since lowercase values were being ignored and causing errors.
* Amend2		10/10/06	Krunch				(Defect- 5350) Monthly to quarterly aggregation not working correctly for 'middle' basis.
* Amend3		24/11/06	gagorr				To get rid of the following warning:  Variable *** has already been defined as numeric.
*													Year. and yyq6. are numeric formats, not character ones.
* Amend4		20/02/07	PS						Fix for defect 23279 - Use standard macro call to get_var_lists to generate by_var variables.
* Amend5		20/08/07	Krunch				Other_dim now used because complex formula were retaining additional character dimensions 
*													which were not part of other_dim - Defect 5602.
* Amend6		20/08/07	Krunch				This is a result of Amend5. Needed to slightly alter Amend5 to use ___pdicity as well 
*													as other_dim if its is in the input data.
*													When called from Unchain for example, the result of the aggregate for monthly and quarterly inputs
*													must contain the ___pdicity column with its input value.
* Amend7		16/10/07	Krunch				DML enhancements.
* Amend8		30/07/08	gagorr				a) Program header update.
*													b) Ref. no. defect 25123. Adjust DML code to get rid of some unwanted messages and at the same time ensure,
*														that DML code is only executed when really necessary.
* Amend9		30/10/08	Krunch/Raf			(Defect- 34340) Monthly to quarterly aggregation not working correctly for 'middle' basis AND by-group processing,
*													plus a few other small things. Read amendment for more information.
* Amend10 		31/10/08	Raf's idea			Quite a few changes introduced that were intended to simplify the code. You can make your own mind up 
*													if that plan worked by checking with the previous version.
* Amend11		16/12/08	Krunch				(Defect- 34628) Aggrgate fails if formula contains 2+ aggregate calls. Interim datasets must be deleted.
* Amend12		30/08/11	powelea				Outputing timeseries that cannot be expanded
* Amend13		05/12/14	brookc				Defect 79058 - DML 16001 reinstated, also tidied up some of the code in that area as nested comments were causing issues
*												with compilation
*_____________________________________________________________________________________________________________________________________________
*
* Notes				:	Be aware that if %aggregate gets called from UNCHAIN, then input dataset can contain more than one periodicity,
*							as UNCHAIN is not called by %call_multi_per.	
************************************************************************************************************************************************;

%macro aggregate(ts_value, periodicity, basis);

	%put %sysfunc(ifc(&putflag=1,ERROR-------- In Macro &sysmacroname ... %sysfunc(time(),time11.2) -------,));
	%let periodicity= %upcase(&periodicity);
	%let basis= %upcase(&basis);

	%put ts_value= &ts_value;
	%put periodicity= &periodicity;
	%put basis= &basis;
	%put input= &input;
	%put output= &output;

	/* Amend7: DML function parameter validation. */
	%let message_code=;

	%if &execmode eq 1 %then %do;

	   /**************************************************************************************
	    Read the label on input dataset. (do this before Prepare function.)
	    If the calling macro has been called directly from formula then the label will be blank. 
	    Otherwise it will hold the calling function name.
	   ***************************************************************************************/
		%let dsid=%sysfunc(open(&input,i));
		%if (&dsid eq 0) %then %do;
			%let saserror = ERROR in 'aggregate' function - Cannot open &input SAS table;
			%let stopsas=1;
			%error_check;
		%end;

		%*let agg_label=%sysfunc(attrc(&dsid,label));

		/* Find out if the column '___pdicity ' exists. */
		%let pdicity_exists=%sysfunc(varnum(&dsid,___pdicity));

		%let rc=%sysfunc(close(&dsid));
		%if (&rc gt 0) %then %do;
			%let saserror = ERROR in 'aggregate' function - Cannot close &input SAS table;
			%let stopsas=1;
			%error_check;
		%end;

		%*put agg_label=&agg_label;
		%put pdicity_exists=&pdicity_exists;

		%if &pdicity_exists eq 0 %then %add_pdicity(&input);

		/* Get the preiodicity of the input data. */
		%let input_periodicity=;
		proc sql noprint;
			select unique ___pdicity into :input_periodicity separated by ' '
			from &input
		;
		quit;
		%error_check;

		%put input_periodicity = &input_periodicity;

		/* 
			The aim of the next "%if" statement is to perform the parameter validation just once for entire Aggregate function,
			regardless of the subset of data being currently processed [as Aggregate is a 'multi-periodicity' function].
			For the following "%if" statement to work, it is massively important that macro "call_multi_per" will always process
			any annual data before any other data, then quarterly and finally monthly. Please see the above mentioned macro for
			more details.
		*/
		%if &periodicity ne A and %upcase(&current_function) eq AGGREGATE %then
			/* 
				Only need to do this validation if aggregate was called directly from the parser. 
				If called from another function, assume this validation has been done. 
				If &periodicity eq A, then no point to perform all those checks below as well.
			*/
			%if 
				%index(&input_periodicity,A) gt 0
				or
				&___a_obs eq 0 and %index(&input_periodicity,Q) gt 0
				or
				&___a_obs eq 0 and &___q_obs eq 0 and %index(&input_periodicity,M) gt 0
			%then %do;

				%put Aggregate input validation: ___a_obs=&___a_obs ___q_obs=&___q_obs ___m_obs=&___m_obs ___o_obs=&___o_obs;

				/* Validate input data against the periodicity parameter. */

				%if &___a_obs gt 0 and %index(QM,&periodicity) gt 0 %then %do;
					/* Amend7b: DML function parameter validation. */
					/* 
						WARNING:  [Ref: SAS02010]  The formula periodicity of Monthly is a higher frequency than the Input Time Series which is Annual 
															and no data has been processed. 
					*/
					%if &periodicity eq M %then %let message_code = SAS02010;
						/* 
							WARNING:  [Ref: SAS02012]  The formula periodicity of Quarterly is a higher frequency than the Input Time Series which is Annual 
																and no data has been processed. 
						*/
						%else %let message_code = SAS02012;
					/* Append new DML message and return to call_multi_per. */
					%dml_msg_append;
				%end;
				%if &___q_obs gt 0 and &periodicity eq M %then %do;
					/* Amend7c: DML function parameter validation. */
					/* 
						WARNING:  [Ref: SAS02011]  The formula periodicity of Monthly is a higher frequency than the Input Time Series which is Quarterly 
															and no data has been processed. 
					*/
					%let message_code = SAS02011;
					/* Append new DML message and return to call_multi_per. */
					%dml_msg_append;
				%end;

				/* If non of the input data - periodicity combinations can be processed, then stop SAS. */
				%if not (
					/* 
						Those are the only "workable" scenarios: 
					*/
					&___m_obs gt 0 /* Regardless of the 'periodicity' macro variable. */
					or
					(&___q_obs gt 0 and %index(%str(AQ),%upcase(&periodicity)) gt 0)
					or 
					(&___a_obs gt 0 and %upcase(&periodicity) eq A)
					/*
						Please notice that all combinations with Periodicity parameter being of the higher frequency than the input data 
						periodicity are ruled out. See function spec for more details.
					*/
							) 
				%then %do;
					%put ;
					%put ERROR- ****************************************************************************;
					%put ERROR-  Periodicity incompatibility between the input data and parameter value.;
					%put ERROR-  No data has been output.;
					%put ERROR- ****************************************************************************;
					%put ;
					/* All relevant diagnostic messages should be already added to diagnostic table. */
					/* Amend15: Use the message_code 'JUST_LEAVE', to avoid any extra diagnostic being reported. Requested by end-users.*/
					%let message_code= JUST_LEAVE;
					%let stopsas=1;
					%put message_code=&message_code;
					%error_check;
				%end;
					
			%end /* End of the "%if" statement, that should guarantee that this subset of the code will be executed just once! */;

		;

		/* 
			If the following "%if" statement is executed, then it means that there is at least one "workable" combination
			the input data and the Periodicity parameter. Therefore, even though the currently processed subset of the input data
			fails the parameter validation test, the overall calculation should be continued and the calculation should be moved
			to the next "stage".
		*/
		%if
			/* Non-workable scenarios. */
			(%index(&input_periodicity,Q) gt 0 and %upcase(&periodicity) eq M)
			or 
			(%index(&input_periodicity,A) gt 0 and %index(%str(QM),%upcase(&periodicity)) gt 0)
		%then %return;

	%end /*&execmode eq 1*/;

	/* Amend7: finito! */

	/***********************************************************************
	 Prepare function.  Create lists to call macro 'prepare_function'.
	************************************************************************/  
	%let this=&sysmacroname;
	%let write_flag=0;

	%if %symexist(get_dim_only) %then %do;
		%if &get_dim_only=1 %then %let write_flag=1;
	%end;

	/* Set up Macro lists. */ 
	%let write_list=&ts_value;
	%let write_list = %superq(write_list);

	/* Create list of input parameter(TS only) names. */
	%let var_list=ts_value;
	%let var_list = %superq(var_list);

	/* Create list of input parameter(TS only) values. */
	%let ts_list=&ts_value;
	%let ts_list = %superq(ts_list);

	/* Create list of all input parameter values. */
	%let param_list=&ts_value~&periodicity~&basis;
	%let param_list = %superq(param_list);

	/* Create list of all input parameter types. */
	%let type_list=anyvar~period~basis;
	%let type_list = %superq(type_list);

	/* Validate and evaluate parameters. */
	%prepare_function

	/* Check return codes and flags. */
	%if &write_flag or &return_code or ^&execmode %then %return;
	   
	%put ts_list= &ts_list;
	%put ts_value=&ts_value;

	/************************************************************************
	 End of Prepare function.  
	************************************************************************/

	/* Amend7a: DML function input validation. */
	%dml_stage_validation(diag_stage=FUNCTION_INPUT)
	%if ^&has_some_values %then %return; /* Return to call_multi_per. */

	/**********************************************************************************************
	 If the 'average' basis is required, SAS is too accurate. 
	 Replace this with the 'total' basis and divide the result by 4 or 12.
	***********************************************************************************************/
	%if &basis=AVERAGE %then %let my_basis=TOTAL;
		%else %let my_basis=&basis;
	%put my_basis= &my_basis;

  /* Amend5 - other_dim now used because complex formula were retaining additional character dimensions which were not part of other_dim - Defect 5602.
              Removed %get_var_lists call above and replaced by_vars_agg with the contents of other_dim below. */

	/* Amend6 - Find out if ___pdicity in in the input dataset. */
	%let agg_open=%sysfunc(open(&input,i));
	%let pdicity_num=%sysfunc(varnum(&agg_open,___pdicity));
	%let rc=%sysfunc(close(&agg_open));
	%put pdicity_num=&pdicity_num;

	%if &pdicity_num>0 %then %do;
		%put WARNING- ___pdicity :- its there so use it!;
		%let by_vars_agg=&other_dim ___pdicity;
	%end;
		%else %do;
			%put WARNING- ___pdicity :- its NOT there so DONT use it!;
			%let by_vars_agg=&other_dim;
		%end;
	%put NEW by_vars_agg = &by_vars_agg; 

	%if (&periodicity EQ A) %then %do;
		%let to=year;
		%let my_format = year.;
	%end;
	%if (&periodicity EQ Q) %then %do;
		%let to=qtr;
		%let my_format = yyq6.;
	%end;
	%if (&periodicity EQ M) %then %let to=month; /* no format because you always agg to a lower frequency. */
	%let set=set;

	%if %length(&input_periodicity) eq 1 %then %do;
		proc datasets lib=work nolist;				
			change &input = agg_input_&input_periodicity;
		quit;
		/* If only 1 periodicity in the input,	we need to make sure that all the '_obs' macro variables are correct. */
		%if %upcase(&input_periodicity) eq A %then %do;
			%let ann_obs = %nobs(agg_input_&input_periodicity);
			%let qtr_obs = 0;
			%let mon_obs = 0;
		%end;
			%else %if %upcase(&input_periodicity) eq Q %then %do;
				%let ann_obs = 0;
				%let qtr_obs = %nobs(agg_input_&input_periodicity);
				%let mon_obs = 0;
			%end;
			%else %do;
				%let ann_obs = 0;
				%let qtr_obs = 0;
				%let mon_obs = %nobs(agg_input_&input_periodicity);
			%end;
	%end;
		%else %if %upcase(&current_function) ne AGGREGATE %then %do;
			/* If aggregate was called by other function, then call '%subset_by_periodicity' macro, 
				but only if multiperiodicity input data is to be processed. */
			%subset_by_periodicity(&input);
			/* Align the output datasets from the above step to the correct names (as if called by call_multi_per). */
			proc datasets lib=work nolist;
				%if &mon_obs gt 0 %then change subset_mon = agg_input_m;;
				%if &qtr_obs gt 0 %then change subset_qtr = agg_input_q;;
				%if &ann_obs gt 0 %then change subset_ann = agg_input_a;;
			quit;
		%end;
	%error_check;


	/*********************************************************************************** 
		Aggregate data.
		This loop only exists because of Unchain.
	************************************************************************************/
	%do i=1 %to 3;

		%if &i eq 1 %then %do;
			%let freq=mon;
			%let from=month;
		%end;
			%else %if &i eq 2 %then %do;
				%let freq=qtr;
				%let from=qtr;
			%end;
				%else %let freq=ann;

		%if &&&freq._obs gt 0 %then %do;

			%if &freq eq ann %then %goto _ann_;/* Nothing to do with this data. */

			%let rows=0;

			%if %upcase(&current_function) eq AGGREGATE /* Not from another function. */ %then %do;

				/* Validate the data before aggregate. */
				%validate_ds(agg_input_%substr(&freq,1,1), %substr(&freq,1,1));

				%if %nobs(agg_input_%substr(&freq,1,1)) eq 0 %then %return; /* Return to call_multi_per. */

			%end;

			%sasdate_ds(agg_input_%substr(&freq,1,1), ts_period);

			/* Aggregate the monthly series. */
			%if (&to ne &from) %then %do; /* Only if aggregation is really necessary. */
				/* Sort subset_mon by the by_vars_agg ready for the proc expand. */
				proc sort data=agg_input_%substr(&freq,1,1); 
					by &by_vars_agg sas_period;
				run;
				%error_check;

				/*****************************************************************************************************************
				Amend2.
				Krunch 10/10/06 - 5350
				There is a SAS bug when using observed=middle with from=month to=qtr options in proc expand next.
				The first observation must be removed if it is not a middle month of the resulting qtr before the expand step. 

				Amend9 - Two years later... this Amend2 fix above needs to work for all by-groups as well. DOH! 
				We also need to drop all missing value observations AND all observations regardless of value that are months 3,6,9 or 12 
				from the START of each group as these will give incorrect results in the proc expand later. 
				******************************************************************************************************************/
				%if %upcase(&to)=QTR and %upcase(&my_basis)=MIDDLE %then %do;
					data agg_input_%substr(&freq,1,1) (drop=first_value);
						set agg_input_%substr(&freq,1,1);
							length first_value $ 1;
							by &by_vars_agg;
							retain first_value;
								if first.___pdicity then first_value='0';

								if "&ts_value"n not in(.,.M) and first_value ne '2' and month(sas_period) not in (3,6,9,12) then first_value='1';

								if first_value eq '0' then delete;/* Remove observation.*/
									else first_value='2';
					run;
					%error_check;
				
					proc sort data=agg_input_%substr(&freq,1,1); 
					by &by_vars_agg sas_period;
					run;

					%error_check;
				%end;
		

				proc summary data=agg_input_%substr(&freq,1,1) nway;
 					by &by_vars_agg;
					freq;
  					output out=check_freq ;
			   	run;
				%error_check;

				data timeseries_freq;
					set check_freq;
					where _freq_ < 2;
				run;
				%error_check;
				/*Amend 13 - Check for DML 16001 reinstated at users request. It hadn't been removed properly and was causing looping*/
				%if %nobs(timeseries_freq) > 0 %then %do;
					/*%put Not enough;*/

					data dml_16001_failures /*(keep=&other_dim) */;
						set timeseries_freq;
					run;
					

					%let message_code = SAS16001; 
					%let dml_file=dml_16001_failures;
					%let dml_number=16001;
					%let dml_msg=Time series has only one usable record.;

					%put *****************************;
					%put &dml_msg;
					%put *****************************;
					%create_dml_msg(&dml_file,&dml_number,&dml_msg);
	
					proc sort data=agg_input_%substr(&freq,1,1);
						by  &by_vars_agg;
					run;
					%error_check;

					proc sort data=timeseries_freq;
						by  &by_vars_agg;
					run;
					%error_check;

					data agg_input_%substr(&freq,1,1);
						merge agg_input_%substr(&freq,1,1)(in=a) timeseries_freq(in=b);
						by  &by_vars_agg;
						if a and not b;
					run;
					%error_check;

				
				
						%if %nobs(agg_input_%substr(&freq,1,1)) eq 0 %then %do;
							
							%let message_code = SAS16002;
							%put ;
							%put ERROR- *****************************************************************;
							%put ERROR- *  Aggregate has failed DML validation checkpoint. *; 
							%put ERROR- *****************************************************************;
							%put ;
							%goto output_terminal_dml;
						%end;
				%end;
				%else %do;
					%put plenty;
				
				%end;

/*				 Aggregate the data. */
				proc expand data=agg_input_%substr(&freq,1,1) out=output_&freq from=&from to=&to;
					convert "&ts_value"n / observed=&my_basis method=aggregate;  
					id sas_period; 
					by &by_vars_agg;
				run;
				%error_check;

				/* Change 'sas_period' date back to a character string called 'ts_period'. */
				data output_&freq 
					(
						keep=&by_vars_agg period "&ts_value"n 
						rename=(period=ts_period)
					)
				;
					set output_&freq;
						/*Sort out the real average value. */
						%if &freq eq mon %then %do;
							%if %upcase(&basis) eq AVERAGE %then %do;
								%if &to eq qtr %then "&ts_value"n = "&ts_value"n/3;;
								%if &to eq year %then "&ts_value"n = "&ts_value"n/12;;
							%end;
						%end;
							%else %if &freq eq qtr and %upcase(&basis) eq AVERAGE %then "&ts_value"n = "&ts_value"n/4;;
						period = put(sas_period, &my_format);
				run;
				%error_check;

			%end /*(&to NE month)*/;

				%else %do;
					%_ann_:
					/* Just copy the dataset to the output dataset name and do nothing else. */
					data output_&freq;
						set agg_input_%substr(&freq,1,1)/* (drop=sas_period)*/;
					run;
					%error_check;
				%end;

			/* Load the set variable. */
			%let set = %sysfunc(strip(&set)) output_&freq;

		%end /* &freq._obs gt 0 */;

	%end /* do loop */;

	%output_terminal_dml:
	
	%if &message_code ne %then %do;
		/* This is a show stopping problem. No need to continue processing. Append new DML message and leave SAS. */
		%let stopsas=1;
		%error_check;
	%end;

	/*****************************************************************************
	 Slap all the outputs together and deliver.
	******************************************************************************/
	%if (%length(&set)>3) %then %do;
		/* We have some output. */
		data &output(drop=sas_period rename=("&ts_value"n="&result"n));
			&set;
		run;
		%error_check;
	%end;

	%if %upcase(&current_function) ne AGGREGATE %then 
		%if %length(&input_periodicity) eq 1 %then %do;
			/* Replace the real input dataset name. */
			proc datasets lib=work nolist;				
				change agg_input_&input_periodicity = &input;
			quit;
			%error_check;
		%end;
	
	/* Amend11 - Always delete these 3 interim datasets as SAS will get in a twist if Aggregate is called again within the same formula (e.g. Aggregate(...)+Aggregate(...)).*/
	proc datasets lib=work nolist force;
		delete 
			%if &mon_obs gt 0 %then agg_input_m;
			%if &qtr_obs gt 0 %then agg_input_q;
			%if &ann_obs gt 0 %then agg_input_a;
			/*	&input - this could be needed according to the 'proc datasets' step just above, but need to prove it.*/
	;
	quit;
	%error_check;
	
%mend aggregate;

/* Sample call: %aggregate(S.1313,A, total) */
