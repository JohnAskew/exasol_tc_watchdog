--
-- name: tc_watchdog.sql
--
-- desc: Configuraable script to kill transaction conflicts and log
--       an entry in the tc_log table (created in this script).
--       If there are no current transaction conflicts, then runnning 
--       this script will not impact anything; it will error out 
--       if no transaction conflicts are found meeting the 
--       arguements criteria. 
--
-- configurations:
--      armed = Whether to actually kill the transaction conflict. 
--              false is "safe mode" where nothing will be killed.
--              true will actually kill a session causing the transction conflict.
--      aggressive_mode = What kind of EXA_DBA_SESSIONS.status to target for termination.
--              IDLE means only kill blocking sessions that are in IDLE status.
--              EXECUTE SQL means kill blocking sessions that are currently active.
--              ALL means kill any blocking transaction
--      wait_time = Seconds session has been in blocking sessions.   
--
-- usage: EXECUTE SCRIPT tc_watchdog(armed, aggressive_mode, wait_time) <with output>;
--              where <with output> is optional.
--        
-- usage notes:
--       If you are currently executing a long running job and other transactions are 
--       being blocked by the long running job, this script can kill the long job.
--       The script will look for transaction_conflicts that have no STOP_TIME
--       (currently blocking) in the EXA_DBA_TRANSACTION_CONFLICTS table. 

--       The conflict_session_id will be targeted for termination
--       if:
--          1. The EXA_DBA_SESSIONS.status field matches the variable aggressive_mode
--          2. The variable wait_time is less than NOW - The EXA_DBA_TRANSACTION_CONFLICTS.start_time
--          3. The variable armed is set to true. If armed is set to false,
--             it will display what would have happened but will not 
--             write to the log.

--       If you are running the script with aggressive_mode = EXECUTE SQL, it
--       will only kill active SQL and leave the sessions in IDLE status alone.
--       Running with aggressive_mode = 'ALL' will kill any blocking session.
--
-- For explanation of transaction conflicts, see:
--       https://www.exasol.com/portal/pages/viewpage.action?pageId=22518143
--       
--=============================================================================

--/
CREATE OR REPLACE LUA SCRIPT tc_watchdog(in_schema, in_armed, in_aggressive, in_wait)

AS 

--#####################################
--# HOUSEKEEPING
--#####################################

--log_schema = 'BIAA_PUBLISH'

--=====================================
local function get_date() 
--=====================================

    local suc_date, res_date = pquery([[select current_timestamp]])
    
    if suc_date then
    
        local return_date = "'"..res_date[1][1].."'"
        
        return return_date
        
    else
    
        return -1
        
    end -- end if
    
end -- end function


--=======================================
-- Capture runtime session_id to report
-- if the script execution failed due
-- to inappropriate arguments being passed 
-- to the script.
--
-- This variable is nice to have but is
-- not critical to the core functionality.
--=======================================

local suc_sess, res_sess = pquery([[select to_char(current_session)]])

if #res_sess then

    my_sess = res_sess[1][1]
    
else

    return -1
    
end
---------------------------------------
-- local variables and tables
---------------------------------------
my_session ='unknown'

sess_hold = 0

user_hold = ''

status_hold = ''

command_hold = ''

duration_hold = 0

session_list = {}

reason_hold = 'tc_watchdog'

reason_no_transaction_conflicts = 'No open transaction conflicts found meeting input criteria'

reason_failed = 'tc_watchdog unable to kill session'

reason_invalid_schema = ('Not run --> Argument {schema} invalid. Read in '..tostring(in_schema)..[[. Must enter existing schema.]]) 

reason_invalid_schema_missing = ('Not run --> Argument {schema} missing. Must enter existing schema.') 

reason_invalid_input_armed = ('Not run --> Argument {armed} invalid. Read in '..tostring(in_armed)..[[. Values must be TRUE or FALSE.]])

reason_invalid_input_aggressive = ('Not run --> Argument {aggressive_mode} invalid. Read in '..tostring(in_aggressive)..[[. Values must be IDLE, EXECUTE SQL, or ALL.]] )

reason_invalid_input_wait_time = ('Not run --> Argument {wait_time} invalid.  Read in '..tostring(in_wait)..[[. Values must be a number >= 0. Did you enclose the number in quotes?]])

schema_valid = false

armed_valid = false

aggressive_valid = false

wait_valid = false

valid_aggressive = {'IDLE', 'EXECUTE SQL', 'ALL'} --Do not change me unless you know what you are doing

--=====================================
local function isempty(s)
--=====================================
       
      output("Reporting Schema will be  "..s)
                
       return s
    

end

--=====================================
function log_insert(...)
--=====================================

    log_insert_list = {...}
    
    local hold_date     = get_date()
    
    local log_insert_type  = log_insert_list[1][1]
    
    local my_sess       = log_insert_list[1][2]
    
    local sess_hold     = log_insert_list[1][3]
    
    local reason_hold   = log_insert_list[1][4]
    
    local user_hold     = log_insert_list[1][5]
    
    local status_hold   = log_insert_list[1][6]
    
    local command_hold  = log_insert_list[1][7]
    
    local duration_hold = log_insert_list[1][8]
    
    my_sql_text = [[INSERT INTO tc_log values (]]
                   
                  ..hold_date
                   
                  ..[[,]]
      
                   ..my_sess
                    
                   ..[[,]]
                    
                   ..sess_hold
                    
                   ..[[,']]
                    
                   ..reason_hold
                    
                   ..[[',']]
                    
                   ..user_hold
                    
                   ..[[',']]
                    
                   ..status_hold
                    
                   ..[[',']]
                    
                   ..command_hold
                    
                   ..[[',]]
                    
                   ..duration_hold
                    
                   ..[[)
                    
                   ]]
                    
      suc_li, res_li = pquery(my_sql_text)
                  
       if not suc_li then
                  
           output("Kill_session "
           
                  ..sess_hold
                  
                  .." failed to insert log for invalid value for "
                  
                  ..log_type
                  
                  )
                      
       end -- end if
end


--#######################################
--# BEGIN VALIDATING INPUT PARAMETERS / ARGUMENTS
--#######################################
-----------------------------------------
-- Defaults for inputs 
-----------------------------------------
-- (template with default parameters and arguments)
---Reason: Set up safe mode in case the script
--         is run with inappropriate script arguments.
--         We build a non-destructive argument 
--         list to be used as the template or 
--         default settings at runtime.
--         At runtime, we instantiate a copy
--         of the template and valiate the script
--         inputs. The script inputs become
--         active if they pass the edit tests,
--         otherwise, our safety net is to use
--         the default settings which do not
--         kill any sessions, only report
--         as if they did.


local inputs = {schema = 'DUMMY', armed = false, aggressive_mode = 'IDLE', wait_time = 864000}  -- Template or Default input object

--inputs.schema = DUMMY                    -- Default: DUMMY. This ensures the user
                                           --          enters a valid schema to own
                                           --          the TC_LOG table for reporting.

--inputs.armed = false                     -- Default: false - don't actually kill session, 
                                           --                  just report as if you did
--                                         --          true - kill the session and report it
--
--inputs.aggressive_mode = 'IDLE'          -- Default: IDLE = Only kill IDLE sessions
--                                         --          EXECUTE SQL = Only kill active SQL
--                                         --          ALL - Kill anything blocking, IDLE or EXECUTE SQL, etc
--
--inputs.wait_time = 300                   -- Seconds query has been in transaction conflict state. Base on 
                                           --          field EXA_DBA_TRANSACTION_CONFLCTS.start_time

inputs.whoami = function (self) output("Session Runtime INFO: "
                                       
                                       ..my_sess
                                        
                                       .." ran with options -->  schema: "
                                       
                                       ..self.schema
                                       
                                       .."    aggressive_mode  is  "
                                       
                                       ..self.aggressive_mode
                                       
                                       .."      wait_time is  "
                                       
                                       ..self.wait_time)
                                       
                end 

--=======================================
-- Input valiation: Build input validation object
--=======================================
-----------------------------------------
-- Validate schema
-----------------------------------------

local suc_sch, res_sch = pcall(isempty, in_schema)

assert(suc_sch, reason_invalid_schema_missing)

--=======================================
-- Instantiate runtime object (table)
--=======================================

local runtime = inputs                   -- Our input validation table which will 
                                         -- hold our runtime arguments as they 
                                         -- are validated in the next step

---------------------------------------
-- Set the reporting SCHEMA
---------------------------------------
 
runtime.schema = in_schema 

---------------------------------------
-- Try an open schema
---------------------------------------

assert(pquery([[OPEN SCHEMA ::lsch]],{lsch = runtime.schema}) 
                                       
       , "Error on opening schema "
                                       
       ..runtime.schema
                                       
       ..". Aborting with no action taken"
                                       
       )       


schema_valid = true -- do I even need this?

---------------------------------------
-- Create a log table if not exists. 
---------------------------------------

local succ_cr_tbl, res_cr_tbl = pquery([[CREATE TABLE IF NOT EXISTS tc_log(kill_date timestamp

                                       , script_session varchar(20)
                                       
                                       , killed_session_id varchar(20)
                                       
                                       , reason varchar(300)
                                       
                                       , user_name varchar(50)
                                       
                                       , status varchar(50)
                                       
                                       , command varchar(50)
                                       
                                       , duration varchar(50))
                                       
                                       ]])

assert(succ_cr_tbl, "Unable to allocate tc_log table to log results. Aborting")
  


if (string.upper(tostring(in_armed)) == 'FALSE'  or string.upper(tostring(in_armed)) == 'TRUE') then

    armed_valid = true

    if in_armed then
   
        runtime.armed = true
        
    end  -- end if
    
end  -- end if

runtime.aggressive_mode = in_aggressive

aggressive_valid = false

for _, value in ipairs(valid_aggressive) do

     if value == in_aggressive then
           
         aggressive_valid = true

     end -- end if
     
end -- end for

runtime.wait_time = in_wait                        -- Seconds query has been in blocking.

wait_valid = false
   
if type(runtime.wait_time) == 'number' then
    
     if runtime.wait_time >= 0 then  
      
         wait_valid = true
     
     end -- end if
     
end -- end -if

if not armed_valid then

     sess_hold = 0
     
     log_type = 'armed'
         
     reason_hold = reason_invalid_input_armed
     
     log_list = {log_type
     
               , my_sess
               
               , sess_hold
               
               , reason_hold
               
               , user_hold
               
               , status_hold
               
               , command_hold
               
               , duration_hold
               
                }
     
     log_insert(log_list)
     
     error([[Would ]]..reason_invalid_input_armed)
    
end
    
if not aggressive_valid then 
     
    sess_hold = 0
        
    if runtime.armed then
        
        log_type = 'aggressive'
         
        reason_hold = reason_invalid_input_aggressive
     
        log_list = {log_type
                  
                  , my_sess
                  
                  , sess_hold
                  
                  , reason_hold
                  
                  , user_hold
                  
                  , status_hold
                  
                  , command_hold
                  
                  , duration_hold
                  
                  }
     
        log_insert(log_list)
     
     end -- end if
                  
     error([[Would ]]..reason_invalid_input_aggressive)
       
end -- end if
       
if ( not wait_valid ) then -- or not wait_valid ) then

    if runtime.armed then
       
        log_type = 'wait_time'
         
        reason_hold = reason_invalid_input_wait_time
     
        log_list = {log_type
                  
                  , my_sess
                  
                  , sess_hold
                  
                  , reason_hold
                  
                  , user_hold
                  
                  , status_hold
                  
                  , command_hold
                  
                  , duration_hold
                  
                  }
     
        log_insert(log_list)
      
      end -- end if
                  
      error([[Would ]]..reason_invalid_input_wait_time)
       
end -- end if
       

--=====================================
-- Debug display of runtime inputs.
-- Will display session runtiime info
-- if the EXECUTE SCRIPT is run with 
-- the optional suffix "with output".
--=====================================

runtime:whoami()

--=====================================
local function kill_session(...)
--=====================================
    local kill_session_list = {...}
    
    --
    -- Do NOT make these variables local
    -- unless you know what you are doing!
    --
       
    sess_hold    = kill_session_list[1][1]
       
    user_hold    = kill_session_list[1][2]
       
    status_hold  = kill_session_list[1][3]
       
    command_hold = kill_session_list[1][4]
       
    duration_hold= kill_session_list[1][5]

    if runtime.armed then
        
        suc2_ks, res2_ks = pquery([[kill session ]]..sess_hold)
        
        assert(suc2_ks, "Function kill_session: Armed mode but failed to kill session "
        
               ..sess_hold
               
               .." Aborting now!"
               )
            
        query([[commit;]])
        
        my_log_text = ('Script session '
        
                       ..my_sess
                       
                       ..' : killed  '
                       
                       ..sess_hold
                       
                       ..' : reason '
                       
                       ..reason_hold
                       
                       ..' : user '
                       
                       ..user_hold
                       
                       ..' : sql_status  '
                       
                       ..status_hold..' : sql '
                       
                       ..command_hold
                       
                       ..' : wait '
                       
                       ..duration_hold)
        
        suc_log,res_log = pquery([[select tc_syslogger('tc_watchdog', 'INFO', :mlt) from dual;]],{mlt=my_log_text})
                
    else
        
        suc2_ks, res2_ks = pquery([[select 'kill session ']])
        
        assert(suc2_ks, "Function kill session running in unarmed mode, failed to execute faux sql. Fix the query. Aborting now!")
        
        output("Would have killed session "..sess_hold)
            
    end -- end if
        
    if (suc2_ks and runtime.armed) then
        
        log_list = {log_type
        
                   , my_sess
                   
                   , sess_hold
                   
                   , reason_hold
                   
                   , user_hold
                   
                   , status_hold
                   
                   , command_hold
                   
                   , duration_hold
                   
                   }
     
        log_insert(log_list)

        output("killed session "..sess_hold)
        
    end  -- end if
        
end -- end function 


--#####################################
-- MAIN LOGIC starts with bring status current
--#####################################

query([[FLUSH STATISTICS;]])

--
-- Gat all current Conflicting Transactions with IDLE/ACTIVE sessions
--

if runtime.aggressive_mode == 'ALL' then

    suc_sl, session_list = pquery([[with SUBR as (select

	to_char(s.SESSION_ID) as SESSION_ID
			
	, s.USER_NAME
			
	, s.STATUS
			
	, s.COMMAND_NAME
			
	,seconds_between((select systimestamp), tc.start_time) as my_duration
		    
	from
			    
		exa_dba_sessions s 
			    
	    join exa_dba_transaction_conflicts tc on s.session_id =tc.conflict_session_id
			
			where to_char(s.session_id) in (select to_char(conflict_session_id) 
			
			                from exa_dba_transaction_conflicts 
			                
			                  where stop_time IS NULL
			              ) 
			 
		     and s.session_id != '4'
		     
		     and s.temp_db_ram > 0 )
		     
	select SUBR.session_id
	
	, SUBR.user_name
	
	, SUBR.status
	
	, SUBR.command_name
	
	, SUBR.my_duration 
	
	  from SUBR 
	  
	    where SUBR.my_duration > :wt
	    
	  ]],{wt = runtime.wait_time})
	  
else 
    
    suc_sl, session_list = pquery([[with SUBR as (select
 
    to_char(s.SESSION_ID) as SESSION_ID
			
	, s.USER_NAME
			
	, s.STATUS
			
	, s.COMMAND_NAME
			
	,seconds_between((select systimestamp), tc.start_time) as my_duration
		    
	from
			    
		exa_dba_sessions s 
			    
	    join exa_dba_transaction_conflicts tc on s.session_id =tc.conflict_session_id
			
		where to_char(s.session_id) in (select to_char(conflict_session_id) 
		                                 
		                                   from exa_dba_transaction_conflicts 
		                                    
		                                      where stop_time IS NULL
		                                ) 
			 
		  and s.status = :am
		     
		  and s.session_id != '4'
		     
		  and s.temp_db_ram > 0 )
		     
	select SUBR.session_id
	
	, SUBR.user_name
	
	, SUBR.status
	
	, SUBR.command_name
	
	, SUBR.my_duration 
	
	  from SUBR 
	  
	    where SUBR.my_duration > :wt
	    
	  ]],{am = runtime.aggressive_mode, wt = runtime.wait_time})
	 
end -- end if

assert(suc_sl, "Section Main Logic--> Running Aggressive mode "

       ..runtime.aggressive_mode
       
       ..". Query to extract Trans Conflicts failed. Aborting now!"
       
       )

--
-- Technically, we don't need the next if statement, but leaving in
-- in case someone removes the previous assert statement to catch suc_s1 = false.
--
	  
if suc_sl then

    if (#session_list == 0 and not runtime.armed) then
    
        sess_hold = 0
      
        reason_no_transaction_conflicts = (reason_no_transaction_conflicts
        
                                           .." --> "
                                           
                                           ..runtime.aggressive_mode
                                           
                                           .." : "
                                           
                                           ..runtime.wait_time
                                           
                                          )
    
        error(reason_no_transaction_conflicts)
        
    end -- end if

    if (#session_list == 0 and runtime.armed) then
    
        sess_hold = 0
      
        reason_no_transaction_conflicts = (reason_no_transaction_conflicts
                                           
                                           .." --> "
                                           
                                           ..runtime.aggressive_mode
                                           
                                           .." : "
                                           
                                           ..runtime.wait_time
                                          )
 
        reason_hold = reason_no_transaction_conflicts
    
        log_list = {log_type, my_sess, sess_hold, reason_hold , user_hold , status_hold, command_hold, duration_hold}
     
        log_insert(log_list)
           
        error (reason_no_transaction_conflicts)
    
    end -- end if
    
    --
    -- This next statement is a loop
    --

    for snum = 1, #session_list do
          
       --
       -- Here is what we are sending kill_session: sess_hold, user_hold, status_hold, 
       --    command_hold, duration_hold. 
       -- See the definition of session_list at top of script. 
       --    If you wish to pass more parameters, simply 
       --    add them to the session_list definition and then the additional code 
       --    inside function kill_session.
       --
       
       local suc_ks, res_ks = pcall (kill_session, session_list[snum])
       
       assert(suc_ks, "Script error on line: local suc_ks, res_ks = pcall (kill_session, session_list[snum])-->Unable to kill and log session "
             
             ..session_list[snum][1]
             
             ..". Aborting now."
            
             )
       
   end -- end for
   
   
end -- end if

/
--=====================================
-- TESTING
--=====================================
--[[ 
--  Testing: Does not execute, only displays runtime info and what would have been killed.
--            It will generate an error if no transaction conflict is found meeting
--            the criteria (arguments) specified. Obviously the script fails
--            if an invalid arguement is specified.
--]]
-- Testing in_armed = False
--execute script tc_watchdog('',false, 'IDEL', 86400) with output;
--execute script tc_watchdog('BIAA_PUBLISH',false, 'IDEL', 86400) with output;
--execute script tc_watchdog('BIAA_PUBLISH',false, 'IDLE', 'apple') with output;
--execute script tc_watchdog('BIAA_PUBLISH',false, 'ALL', '10') with output;
--execute script tc_watchdog('BIAA_PUBLISH',false, 'IDLE', 86400) with output;
--execute script tc_watchdog('BIAA_PUBLISH',false, 'IDLE', 300) with output;
--execute script tc_watchdog('BIAA_PUBLISH',false, 'IDLE', 10) with output;
--execute script tc_watchdog('BIAA_PUBLISH',false, 'EXECUTE SQL', 86400) with output;
--execute script tc_watchdog('BIAA_PUBLISH',false, 'EXECUTE SQL', 300) with output;
--execute script tc_watchdog('BIAA_PUBLISH',false, 'EXECUTE SQL', 10) with output;
--execute script tc_watchdog('BIAA_PUBLISH',false, 'ALL', 86400) with output;
--execute script tc_watchdog('BIAA_PUBLISH',false, 'ALL', 300) with output;
--execute script tc_watchdog('BIAA_PUBLISH',false, 'ALL', 10) with output;

--[[ 
--  Testing: These will fail and write failure to log 
--   You should see 4 failure entries in the log 
--]]
--execute script tc_watchdog('BIAA_PUBLISH','Ture', 'ALL', 10) with output;
--execute script tc_watchdog('BIAA_PUBLISH',true, 'IDEL', 10) with output;
--execute script tc_watchdog('BIAA_PUBLISH',true, 'BOTH', 10) with output;
--execute script tc_watchdog('BIAA_PUBLISH',true, 'ALL','X' ) with output;

--[[
--  Tesing: Will execute with with errors, but writes to log 
--  You should see 3 entries in the log 
--]]
--execute script tc_watchdog('BIAA_PUBLISH',true, 'IDLE', 864000) with output;
--execute script tc_watchdog('BIAA_PUBLISH',true, 'EXECUTE SQL', 864000) with output;
--execute script tc_watchdog('BIAA_PUBLISH',true, 'ALL', 864000) with output;

--=====================================
--[[ PRODUCTION ]]
--=====================================
execute script tc_watchdog('BIAA_PUBLISH',false, 'ALL', 300) with output;
