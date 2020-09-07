-- name: tc_watchdog.sql
--
-- desc: Configuraable script to kill transaction conflicts and log
--       an entry in the tc_log table (created in this script).
--       If there are no current transaction conflicts, then runnning 
--       this script will not impact anything; it will complete 
--       successfully without taking any action.
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
-- usage: EXECUTE SCRIPT tc_watchdog(armed, aggressive_mode, wait_time) <with output>; -- where <with output> is optional.
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
--=============================================================================

--/
CREATE OR REPLACE LUA SCRIPT tc_watchdog(in_armed, in_aggressive, in_wait)

AS 

--suc,res = pquery([[select logger('tc_watchdog', 'INFO', 'Test Message') from dual;]])
--=====================================
-- local variables and tables
--=====================================
my_session ='unknown'

sess_hold = 0

user_hold = ''

status_hold = ''

command_hold = ''

duration_hold = 0

session_list = {sess_hold = 0, user_hold = "", status_hold = "", command_hold = "", duration_hold = 0}

reason_hold = 'tc_watchdog'

reason_failed = 'tc_watchdog unable to kill session'

reason_invalid_input_armed = ('Not run--> Argument {armed} invalid -- must be true or false. Read in '..tostring(in_armed))

reason_invalid_input_aggressive = ('Not run --> Argument {aggressive_mode} invalid. Read in '..tostring(in_aggressive))

reason_invalid_input_wait_time = ('Not run --> Argument {wait_time} -- must be numeric & > 0. Read in '..tostring(in_wait))

reason_no_transaction_conflicts = 'No open transaction conflicts found meeting input criteria'

armed_valid = false

aggressive_valid = false

wait_valid = false

valid_aggressive = {'IDLE', 'EXECUTE SQL', 'ALL'} --Do not change me unless you know what you are doing

--=======================================
-- Capture runtime session_id to report
-- if the script execution failed due
-- to inappropriate arguments being passed 
-- to the script.
--=======================================

local suc_sess, res_sess = pquery([[select to_char(current_session)]])

if #res_sess then

    my_sess = res_sess[1][1]
    
end

--=======================================
-- Defaults for inputs 
--=======================================
-- (template with default parameters and arguments)
---Reason: Set up safe mode incase the script
--         is run with inappropriate script arguments.
--         We build a non-destructive argument 
--         list to be used as the template or 
--         default settings at runtime.
--          At runtime, we instantiate a copy
--          of the template and valiate the script
--          inputs. The script inputs become
--          active if they pass the edit tests,
--          otherwise, our safety net is to use
--          the default settings which do not
--          kill any sessions, only report
--          as if they did.


local inputs = {armed = false, aggressive_mode = 'IDLE', wait_time = 300}                        -- Template or Default input table object

--inputs.armed = false                     -- Default: false - don't actually kill session, just report as if you did
--                                         --          true - kill the session and report it
--
--inputs.aggressive_mode = 'IDLE'          -- Default: IDLE = Only kill IDLE sessions; EXECUTE SQL = kill active sessions
--                                         --          EXECUTE SQL = Only kill active SQL
--                                         --          ALL - Kill anything blocking, IDLE or EXECUTE SQL, etc.
--
--inputs.wait_time = 300                   -- Seconds query has been in transaction conflict state. Base on 
                                         --          field EXA_DBA_TRANSACTION_CONFLCTS.start_time

inputs.whoami = function (self) output("Session Runtime INFO: "..my_sess.." ran with options -->  aggressive_mode  is  "..self.aggressive_mode.."      wait_time is  "..self.wait_time) end 

--=======================================
-- Input valiation: Build input validation object
--=======================================

local runtime = inputs                   -- Our input validation table which will 
                                         -- hold our runtime arguments as they 
                                         -- are validated in the next step

--=======================================
-- Script arguments validation
--=======================================

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

runtime.wait_time = in_wait                        -- Seconds query has been in running/IDLE. See EXA_DBA_SESSIONS.duration
   
if type(runtime.wait_time) == 'number' then
    
     if runtime.wait_time >= 0 then  
      
         wait_valid = true
     
     else
     
         wait_valid = false
         
     end -- end if
     
else

    wait_valid = false

end -- end -if

if not armed_valid then

     sess_hold = 0
        
      my_sql_text = [[INSERT INTO tc_log values ((select current_timestamp),]]..my_sess..[[,]]..sess_hold..[[,']]..reason_invalid_input_armed..[[',']]..user_hold..[[',']]..status_hold..[[',']]..command_hold..[[',]]..duration_hold..[[)]]

      suc_ks_ins, res_ks_ins = pquery(my_sql_text)
                  
       if not suc_ks_ins then
                  
           output("Kill_session failed to insert log for invalid value for armed")
                      
       end -- end if
                  
       exit()
    
end

if runtime.armed then
    
    if ( not aggressive_valid ) then -- or not wait_valid ) then
     
        sess_hold = 0
        
        my_sql_text = [[INSERT INTO tc_log values ((select current_timestamp),]]..my_sess..[[,]]..sess_hold..[[,']]..reason_invalid_input_aggressive..[[',']]..user_hold..[[',']]..status_hold..[[',']]..command_hold..[[',]]..duration_hold..[[)]]

         suc_ks_ins, res_ks_ins = pquery(my_sql_text)
                  
         if not suc_ks_ins then
                  
             output("Kill_session failed to insert log for invalid aggressive")
                      
         end -- end if
                  
         exit()
       
     end -- end if
       
     if ( not wait_valid ) then -- or not wait_valid ) then
       
          sess_hold = 0
       
           my_sql_text = [[INSERT INTO tc_log values ((select current_timestamp),]]..my_sess..[[,]]..sess_hold..[[,']]..reason_invalid_input_wait_time..[[',']]..user_hold..[[',']]..status_hold..[[',']]..command_hold..[[',]]..duration_hold..[[)]]

           suc_ks_ins, res_ks_ins = pquery(my_sql_text)
           
           if not suc_ks_ins then
                  
               output("Kill_session failed to insert log for invalid wait_time")
                      
           end -- end if
                  
           exit()
       
       end -- end if
       
end -- end if

--=====================================
-- Debug display of runtime inputs.
-- Will display session runtiime info
-- if the EXECUTE SCRIPT is run with 
-- the optional suffix "with output".
--=====================================

runtime:whoami()

--=====================================
-- Functions
--=====================================
---------------------------------------
local function kill_session(sess_hold, user_hold, status_hold, command_hold, duration_hold)
---------------------------------------

    if runtime.armed then
        
        suc1_ks, res1_ks = pquery([[kill session ]]..sess_hold)
            
        query([[commit;]])
            
    else
        
        suc1_ks, res1_ks = pquery([[select 'kill session ']])
        
        output("Would have killed session "..sess_hold)
            
    end -- end if
        
    if suc1_ks then
        
        my_sql_text = [[INSERT INTO tc_log values ((select current_timestamp),]]..my_sess..[[,]]..sess_hold..[[,']]..reason_hold..[[',']]..user_hold..[[',']]..status_hold..[[',']]..command_hold..[[',]]..duration_hold..[[)]]
       
        my_log_text = ('Script session '..my_sess..' : killed  '..sess_hold..' : reason '..reason_hold..' : user '..user_hold..' : sql_status  '..status_hold..' : sql '..command_hold..' : wait '..duration_hold)
        
        output(my_log_text)
        
        suc_log,res_log = pquery([[select tc_syslogger('tc_watchdog', 'INFO', :mlt) from dual;]],{mlt=my_log_text})
        
        if runtime.armed then
        
            output("killed session "..sess_hold)
                  
            suc_ks_ins, res_ks_ins = pquery(my_sql_text)
                
            if suc_ks_ins then
            
                output("Log table tc_log received this entry:"..my_sql_text)
                    
                query([[commit;]])
                
            else
             
                output("Error on logging the kill session to the table tc_log")
                    
                query([[rollback;]])
                
            end -- end if
            
        else
        
             output("Would have logged "..my_sql_text)
                 
        end  -- end if
        
    else
        
        output("kill_session --> killed session failed!")
            
                my_sql_text = [[INSERT INTO tc_log values ((select current_timestamp),]]..my_sess..[[,]]..sess_hold..[[,']]..reason_failed..[[',']]..user_hold..[[',']]..status_hold..[[',']]..command_hold..[[',]]..duration_hold..[[)]]
 
                query(my_sql_text)
    
    end -- end if
     
end -- end function 

--=====================================
-- Create a log table if not exists
--=====================================

local succ_cr_tbl, res_cr_tbl = pquery([[CREATE TABLE IF NOT EXISTS tc_log(kill_date timestamp, script_session varchar(20), killed_session_id varchar(20), reason varchar(100), user_name varchar(50), status varchar(50), command varchar(50), duration varchar(50))]])

if not succ_cr_tbl then

    local error_txt = "Unable to allocate tc_log table to log results. Aborting"
  
    output("succ_cr_tbl:"..succ_cr_tbl.." Error: " ..error_txt)
  
    exit()
  
end -- end if

--=====================================
-- Bring status current
--=====================================

query([[FLUSH STATISTICS;]])

--=====================================
-- Get Conflicting Transaction Sessions with IDLE/ACTIVE sessions
--=====================================

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
			
			where to_char(s.session_id) in (select to_char(conflict_session_id) from exa_dba_transaction_conflicts where stop_time IS NULL) 
			 
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
			
		where to_char(s.session_id) in (select to_char(conflict_session_id) from exa_dba_transaction_conflicts where stop_time IS NULL) 
			 
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
	  
if suc_sl then

    if (#session_list == 0 and runtime.armed ) then
    
      sess_hold = 0
      
      reason_no_transaction_conflicts = (reason_no_transaction_conflicts.." --> "..runtime.aggressive_mode.." : "..runtime.wait_time)
      
      if runtime.aggressive_mode == 'ALL' then
      
           my_sql_text = [[INSERT INTO tc_log values ((select current_timestamp),]]..my_sess..[[,]]..sess_hold..[[,']]..reason_no_transaction_conflicts..[[',']]..user_hold..[[',']]..status_hold..[[',']]..command_hold..[[',]]..duration_hold..[[)]]

           suc_ks_ins, res_ks_ins = pquery(my_sql_text)
      
      else
      
           my_sql_text = [[INSERT INTO tc_log values ((select current_timestamp),]]..my_sess..[[,]]..sess_hold..[[,']]..reason_no_transaction_conflicts..[[',']]..user_hold..[[',']]..status_hold..[[',']]..command_hold..[[',]]..duration_hold..[[)]]

           suc_ks_ins, res_ks_ins = pquery(my_sql_text)
           
       end -- end if
    
    end -- end if

    for snum = 1, #session_list do
    
       sess_hold    = session_list[snum][1]
       
       user_hold    = session_list[snum][2]
       
       status_hold  = session_list[snum][3]
       
       command_hold = session_list[snum][4]
       
       duration_hold= session_list[snum][5]
       
       suc = kill_session(sess_hold, user_hold, status_hold, command_hold, duration_hold)
       
   end -- end for
   
   
end -- end if

/

execute script tc_watchdog(false, 'ALL', 300) with output;
