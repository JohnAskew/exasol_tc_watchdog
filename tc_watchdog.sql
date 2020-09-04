-- name: tc_watchdog.sql
--
-- desc: Configuraable script to kill transaction conflicts and log
--       and entry in the tc_log table (created in this script).
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
--      wait_time = Seconds query has been in blocking sessions.   
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
--             it will report what would have happened.

--       If you are running the script with aggressive_mode = EXECUTE SQL, it
--       will only kill active SQL and leave the sessions in IDLE status alone.
--       Running with aggressive_mode = 'ALL' will kill any blocking session.
--
--=============================================================================

--/
CREATE OR REPLACE SCRIPT tc_watchdog(in_armed, in_aggression, in_wait)

AS 
----=====================================
---- SETTINGS and SETTINGS Validation
----=====================================

valid_aggression = {'IDLE', 'EXECUTE SQL', 'ALL'} --Do not change me unless you know what you are doing

armed = false                          -- Default: false - don't actually kill session, just report as if you did
                                       --          true - kill the session and report it

aggressive_mode = 'IDLE'               -- Default: IDLE = Only kill IDLE sessions; EXECUTE SQL = kill active sessions
                                       --          EXECUTE SQL = Only kill active SQL
                                       --          ALL - Kill anything blocking, IDLE or EXECUTE SQL, etc.

wait_time = 300                        -- Seconds query has been in transaction conflict state. Base on 
                                       --          field EXA_DBA_TRANSACTION_CONFLCTS.start_time

if in_armed then
   
    armed = true
    
else

    armed = false                       -- Set to true (lowercase) to actually kill sessions, otherwise, just pretend to and report.
    
end

for _, value in ipairs(valid_aggression) do

     if value == in_aggression then
     
         aggressive_mode = in_aggression

     end -- end if
     
end -- end for

if (tonumber(in_wait) and in_wait >= 0) then  

      wait_time = in_wait                        -- Seconds query has been in running/IDLE. See EXA_DBA_SESSIONS.duration

end -- end -if


--=====================================
-- local variables and tables
--=====================================

local session_list = {}

local sess_hold = 0

local stmt_hold = 0

local reason_hold = 'tc_watchdog'

local reason_failed = 'tc_watchdog unable to kill session'

local user_hold = ''

local status_hold = ''

local command_hold = ''

local duration_hold = 0

--=====================================
-- Functions
--=====================================
---------------------------------------
local function kill_session(sess_hold, user_hold, status_hold, command_hold, duration_hold)
---------------------------------------

    if armed then
        
        suc1_ks, res1_ks = pquery([[kill session ]]..sess_hold)
            
        query([[commit;]])
            
    else
        
        suc1_ks, res1_ks = pquery([[select 'kill session ']])
        
        output("Would have killed session "..sess_hold)
            
    end -- end if
        
    if suc1_ks then
        
        my_sql_text = [[INSERT INTO tc_log values ((select current_timestamp),]]..sess_hold..[[,']]..reason_hold..[[',']]..user_hold..[[',']]..status_hold..[[',']]..command_hold..[[',]]..duration_hold..[[)]]
       
        if armed then
        
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
            
                my_sql_text = [[INSERT INTO tc_log values ((select current_timestamp),]]..sess_hold..[[,']]..reason_fail..[[',']]..user_hold..[[',']]..status_hold..[[',']]..command_hold..[[',]]..duration_hold..[[)]]
 
                query(my_sql_text)
    
    end -- end if
     
end -- end function 

--=====================================
-- Create a log table if not exists
--=====================================

local succ_cr_tbl, res_cr_tbl = pquery([[CREATE TABLE IF NOT EXISTS tc_log(kill_date timestamp, session_id varchar(20), reason varchar(100), user_name varchar(50), status varchar(50), command varchar(50), duration varchar(50))]])

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

if aggressive_mode == 'ALL' then

    suc_sl, session_list = pquery([[with SUBR as (select

			to_char(SESSION_ID) as SESSION_ID
			
			, USER_NAME
			
			, STATUS
			
			, COMMAND_NAME
			
			,right(duration,2) + 60*regexp_substr(duration, '(?<=:)[0-9]{2}(?=:)') + 3600 * regexp_substr(duration, '^[0-9]+(?=:)') as my_duration
		    
		    from
			    
			    exa_dba_sessions
			
			where to_char(session_id) in (select to_char(conflict_session_id) from exa_dba_transaction_conflicts where stop_time IS NULL) 
			 
 		      and session_id != '4'
		     
		      and temp_db_ram > 0 )
		     
	select SUBR.session_id
	
	, SUBR.user_name
	
	, SUBR.status
	
	, SUBR.command_name
	
	, SUBR.my_duration 
	
	  from SUBR 
	  
	    where SUBR.my_duration > :wt
	    
	  ]],{wt = wait_time})
	  
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
	    
	  ]],{am = aggressive_mode, wt = wait_time})
	 
end -- end if
	  
if suc_sl then

    for snum = 1, #session_list do
    
       sess_hold    = session_list[snum][1]
       
       user_hold    = session_list[snum][2]
       
       status_hold  = session_list[snum][3]
       
       command_hold = session_list[snum][4]
       
       duration_hold= session_list[snum][5]
       
       suc = kill_session(sess_hold, user_hold, status_hold, command_hold, duration_hold)
       
   end
    
end

/

execute script tc_watchdog(false, 'ALL', 300) with output;