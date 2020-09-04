# exasol_tc_watchdog
# name:
       tc_watchdog.sql
       
# desc: 
       Configuraable script to kill transaction conflicts and log and entry in the tc_log table (created in this script).
       If there are no current transaction conflicts, then runnning this script will not impact anything; it will complete 
       successfully without taking any action
       
# configurations:
    armed
      Whether to actually kill the transaction conflict. 
      False is "safe mode" where nothing will be killed. 
      True will actually kill a session causing the transction conflict.
    
    aggressive_mode
      What kind of EXA_DBA_SESSIONS.status to target for termination.
        IDLE means only kill blocking sessions that are in IDLE status.
        EXECUTE SQL means kill blocking sessions that are currently active.
        ALL means kill any blocking transaction
      
    wait_time = Seconds query has been in blocking sessions.   
      
# usage: 

EXECUTE SCRIPT tc_watchdog (armed,  aggressive_mode,  wait_time)

# usage notes:

     If you are currently executing a long running job and other transactions are being 
     blocked by the long running job, this script can kill the long job. The script will 
     look for transaction_conflicts that have no STOP_TIME (currently blocking) in the EXA_DBA_TRANSACTION_CONFLICTS table. 

     The conflict_session_id will be targeted for termination
     if:
        1. The EXA_DBA_SESSIONS.status field matches the variable aggressive_mode
        2. The variable wait_time is less than NOW - The EXA_DBA_TRANSACTION_CONFLICTS.start_time
        3. The variable armed is set to true. If armed is set to false, it will report what would have happened.

     If you are running the script with aggressive_mode = EXECUTE SQL, 
     it will only kill active SQL and leave the sessions in IDLE status alone. 
     Running with aggressive_mode = 'ALL' will kill any blocking session.
