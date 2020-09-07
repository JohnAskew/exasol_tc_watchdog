-- name: tc_syslogger
--
-- desc: Accept arguments and write to remote syslog server
--       if remote syslog server is defined to Exasol.
--       The script returns a string 0 if all went well.
--
-- usage: select tc_syslogger(log_identifier,log_topic,message);
--
--        log_identifier is lable showing where the log entry originated.
--               That is where did this log message come from?
--        log_topic is whatever is meaningful to you for describing
---               what this log is about, such as" "INFORMATIONAL".
--        message is the content of your log entry.
--
--
--/
CREATE OR REPLACE PYTHON SCALAR SCRIPT TC_SYSLOGGER(in_identifier varchar(50), in_topic varchar(50), in_msg varchar(200)) RETURNS VARCHAR(300) AS
import os

import sys, syslog

def run(ctx):

    id = ctx.in_identifier
    
    topic = ctx.in_topic
    
    id = (str(id)+':'+str(topic))
    
    if ctx.in_msg is None:
    
        msg = 'Empty message --> log message was not received by tc_syslogger'
        
        return msg
        
    else:
    
        msg = ctx.in_msg
    
    syslog.openlog(ident=id, facility=syslog.LOG_LOCAL1)
    
    syslog.syslog(syslog.LOG_INFO, (msg) + "\n")
    
    return str(0)
/
select tc_syslogger('tc_syslogger','INFO','test');