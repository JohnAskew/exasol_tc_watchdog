--/
CREATE OR REPLACE PYTHON SCALAR SCRIPT TC_SYSLOGGER(in_identifier varchar(50), in_topic varchar(50), in_msg varchar(200)) RETURNS VARCHAR(300) AS
import os
import sys, syslog
def run(ctx):
    id = ctx.in_identifier
    topic = ctx.in_topic
    id = (str(id)+':'+str(topic))
    msg = ctx.in_msg
    syslog.openlog(ident=id, facility=syslog.LOG_LOCAL1)
    syslog.syslog(syslog.LOG_INFO, (msg) + "\n")
/
select tc_syslogger('tc_syslogger','INFO','test msg');