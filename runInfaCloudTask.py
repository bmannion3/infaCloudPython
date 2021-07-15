#!/usr/bin/python
# -*- coding: utf-8 -*-

import json  # import json module to work with json response
import urllib  # import urllib to convert arguments to desired format
import urllib2  # import urllib2 to call REST API
import os  # import for operating system commands
import sys  # import system commands
import time as Time  # import time functions
import argparse  # use argparse to parse arguments
import logging  # standard library for logging
from pprint import pprint  # standarad library for pretty print
# from jq import jq    # jq for querying json
import ConfigParser as configparser
import smtplib
import email

from operator import itemgetter  # for iterating through json list returned for activity log
from datetime import *  # For converting timezones as informatica cloud works in PST
#import dateutil.parser as dateparser  # Date parser for converting the string returned in json to date
try:
    from Queue import Queue
except ImportError:
    from Queue import Queue

from email import encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
################################################################################


class PollingException(Exception):
    """Base exception that stores all return values of attempted polls"""
    def __init__(self, values, last=None):
        self.values = values
        self.last = last
################################################################################


class TimeoutException(PollingException):
    """Exception raised if polling function times out"""
################################################################################


class MaxCallException(PollingException):
    """Exception raised if maximum number of iterations is exceeded"""
################################################################################


def check_target_success(val):
    if val != 9999:
        logging.debug('Default result NOT found.')
        return True
################################################################################

def ParseCommandLine():
    """  
    Name: ParseCommandLine() Function                                     
    Desc: Process and Validate the command line arguments                 
    use Python Standard Library module argparse                            
    Input: none                                                           
    Actions:                                                              
    Uses the standard library argparse to process the command line        
    establishes a global variable gl_args where any of the functions can  
    obtain argument information   
    
    """

    # define an object of type parser

    parser = \
        argparse.ArgumentParser(description='Argument parser for informatica cloud run job... runInfaCloudTask'
                                )

    # add argument verbose to the parser to give user ability to see verbose execution messages

    parser.add_argument('-v', '--verbose', action='store_true',
                        help='allows prograss messages to be displayed')

    # add creadentials file option

    parser.add_argument('-c', '--credFile', required=True,
                        help='specifies credentails file name')

    # add job file option

    parser.add_argument('-j', '--jobFile', required=True,
                        help='specifies job information file')

    # add wait time option

    parser.add_argument('-w', '--waitTime', required=False, type=int,
                        help='how many secs to wait while checking task status recursively'
                        )

    parser.add_argument('-i', '--intervalTime', required=False, type=int,
                        help='how many secs to wait between status checks'
                        )

    parser.add_argument('-m', '--maxTries', required=False, type=int,
                        help='how many times to retry to check the status'
                        )
    # create global object that can hold all valid arguments and make it avialable to all functions

    global gl_args

    # save the arguments gl_args

    gl_args = parser.parse_args()

    DisplayMessage('Command Line processing finished: successfully')

    return


################################################################################

def DisplayMessage(msg):
    """
    Name: DisplayMessage() Function 
    Desc: Displays the message if the verbose command line option is present 
    Input: message type string 
    Actions: Uses the standard library print function to display the message 
    
    """

    if gl_args.verbose:
        pprint(msg)


################################################################################

def SendMessage(msg_text, filename):
    config = configparser.ConfigParser()
    config.sections()
    config.read('/app/data/infaCloudPython/config.ini')

    snd_from = config.get('Email', 'from')
    snd_to = config.get('Email', 'to')
    snd_subject = config.get('Email', 'subject')
    snd_filename = gl_fileName

# gl_args.jobFile.split('.')[0] + '_' \
# + datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f') + '.log'

    from email.mime.multipart import MIMEMultipart
    from email.mime.text import MIMEText
    # Create message container - the correct MIME type is multipart/alternative.
    msg = MIMEMultipart('alternative')
    msg['Subject'] = snd_subject + ' - ' + gl_args.jobFile.split('.')[0]
    msg['From'] = snd_from
    msg['To'] = snd_to

    # Create the body of the message (a plain-text and an HTML version).
    # text = str(msg)
    # html = """\
    # <html>
    #   <head></head>
    #   <body>
    #     <p>Below are the results from the most recent running of """ + gl_args.jobFile.split('.')[0] + \
    #     """<br>""" + str(msg) + """</p>
    #   </body>
    # </html>
    # """
    text = str(msg_text)
    html = """\
    <html>
      <head></head>
      <body>
        <p>Below is the end result from the running of: <strong> """ + gl_args.jobFile.split('.')[0] + """<br /><br />""" + \
        str(msg_text) + """</p>
      </body>
    </html>
    """

    # Record the MIME types of both parts - text/plain and text/html.
    part1 = MIMEText(text, 'plain')
    part2 = MIMEText(html, 'html')

    # Attach parts into message container.
    # According to RFC 2046, the last part of a multipart message, in this case
    # the HTML message, is best and preferred.
    msg.attach(part1)
    msg.attach(part2)

    # Open the file in binary mode
    with open(filename, "rb") as attachment:
        # Add file as application/octet-stream
        # Email client can usually download this automatically as attachment
        part = MIMEBase("application", "octet-stream")
        part.set_payload(attachment.read())

    # Encode file in ASCII characters to send by email
    encoders.encode_base64(part)

    # Add header as key/value pair to attachment part
    part.add_header(
        "Content-Disposition",
        "attachment; filename= " + snd_filename,
    )

    # Add attachment to message and convert message to string
    msg.attach(part)


    # Send the message via local SMTP server.
    s = smtplib.SMTP('localhost')
    # sendmail function takes 3 arguments: sender's address, recipient's address
    # and message to send - here it is sent as one string.
    s.sendmail(snd_from, snd_to, msg.as_string())
    s.quit()
################################################################################

def ReadFileToDict(fileName):
    """
    Name: ReadFileToDict() Function 
    Desc: Reads credentials from the supplied file and returns dictionary 
    Input: file name that must be read 
    Actions: Uses the standard library file open function to read data 
    
    """

    # define the dictionary

    keyStore = {}

    DisplayMessage('Reading keys and values from the file ' + fileName)

    try:
        with open(fileName, 'rb') as fileData:
            for line in fileData:
                (key, value) = line.strip().split(':')
                keyStore[key] = value
        fileData.close()
        DisplayMessage('Reading keys and values from the file '
                       + fileName + ' Successful')
        return keyStore
    except Exception, e:
        logging.error('open / read the file provided failed with error '
                       + str(e))
        DisplayMessage('open / read the file provided failed with error '
                        + str(e))
        raise


################################################################################
def time_in_range(start, end, x):
    """

    Name: time_in_range
    Desc: check if the current time is in timerange
    Input: Start Time, end Time, wait time
    Output: Return true if x is in the range [start, end]

    """
    logging.debug('Start: ' + str(start))
    logging.debug('End: ' + str(end))
    logging.debug('Duration: ' + str(x))
    if start <= end:
        return start <= x <= end
    else:
        return start <= x or x <= end


################################################################################

def InfaCloudLogin(url, payload):
    """
    Name: InfaCloudLogin() Function 
    Desc: Takes payload and url and calls login method in Informatica Rest API
    Input: base URL to be used for logging in and the credentails payload data
    Actions: takes url and payload and uses urllib and urllib2 for calling APIs
    Output: Session Id for informatica session
    
    """

    DisplayMessage('Starting informatica Cloud Login module')
    logging.debug('Starting informatica Cloud Login module')

    try:

        # encode the dictionay values in payload to url type arguments using urllib

        payloadEncoded = json.dumps(payload)

        # use urllib2 to form the request

        loginRequest = urllib2.Request(url, payloadEncoded,
                headers={'Content-type': 'application/json',
                'Accept': 'application/json'})

        # open a handle for the API using urllib2 urlopen method

        loginHandle = urllib2.urlopen(loginRequest)

        # The above handle returns a json message which we use to get session id

        for loginLine in loginHandle:
            loginResponse = json.loads(loginLine)

        # close login Handle

        loginHandle.close()

        DisplayMessage('informatica Cloud Login module successfull with Session id: '
                        + loginResponse['icSessionId'])
        logging.info('informatica Cloud Login module successfull with Session id: '
                      + loginResponse['icSessionId'])

        # return session ID output

        return loginResponse['icSessionId']
    except Exception, e:

        logging.error('informatica login module failed with error'
                      + str(e))
        DisplayMessage('informatica login module failed with error'
                       + str(e))
        raise


################################################################################

def InfaRunJob(url, payload, sessionId):
    """
    Name: InfaRunJob() Function 
    Desc: Takes sessionid and url and calls runjob method in Informatica Rest API
    Input: base URL to be used for running job and the job properties payload data
    Actions: takes url, sessioniD and payload and uses urllib and urllib2 for calling APIs
    Output: success or failure for informatica job submission
    
    """

    DisplayMessage('Starting informatica Cloud run job module')
    logging.info('Starting informatica Cloud run job module')

    try:

        # encode the dictionay values in payload to url type arguments using urllib

        payloadEncoded = json.dumps(payload)
        DisplayMessage('encoded arguments are: ' + payloadEncoded)
        logging.debug('encoded arguments are: ' + payloadEncoded)
        logging.debug(url)

        # use urllib2 to form the request

        jobRequest = urllib2.Request(url, payloadEncoded)
        jobRequest.add_header('icSessionId', sessionId)
        jobRequest.add_header('Accept', 'application/json')
        jobRequest.add_header('Content-Type', 'application/json')

        # Uncomment this code for using proxy information
        # opener = urllib2.build_opener(gl_proxy)
        # urllib2.install_opener(opener)

        # Save job kick off time to a global variable for later use
        # kickoffTime = datetime.now(pytz.timezone('US/Pacific'))

        kickoffTime = datetime.now()

        global gl_KickoffTime

        # delete timezone info to make it convenient for time arthmatic

        gl_KickoffTime = kickoffTime.replace(tzinfo=None)

        # let it sleep for couple of secs to make it easier for aritmetic

        Time.sleep(2)

        # open a handle for the API using urllib2 urlopen method

        jobHandle = urllib2.urlopen(jobRequest)

        # The above handle returns a json message which we use to get session id
        for jobLine in jobHandle:
            jobResponse = json.loads(jobLine)
            global gl_Job_Id
            gl_Job_Id = jobResponse['taskId']
            global gl_Job_Num
            gl_Job_Num = jobResponse['runId']

        # close login Handle

        jobHandle.close()

        DisplayMessage('informatica Cloud Login module successfull with Session id: '
                        + sessionId)
        logging.info('informatica Cloud Login module successfull with Session id: '
                      + sessionId)
        DisplayMessage('Informatica Cloud Task ID: ' + str(gl_Job_Id))
        logging.info('Informatica Cloud Task ID: ' + str(gl_Job_Id))

        # return session ID output

        return True
    except urllib2.HTTPError, e:
        logging.error('HTTPError = ' + str(e.code))
        logging.error('HTTPError = ' + str(e.reason))
    except urllib2.URLError, e:
        logging.error('URLError = ' + str(e.reason))
    except Exception, e:
        logging.error('informatica run job module failed with error'
                      + str(e))
        DisplayMessage('informatica run job module failed with error'
                       + str(e))
        raise


################################################################################

def InfaJobStatus(url, payload, jobName, waitTime, credentials):
    """
    Name: InfaJobStatus() Function 
    Desc: Takes payload and url and calls activity log method in Informatica Rest API
    Input: base URL to be used for activitylog and the payload data
    Actions: takes url, sessioniD and payload and uses urllib and urllib2 for calling APIs
    Output: success or failure for informatica current informatica job
    
    """

    DisplayMessage('Starting informatica Cloud activity log module')
    logging.debug('Starting informatica Cloud activity log module')
    returnValue = 9999
    try:

        # Check the status of the session id
        chckdSessionID = InfaValidateToken(gl_sessionID, credentials, waitTime)
        logging.debug('Check session id is now: ' + str(chckdSessionID))
        payload['icSessionId'] = chckdSessionID

        # encode the dictionary values in payload to url type arguments using urllib

        payloadEncoded = json.dumps(payload)
        # DisplayMessage('encoded arguments are: ' + payloadEncoded)
        logging.debug('encoded arguments are: ' + payloadEncoded)

        # since this is get type url, we need to build url first
        logUrl = url + '?' + payloadEncoded

        #DEBUG
        # gl_Job_Id = 24

        logUrl = url + '?taskId=' + str(gl_Job_Id)
        logging.debug(logUrl)

    # open a handle for the API using urllib2 urlopen method (get)

        request = urllib2.Request(logUrl, headers=payload)
        logData = urllib2.urlopen(request)
        data = logData.read()
        j = json.loads(data)
        ###### DEBUG SECTION #######
        # logging.debug('RAW activity:' + str(j))
        # logging.debug('will be looking for job id: ' + str(gl_Job_Id))
        # logging.debug('will be looking for run id: ' + str(gl_Job_Num))
        # Loop to check if values are for gl_Job_Num
        for row in j:
            ###### DEBUG SECTION #######
            # logging.debug('Row object id: ' + str(row['objectId']))
            # logging.debug('Row run id: ' + str(row['runId']))

            if row['objectId'] == gl_Job_Id:
                if row['runId'] == gl_Job_Num:
                    tn = row['type']
                    tid = row['id']
                    tname = row['objectName']
                    exec_state = row['state']
                    rid = row['runId']
                    strt = row['startTime']
                    endt = row['endTime']
                    fsrws = row['failedSourceRows']
                    ssrws = row['successSourceRows']
                    ftrws = row['failedTargetRows']
                    strws = row['successTargetRows']
                    # err_msg = row['errorMsg']

                    logging.debug('Values Parsed: ' + str(tn) + '|' + str(tid) + '|' + str(tname) + '|' + str(exec_state)
                                  + '|' + str(rid))
                    logging.debug('Parsed Start/End: ' + str(strt) + '|' + str(endt))
                    logging.debug('Parsed Rows (source/target) fail/success: ' + str(fsrws) + '|' + str(ssrws) + '|' + str(ftrws)
                                  + '|' + str(strws))

                    if exec_state == 1:
                        DisplayMessage(jobName + ' is successful')
                        logging.info(jobName + ' is successful')
                        logging.info('Successful Source Rows: ' + str(ssrws))
                        logging.info('Successful Target Rows: ' + str(strws))
                        returnValue = 0

                    if exec_state == 2:
                        DisplayMessage(jobName + ' The task completed with errors')
                        logging.error(jobName + ' The task completed with errors')
                        # logging.error('Error Message: ' + err_msg)
                        logging.error('Successful Source Rows: ' + str(fsrws))
                        logging.error('Successful Target Rows: ' + str(ftrws))
                        returnValue = -1

                    if exec_state == 3:
                        DisplayMessage(jobName + ' The task failed to complete')
                        logging.error(jobName + ' The task failed to complete')
                        # logging.error('Error Message: ' + err_msg)
                        logging.error('Successful Source Rows: ' + str(fsrws))
                        logging.error('Successful Target Rows: ' + str(ftrws))
                        returnValue = -1

        return returnValue
        # DEBUG
        # return 0
    except Exception, e:
        logging.error('informatica job status module failed with error'
                      + str(e))
        DisplayMessage('informatica job status module failed with error'
                        + str(e))
        raise


################################################################################
def InfaValidateToken(sessionId, credentials, waittime):
    """

    Name: InfaValidateToken
    Desc: Determines if the session is is still active. Sometimes the Wait time requested is longer then the session.
    Input: session id to be evualted, wait time and the start time of the processing
    Actions: Uses math to determine if a check is needed, then uses API to validate session
    Output: A Valid Session ID

    """
    DisplayMessage('Checking to confirm session still valid')
    logging.debug('Checking to confirm session still valid')
    logging.debug("Wait Time set at: " + str(waittime))
    try:
        payload = {}
        payload['icToken'] = sessionId
        payload['userName'] = credentials.get('username')
        payload['@type'] = 'validatedToken'

        payloadEncoded = json.dumps(payload)
        logging.debug('Validation Request: ' + str(payloadEncoded))

        ValidateTokenRequest = urllib2.Request(gl_validatesession_url, payloadEncoded,
                        headers={'Content-type': 'application/json',
                                 'Accept': 'application/json'})

        # Gateway Default TTL for Tokens is 270 seconds
        if waittime >= 270:
            IsValidResponse = urllib2.urlopen(ValidateTokenRequest)

            # The above handle returns a json message which we use to get valid state of session id
            # logging.debug('Validation check: ' + IsValidResponse.getcode())
            for jobLine in IsValidResponse:
                logDataJson = json.loads(jobLine)

            logging.debug('Validation check: ' + str(logDataJson))

            tue = logDataJson['timeUntilExpire']
            ivt = logDataJson['isValidToken']
            logging.debug('tur: ' + str(tue))
            logging.debug('ivt: ' + str(ivt))

            # close login Handle
            IsValidResponse.close()

            if ivt:
                logging.debug('session token is still valid, checking for duration.')
                start_time = gl_starttime
                current_time = Time.time()

                elapsed_time = current_time - start_time
                logging.debug('Elapsed Time: ' + str(elapsed_time))

                if elapsed_time < tue:
                    logging.debug('session token will still be valid for a while longer')
                    logging.debug('returning original session id')
                    # DisplayMessage('Returning session id.')
                    return sessionId
                else:
                    logging.debug('Session will expire soon, need new session id')
                    logging.debug('Get new informatica login session id')
                    sessionId = InfaCloudLogin(gl_login_url, credentials)
                    logging.info('returning new session id')
                    # DisplayMessage('Returning session id.')
                    return sessionId
            else:
                logging.debug('Session has expired, need new session id')
                logging.debug('Get new informatica login session id')
                sessionId = InfaCloudLogin(gl_login_url, credentials)
                logging.info('returning new session id')
                # DisplayMessage('Returning session id.')
                return sessionId
        else:

            logging.debug('short wait times should not need to be validated.')
            logging.debug('returning original session id')
            # DisplayMessage('Returning session id.')
            return sessionId
            # return session ID output
    except Exception, e:
       logging.error('informatica validate session module failed with error '
                      + str(e))
       DisplayMessage('informatica validate session module failed with error '
                       + str(e))
       raise


################################################################################
def step_constant(step):
    """Use this function when you want the step to remain fixed in every iteration (typically good for
    instances when you know approximately how long the function should poll for)"""
    return step
################################################################################


def step_linear_double(step):
    """Use this function when you want the step to double each iteration (e.g. like the way ArrayList works in
    Java). Note that this can result in very long poll times after a few iterations"""
    return step * 2


################################################################################
def is_truthy(val):
    """Use this function to test if a return value is truthy"""
    return bool(val)


################################################################################
def poll(target, step, args=(), kwargs=None, timeout=None, max_tries=None, check_success=is_truthy,
         step_function=step_constant, ignore_exceptions=(), poll_forever=False, collect_values=None, *a, **k):
    """Poll by calling a target function until a certain condition is met. You must specify at least a target
    function to be called and the step -- base wait time between each function call.
    :param step: Step defines the amount of time to wait (in seconds)
    :param args: Arguments to be passed to the target function
    :type kwargs: dict
    :param kwargs: Keyword arguments to be passed to the target function
    :param timeout: The target function will be called until the time elapsed is greater than the maximum timeout
    (in seconds). NOTE that the actual execution time of the function *can* exceed the time specified in the timeout.
    For instance, if the target function takes 10 seconds to execute and the timeout is 21 seconds, the polling
    function will take a total of 30 seconds (two iterations of the target --20s which is less than the timeout--21s,
    and a final iteration)
    :param max_tries: Maximum number of times the target function will be called before failing
    :param check_success: A callback function that accepts the return value of the target function. It should
    return true if you want the polling function to stop and return this value. It should return false if you want it
    to continue executing. The default is a callback that tests for truthiness (anything not False, 0, or empty
    collection).
    :param step_function: A callback function that accepts each iteration's "step." By default, this is constant,
    but you can also pass a function that will increase or decrease the step. As an example, you can increase the wait
    time between calling the target function by 10 seconds every iteration until the step is 100 seconds--at which
    point it should remain constant at 100 seconds
    >>> def my_step_function(step):
    >>>     step += 10
    >>>     return max(step, 100)
    :type ignore_exceptions: tuple
    :param ignore_exceptions: You can specify a tuple of exceptions that should be caught and ignored on every
    iteration. If the target function raises one of these exceptions, it will be caught and the exception
    instance will be pushed to the queue of values collected during polling. Any other exceptions raised will be
    raised as normal.
    :param poll_forever: If set to true, this function will retry until an exception is raised or the target's
    return value satisfies the check_success function. If this is not set, then a timeout or a max_tries must be set.
    :type collect_values: Queue
    :param collect_values: By default, polling will create a new Queue to store all of the target's return values.
    Optionally, you can specify your own queue to collect these values for access to it outside of function scope.
    :return: Polling will return first value from the target function that meets the condions of the check_success
    callback. By default, this will be the first value that is not None, 0, False, '', or an empty collection.
    """

    logging.info('Starting Polling')

    assert (timeout is not None or max_tries is not None) or poll_forever, \
        ('You did not specify a maximum number of tries or a timeout. Without either of these set, the polling '
         'function will poll forever. If this is the behavior you want, pass "poll_forever=True"')

    assert not ((timeout is not None or max_tries is not None) and poll_forever), \
        'You cannot specify both the option to poll_forever and max_tries/timeout.'

    kwargs = kwargs or dict()
    values = collect_values or Queue()

    max_time = Time.time() + timeout if timeout else None
    tries = 0
    logging.debug('Max Time: ' + str(max_time))
    logging.debug('Max Tries: ' + str(max_tries))

    last_item = None
    while True:

        if max_tries is not None and tries >= max_tries:
            raise MaxCallException(values, last_item)

        try:
            logging.debug('Arguments: ' + str(args))
            val = target(*args, **kwargs)
            logging.debug('Results from Target running: ' + str(val))
            last_item = val
            logging.info('Try #:' + str(tries))
            DisplayMessage('Try #:' + str(tries))
        except ignore_exceptions as e:
            logging.error(str(e))
            last_item = e
        else:
            # Condition passes, this is the only "successful" exit from the polling function
            if check_target_success(val):
                return val

        logging.debug('last result from poll: ' + str(last_item))
        # Condition passes, this is the only "successful" exit from the polling function
        if check_target_success(val):
            return val
        else:
            values.put(last_item)
            tries += 1
            # Check the time after to make sure the poll function is called at least once
            if max_time is not None and Time.time() >= max_time:
                # raise TimeoutException(values, last_item)
                logging.info('Time out reached.')
                logging.info('Checking status of job: ' + val)
                logging.info('Job will now sleep for an additional: ' + step)
                logging.debug('Step value: ' + str(step))
                Time.sleep(step)
        step = step_function(step)


################################################################################
def main():
    # define global variables for urls that will be used for API calls

    global gl_login_url
    global gl_runjob_url
    global gl_status_url
    global gl_Job_ID
    global gl_validatesession_url
    global gl_sessionID
    global gl_starttime
    global gl_interval_sec
    global gl_max_tries
    global gl_fileName

    # define run job version constant. change when version changes

    RUN_INFA_CLOUD_JOB_VERSION = '2.0'

    # Parse arguments using the function defined

    ParseCommandLine()

    # turn on logging
    gl_fileName = gl_args.jobFile.split('.')[0] + '_' + str(datetime.now().strftime('%Y_%m_%d_%H_%M_%S_%f')) + '.log'
    fileName = '/app/data/infa/server/infa_shared/SessLogs/run_IIC_' + gl_fileName

    DisplayMessage('Logging is in:' + fileName)
    logging.basicConfig(filename=fileName, level=logging.INFO,
                        format='%(asctime)s | %(levelname)s | %(message)s'
                        , filemode='w')

    # Record Starting Time

    gl_starttime = Time.time()
    logging.debug('Started @ ' + str(gl_starttime))

    # Record welcome message

    logging.info('')
    logging.info('Welcome to run job version '
                 + RUN_INFA_CLOUD_JOB_VERSION + ' ...')
    logging.info('New job run started...')
    logging.info('')

    # Record some information regarding system

    logging.info('System: ' + sys.platform)
    logging.info('Version: ' + sys.version)

    logging.info('Define global variables for URLs')

    # uncomment this statement for using proxy information
    # global gl_proxy
    # uncomment this block for using proxy information
    # gl_proxy = {'http': '127.0.0.1'}
    # add any other attributes needed for proxy like port

    # Read credentails from credentials file from arguments parsed

    logging.info('Get Credentails')
    logging.info('Credentials file is ' + gl_args.credFile)
    credentials = ReadFileToDict('/app/data/infaCloudPython/'
                                 + gl_args.credFile)
    logging.debug('Credentails contents are: ' + str(credentials))

    # Read Configuraiton file
    logging.info('Getting Configuraiton URLs')
    config = configparser.ConfigParser()
    config.sections()
    config.read('/app/data/infaCloudPython/config.ini')

    gl_login_url = config.get('URLS', 'login_url')
    #    'https://dm-us.informaticacloud.com/ma/api/v2/user/login'
    gl_runjob_url = config.get('URLS', 'runjob_url')
    #    'https://usw3.dm-us.informaticacloud.com/saas/api/v2/job'
    gl_status_url = config.get('URLS', 'status_url')
    #    'https://usw3.dm-us.informaticacloud.com/saas/api/v2/activity/activityLog'
    gl_validatesession_url = config.get('URLS', 'validatesession_url')
    #    'https://usw3.dm-us.informaticacloud.com/saas/api/v2/user/validSessionId'
    logging.debug('login_url: ' + gl_login_url)
    logging.debug('runjob_url: ' + gl_runjob_url)
    logging.debug('status_url: ' + gl_status_url)
    logging.debug('validatesession_url: ' + gl_validatesession_url)


    logging.info('Get informatica login session id')
    gl_sessionID = InfaCloudLogin(gl_login_url, credentials)

    logging.info('informatica login session id is ' + gl_sessionID)

    # Read job properties from job file from arguments parsed

    logging.info('Get Job properties')
    logging.info('Job Properties file is ' + gl_args.jobFile)
    jobProps = \
        ReadFileToDict('/app/data/infa/server/infa_shared/BWParam/ICC/'
                       + gl_args.jobFile)
    # DEBUG
    # jobProps = \
    #     ReadFileToDict('/app/data/infaCloudPython/'
    #                    + gl_args.jobFile)
    # add type to payload
    sessionAdd = {'@type': 'job'}
    jobProps.update(sessionAdd)

    logging.info('running job')
    logging.info(jobProps)

    # run the job wait get the submit status
    passWaitTime = 60
    if gl_args.waitTime != 0:
        passWaitTime = gl_args.waitTime

    # DEBUG
    # submitStatus = True
    # gl_Job_ID = 24
    passInterval = 1
    logging.debug('Checking before: ' + str(passInterval))
    if str(gl_args.intervalTime) != 'None':
        passInterval = gl_args.intervalTime
        logging.debug('Checking param: ' + str(gl_args.intervalTime))
        logging.debug('Checking after: ' + str(passInterval))
    passMaxTries = 1
    if gl_args.maxTries != 'None':
        passMaxTries = gl_args.maxTries

    submitStatus = InfaRunJob(gl_runjob_url, jobProps, gl_sessionID)

    logging.info('Submitting of the job is successful? '
                 + str(submitStatus))
    if submitStatus:

        # Now build url and the parameters for checking activity log for completion of this job

        DisplayMessage('job submission was successful, now checking activity log for job completion...')
        logging.info('job submission was successful, now checking activity log for job completion...')
        activityKeyStore = {}
        activityKeyStore['icSessionId'] = gl_sessionID
        activityKeyStore['Accept'] = 'application/json'
        activityKeyStore['Content-Type'] = 'application/json'
        params = [gl_status_url, activityKeyStore, jobProps['taskName'], passWaitTime, credentials]

        retValue = poll(lambda: InfaJobStatus(gl_status_url, activityKeyStore, jobProps['taskName'], passWaitTime, credentials),
                        passInterval, (), None, passWaitTime,
                        passMaxTries, is_truthy, step_constant, (), False, None)
    else:
        DisplayMessage('job submission failed')
        logging.error('job submission failed')
        retValue = -1

    # Record end Time

    endTime = Time.time()
    duration = endTime - gl_starttime

    DisplayMessage('Elapsed time is ' + str(duration) + ' Seconds')
    logging.info('Elapsed time is ' + str(duration) + ' Seconds')

    if retValue == 0:
        DisplayMessage('Execution was successful')
        logging.info('Execution was successful')
        SendMessage('Execution was successful.', fileName)
        sys.exit(0)
    else:
        DisplayMessage('Execution failed')
        logging.error('Execution failed')
        SendMessage('Execution failed.', fileName)
        sys.exit(-1)


################################################################################

if __name__ == '__main__':
    main()
