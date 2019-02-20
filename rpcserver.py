#!/usr/bin/env python3
"""RPC Server NG automatic provisioning server"""
import asyncio
import atexit
import functools
import json
import gc
import os
import signal
import sys
import traceback

from nb_handler import nb_session_handler
from states import RPCS
from config import LOG, LOGGING_FILENAME, LOGGING_LEVEL_CONSOLE, LOGGING_LEVEL_LOGFILE, RPCSD_TIMEOUT
from misc import get_element, log_to_database

assert sys.version >= '3.4', 'Please use Python 3.4 or higher.'


def parse_result(message_result):
    # TODO create error code mapping
    error = {'error': {'code': -32603, 'message': 'Invalid response'}}

    # Assert that list results are composed of integers >= 1
    # and that no two items in the same sublist are the same
    if type(message_result) is list:
        for element in message_result:
            if type(element) is list:
                seen = set()

                for subelement in element:
                    if not isinstance(subelement, int):
                        error['error']['data'] = 'wrong data type: result'
                        return error
                    # Make sure its an integer value between 1 and 2^32-1
                    elif not 1 <= subelement <= 4294967295:
                        error['error']['data'] = 'object identifiers must be in range (1:4294967295): result'
                        return error
                    # Makes sure all elements of list are unique object ids
                    elif subelement not in seen:
                        seen.add(subelement)
                    else:
                        error['error']['data'] = 'object identifiers must be unique'
                        return error
            elif not isinstance(element, int):
                error['error']['data'] = 'wrong data type: result'
                return error

        # result list: OK
        return None

    elif type(message_result) is dict:
        return None

    # If not in above cases, must be in VALID_RESULTS
    else:
        for element in RPCS.VALID_RESULTS:
            if message_result is element:
                return None

        # Result not found in VALID_RESULTS
        error['error']['data'] = 'result not in VALID_RESULTS'
        return error


def parse_message(message, message_type):
    """
        Parse a JSON request and return a message in python dictionary format
        Input values:
          request - RPC request from CPE
          message_type - Type of incoming message - Request or Response

        Return values:
          result  - Either succesfully parsed message or described error
    """
    # Check for valid JSON message
    error = None
    session_message = {}
    LOG.debug("Message from CPE:" + str(message))
    try:
        parsed = json.loads(message.decode())
        message_jsonrpc = get_element('jsonrpc', parsed)
        message_id = get_element('id', parsed)
        message_method = get_element('method', parsed)
        message_result = get_element('result', parsed)
        message_params = get_element('params', parsed)

        if message_jsonrpc != '2.0':
            # TODO create error code mapping
            error = {'error': {'code': -32600,
                               'message': 'Invalid request: '
                               'Wrong version of JSON-RPC'}}

        # If method is done, id must be None. Otherwise, it must be an integer
        if not isinstance(message_id, int) and not message_method == 'done':
            # TODO create error code mapping
            error = {'error': {'code': -32600,
                               'message': 'Invalid request: ID field is '
                               'not an integer'}}

        if message_type == 'request':
            # If it is a request and method is not valid
            if not (message_method in RPCS.VALID_METHODS):
                # TODO create error code mapping
                error = {'error': {'code': -32601,
                                   'message': 'Method not found'}}

        elif message_type == 'response':
            # If response has no result, it is an error response
            if message_result is None:
                if 'error' in parsed:
                    error = parsed
                else:
                    # TODO create error code mapping
                    error = {'error': {'code': -32600, 'message':
                                       'Invalid request: Response message '
                                       'with no result field'}}
            # Otherwise we need to parse its result field
            else:
                error = parse_result(message_result)

        if error is None:
            session_message['jsonrpc'] = message_jsonrpc
            session_message['id'] = message_id
            session_message['method'] = message_method
            session_message['result'] = message_result
            session_message['params'] = message_params
            return session_message
        else:
            error['id'] = message_id
            LOG.debug(str(error))
            return error

    except Exception as e:
        LOG.debug("Error during parsing CPE message: %s", e)
        error = {'error': {'code': -32700, 'message': 'Parse error'}}
        return error


def send_rpc(rpc, writer):
    rpc['jsonrpc'] = '2.0'
    writer.write(bytes(json.dumps(rpc), 'utf-8'))
    writer.write(b'\n')


def update_northbound_handler(session):
    try:
        event = get_element('event', session, data_type=str)
        if (event == 'CONNECTION_REQUEST' and session['client_id'] in RPCS.Northbound_Queue):
            # Northbridge session is waiting for a response,
            # return 'error' via stream socket to communicate CPE disconnection
            result = 'error'
            session['nb_response_stream'].send(result.encode())
    except KeyError:
        LOG.debug("Event is CONNECTION_REQUEST but Northbound session was not found!")


@asyncio.coroutine
def session_handler(reader, writer):
    """
        This is a call back routine for an SSL session
        Read messages from the device, handle them and write the responses
    """

    # initialize defaults
    session = {'queue': [], 'rpc': {'id': 0}, 'cpe': {}, 'dm': {}, 'rtwan_layer3_keep': False, 'voice_layer3_keep': False}

    # start authentication of client
    state = RPCS.Authenticating
    state = state.prepare_session(writer, session)

    try:
        LOG.debug("Open connection with: %s", session['cpe']['ip'])

        # RPC SERVER
        while state in RPCS.RPC_SERVER_STATES:
            response = None
            request = yield from asyncio.wait_for(reader.readline(), timeout=RPCSD_TIMEOUT)
            if reader.at_eof():
                # Received disconnect signal from CPE
                update_northbound_handler(session)
                session['log'] = {'rc': 'error', 'msg': 'Connection terminated by client'}
                LOG.debug("CPE closed connection with RPC Server")
                break

            session['rpc']['message'] = parse_message(request, 'request')
            # Respond with error to invalid requests, when possible
            if 'error' in session['rpc']['message']:
                if session['rpc']['message']['error'] is not None:
                    response = session['rpc']['message']
            # Handle message and move between states
            else:
                state, response = state.run(session)
            # Prepare and send response
            if not (response is None):
                LOG.debug("Message to cpe: " + str(response))
                send_rpc(response, writer)

        # RPC CLIENT
        while state in RPCS.RPC_CLIENT_STATES:
            request = None
            rpc = None
            # Only listen for messages during 'ExpectResponse' state
            if state == RPCS.ExpectResponse:
                request = yield from asyncio.wait_for(reader.readline(), timeout=RPCSD_TIMEOUT)
                if reader.at_eof():
                    # Received disconnect signal from CPE
                    update_northbound_handler(session)
                    session['log'] = {'rc': 'error', 'msg': 'Connection terminated by client'}
                    LOG.debug("CPE closed connection with RPC Server")
                    break

                session['rpc']['message'] = parse_message(request, 'response')

            # Either check if there are RPCs in queue to send, or handle CPE
            # response
            state, rpc = state.run(session)
            if rpc is not None:
                LOG.debug("Message to cpe: " + str(rpc))
                send_rpc(rpc, writer)

    except asyncio.TimeoutError:
        LOG.debug("CPE connection timed out")
        # Write an error to the database
        session['log'] = {'rc': 'error', 'msg': 'Connection timeout'}
        reader.feed_eof()
    except ConnectionResetError:
        LOG.debug("CPE closed the connection with RPC Server")
        session['log'] = {'rc': 'error', 'msg': 'Connection reset by peer'}
        reader.feed_eof()
    except Exception:
        # Catch everything else, prevents daemon from crashing
        err = sys.exc_info()[0]
        traceback.print_exc()
        LOG.error("Internal server error: %s", err)
        reader.feed_eof()
    finally:
        # Make sure session is always closed tidily even if an error occurs
        if 'db' in session:
            log_to_database(session)
            session['db'].disconnect()
        writer.close()
        # Get any response left and throw away
        while not (reader.at_eof()):
            yield from reader.read()
        LOG.debug("Close connection with: %s", session['cpe']['ip'])

        # Running a forceful full garbage collection
        collected = gc.collect()
        LOG.debug("Unreachable objects after forceful garbage collection: %d", collected)


@atexit.register
def goodbye():
    LOG.info("Stopping RPC Server Daemon...")


def shutdown(loop):
    for task in asyncio.Task.all_tasks():
        task.cancel()
    sys.exit(0)


def configure_logger():
    # Configure the logger to log both file and console using appropiate format
    root_logger = LOG.getLogger()
    root_logger.setLevel(LOG.NOTSET)

    file_handler = LOG.FileHandler(os.path.abspath(LOGGING_FILENAME))
    file_handler.setLevel(LOGGING_LEVEL_LOGFILE)
    file_formatter = LOG.Formatter('%(asctime)s> [%(levelname)s] %(message)s',
                                   datefmt='%a %b %d %H:%M:%S %Y')
    file_handler.setFormatter(file_formatter)
    root_logger.addHandler(file_handler)

    console_handler = LOG.StreamHandler(sys.stdout)
    console_handler.setLevel(LOGGING_LEVEL_CONSOLE)
    console_formatter = LOG.Formatter('[%(levelname)s] (%(module)s:%(lineno)d)'
                                      ' %(message)s')
    console_handler.setFormatter(console_formatter)
    root_logger.addHandler(console_handler)


def main():
    """
        The main program starts here
    """
    configure_logger()

    LOG.info("Starting PYRPC ServerD...")
    loop = asyncio.get_event_loop()
    loop.add_signal_handler(signal.SIGTERM, functools.partial(shutdown, loop))
    RPCS().Listening.run_forever(session_handler, nb_session_handler)

if __name__ == '__main__':
    main()
