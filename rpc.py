#!/usr/bin/env python3
"""RPCS NG RPC methods"""

from config import LOG
import misc


def prepare_rpc(session, method, rpc_params=None):
    """ Increments RPC 'id' field by one for each new RPC to be sent
        and fills the other main RPC fields
    """
    # New RPC id for each sent RPC
    session['rpc']['id'] += 1

    if rpc_params is None:
        rpc = {'id': session['rpc']['id'], 'method': method}
    else:
        rpc = {'id': session['rpc']['id'], 'method': method, 'params': rpc_params}

    return rpc


class ClientMethodException(Exception):
    """ Exception raised when an error occurs during RPC preparation
        or during handling of a response to a previously sent RPC
    """

    def __init__(self, operation, client_method):
        self.operation = operation
        self.client_method = client_method
        LOG.debug("Error: %s of %s client method" % (self.operation, self.client_method))

    def __str__(self):
        return 'Error: {} of {}'.format(self.operation, self.client_method)


class Rpc:
    """ Most generic implementation of a basic JSON RPC """

    def __init__(self, params=None):
        self.rpc_params = params if params is not None else {}

    def send_request(self, session):
        # Each class that inherits should have its own implementation of the
        # "prepare_request" method
        LOG.debug("Sending RPC for %s" % self.__class__.__name__)
        self.prepare_request(session)
        return prepare_rpc(session, self.method, self.rpc_params)

    def handle_response(self, session, message):
        """ Obtain result from RPC response and send back to SB handler """
        result = misc.get_element('result', message, mandatory=True)
        return result


class RpcGet(Rpc):
    """ Example implementation of a "GET" RPC """
    method = 'get'
    rpc_params = {}

    def __init__(self, params_list=None):
        if params_list is None:
            self.params_list = []
        else:
            self.params_list = params_list

    def prepare_request(self):
        self.rpc_params = {}
        for param in self.params_list:
            self.rpc_params.update({param: None})


class RpcSet(Rpc):
    """ Example implementation of a "SET" RPC """
    method = 'set'

    def handle_response(self, session, message):
        """ Process the return from the set RPC
            an error response is already handled
        """
        result = misc.get_element('result', message)

        if result == 0:
            LOG.debug("Response to 'set': Success")
            pass
        elif result == 1:
            LOG.debug("Response to 'set': Error")
            pass
        else:
            # Unknown result
            raise ClientMethodException('handle response', 'RpcSet')
