#!/usr/bin/env python3

"""
    Miscellaneous functions that come in handy
"""


class MandatoryFieldNotFoundError(Exception):
    """
        Exception raised when mandatory field is not present in given list
    """

    def __init__(self, field):
        self.field = field

    def __str__(self):
        return repr("Mandatory field: %s not found" % (self.field,))


class UnexpectedFieldDataType(Exception):
    """
        Exception raised when field is not an instance of specified type
        Contains a critical field to indicate that the state machine
        cannot advance to the next state when this is raised
    """

    def __init__(self, field, data_type, critical=False):
        self.field = field
        self.data_type = data_type
        self.critical = critical

    def __str__(self):
        return repr("Field: %s is not an instance of %s" % (self.field, self.data_type,))


##############
#
# Get element from object
#
# Input parameters:
#   element   - key of the element
#   vaiable   - object variable
#   mandatory - defines acceptability of the element's absence
#
# Return value:
#   The requested element of the object or None when the element doesn't exist.
#   In case element is mandatory, raise exception.
#
# Note:
#   This function does not distinguish between having received
#   a None value for an element or failing to obtain it from the dictionary.
#
##############
def get_element(element, variable, mandatory=False, data_type=None):
    try:
        result = variable[element]
    except (KeyError, TypeError):
        if mandatory is False:
            return None
        else:
            raise MandatoryFieldNotFoundError(element)
    # If successfully got element, and data_type is defined, validate data type
    if data_type is not None:
        if isinstance(result, data_type):
            return result
        else:
            raise UnexpectedFieldDataType(element, data_type, mandatory)
    else:
        return result


##############
#
# Log error to database
#
# Input parameters:
#   session   - variable that holds all session specific values
#               Data to be logged will be obtained from session variables
#
##############
def log_to_database(session):

    from states import RPCS

    if 'log' in session and 'db' in session:

        cid = get_element('cid', client)
        ip = get_element('ip', session['cpe'])
        event = get_element('event', session['cpe'])
        msg = get_element('msg', session['log'])
        result = get_element('rc', session['log'])

        # Log session result to the database
        session['db'].log(cid, ip, event, msg, result)
