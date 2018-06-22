# ----------------------------------------------------------------------
# Numenta Platform for Intelligent Computing (NuPIC)
# Copyright (C) 2013, Numenta, Inc.  Unless you have an agreement
# with Numenta, Inc., for a separate license for this software code, the
# following terms and conditions apply:
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero Public License version 3 as
# published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the GNU Affero Public License for more details.
#
# You should have received a copy of the GNU Affero Public License
# along with this program.  If not, see http://www.gnu.org/licenses.
#
# http://numenta.org/licenses/
# ----------------------------------------------------------------------

"""
Helper utilities for python scripts that use psycopg2
"""

import inspect
import logging
from socket import error as socket_error

import psycopg2
from nupic.support.decorators import retry as make_retry_decorator

def retrySQL(timeoutSec=60*5, logger=None):
  """ Return a closure suitable for use as a decorator for
  retrying a psycopg2 DAO function on certain failures that warrant retries (
  e.g., RDS/MySQL server down temporarily, transaction deadlock, etc.).
  We share this function across multiple scripts (e.g., ClientJobsDAO,
  StreamMgr) for consitent behavior.

  .. note:: Please ensure that the operation being retried is idempotent.

  .. note:: logging must be initialized *before* any loggers are created, else
     there will be no output; see nupic.support.initLogging()

  Usage Example:

  .. code-block:: python

    @retrySQL()
    def jobInfo(self, jobID):
        ...

  :param timeoutSec:       How many seconds from time of initial call to stop retrying
                     (floating point)
  :param logger:           User-supplied logger instance.

  """

  if logger is None:
    logger = logging.getLogger(__name__)

  def retryFilter(e, args, kwargs):

    #if isinstance(e, (psycopg2.InternalError, psycopg2.OperationalError)):
    #  if e.args and e.args[0] in _ALL_RETRIABLE_ERROR_CODES:
    #    return True

    if isinstance(e, psycopg2.Error):
      if (e.args and
          inspect.isclass(e.args[0]) and issubclass(e.args[0], socket_error)):
        return True

    return False


  retryExceptions = tuple([
    psycopg2.InternalError,
    psycopg2.OperationalError,
    psycopg2.Error,
  ])

  return make_retry_decorator(
    timeoutSec=timeoutSec, initialRetryDelaySec=0.1, maxRetryDelaySec=10,
    retryExceptions=retryExceptions, retryFilter=retryFilter,
    logger=logger)
