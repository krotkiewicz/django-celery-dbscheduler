"""Old django celery integration project."""
# :copyright: (c) 2009 - 2012 by Ask Solem.
# :license:   BSD, see LICENSE for more details.
from __future__ import absolute_import, unicode_literals

VERSION = (3, 2, 1, '')
__version__ = '.'.join(map(str, VERSION[0:3])) + ''.join(VERSION[3:])
__author__ = ''
__contact__ = ''
__homepage__ = ''
__docformat__ = 'restructuredtext'
__license__ = 'BSD (3 clause)'

# -eof meta-


from celery import current_app as celery  # noqa
