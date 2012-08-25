# -*- coding: utf-8 -*-
# __author__ = chenchiyuan

from __future__ import division, unicode_literals, print_function

from djangorestframework.views import View
from utils.http import http_400, http_403, http_200
from engines import MainEngine

import json
import logging

logger = logging.getLogger(__name__)

class EventView(View):
  def post(self, request, *args, **kwargs):
    info = self.CONTENT

    try:
      sig = info.get('sig', '')
      app_name = info.get('app_name', '')
      user_info = json.loads(info.get('user_info', {}))
      events = json.loads(info.get('events', {}))
    except Exception, err:
      logger.info(err)
      return http_400()

    m = MainEngine()
    try:
      m.execute(sig, app_name, user_info, events)
    except Exception, err:
      logger.info(err)
      return http_403()

    return http_200()



      