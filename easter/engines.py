# -*- coding: utf-8 -*-
# __author__ = chenchiyuan

from __future__ import division, unicode_literals, print_function
from models import UserHashTable, EventHandler, RegisteredEvents
from utils.exceptions import InfoIllegalException, SignitureException

import json
import logging

logger = logging.getLogger(__name__)

class MainEngine(object):
  def execute(self, sig, app_name, user_info, events=[]):
    json_data = {
      'app_name': app_name,
      'user_info': user_info,
      'events': events
      }
    try:
      validate = self.verify(sig, json.dumps(json_data))
    except Exception, err:
      logger.info(err)
      raise InfoIllegalException("参数不合法")

    if not validate:
      raise SignitureException("签名验证失败")

    try:
      self.do_events(app_name, user_info, events)
    except Exception, err:
      logger.info(err)
      raise InfoIllegalException("参数不合法")

  def do_events(self, app_name, user_info, events=[]):
    uid = self.authentication(user_info)

    for event in events:
      collection_name = event.get('collection_name', '')
      cls_info = RegisteredEvents.get_by_name(app_name, collection_name)
      self.do_event(cls_info=cls_info, uid=uid, event=event)

  def do_event(self, uid, cls_info, event):
    handler = EventHandler(uid=uid, cls_dict=cls_info, **event)
    handler.record()

  def verify(self, sig, info):
    import md5
    m = md5.new(info)
    print(sig)
    print(m.hexdigest())
    return sig == m.hexdigest()

  def authentication(self, user_info):
    cookie = user_info['cookie']
    uid = user_info.get('uid', '')

    u_hash = UserHashTable(cookie=cookie, uid=uid)
    exists, user_id = u_hash.is_exists_and_registered()

    if not exists:
      u_hash.save()
    if not user_id:
      u_hash.register()

    if not uid and user_id:
      u_hash.uid = user_id

    return u_hash.get_uid()
