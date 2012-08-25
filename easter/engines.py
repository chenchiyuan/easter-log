# -*- coding: utf-8 -*-
# __author__ = chenchiyuan

from __future__ import division, unicode_literals, print_function
from models import UserHashTable, EventHandler, RegisteredEvents

import logging

logger = logging.getLogger(__name__ )

class MainEngine(object):
  def execute(self, db_info, user_info, events=[]):
    app_name = db_info.get('app_name', '')
    collection_name = db_info.get('collection_name', '')

    try:
      cls_info = RegisteredEvents.get_by_name(app_name, collection_name)
    except Exception, err:
      logger.info(err)
      return

    self.do_events(cls_info, user_info, events)

  def do_events(self, cls_info, user_info, events=[]):
    try:
      uid = self.authentication(user_info)
    except Exception, err:
      logger.info(err)
      return

    for event in events:
      self.do_event(cls_info=cls_info, uid=uid, event=event)

  def do_event(self, uid, cls_info, event):
    handler = EventHandler(uid=uid, cls_dict=cls_info, **event)
    try:
      handler.record()
    except Exception, err:
      logger.info(err)
      return
    
  def verify(self, **kwargs):
    pass

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
