# -*- coding: utf-8 -*-
# __author__ = chenchiyuan

from __future__ import division, unicode_literals, print_function
from mixins.time_stat import TimeStatistics
from mixins.total_stat import TotalStatistics
from mixins.recordable import BaseRecord
from mixins.mongoable import Mongoable
from utils.helper import merge_dicts
from core.interpreter import StatTemplateInterpreter
from datetime import datetime as py_time
from utils.exceptions import NotExistsException

import logging

logger = logging.getLogger(__name__)

class UserEventFalls(BaseRecord):
  app_name = 'cayman'
  collection_name = 'user_events'

  unique_fields = ['uid', 'datetime']
  indexes = [({'uid': 1}, {}),
    (dict.fromkeys(unique_fields, 1), {'unique': True}),]

  def __init__(self, uid, datetime=py_time.now(), **kwargs):
    self.uid = uid
    if isinstance(datetime, basestring):
      datetime = py_time.strptime(datetime, '%Y-%m-%d %H:%M:%S')

    self.datetime = datetime
    self.__dict__.update(kwargs)

  @property
  def unique(self):
    unique_dict = {}
    for field in self.unique_fields:
      value = getattr(self, field)
      unique_dict.update({field: value})
    return unique_dict

  @classmethod
  def cls_unique(cls, **kwargs):
    unique_dict = {}
    for field in cls.unique_fields:
      value = kwargs.pop(field, '')
      unique_dict.update({field: value})
    return unique_dict

  @classmethod
  def merge(cls, from_uid, to_uid):
    collection = cls.get_collection()
    try:
      collection.update({'uid': from_uid},
          {'$set': {'uid': to_uid}}, upsert=False, multi=True)
    except Exception, err:
      logger.info(err)

class EventHandler(BaseRecord, TimeStatistics, TotalStatistics):
  app_name = 'cayman'
  collection_name = 'event'

  record_time_fields = ['clip', 'board', 'clip__board']
  record_total_fields = ['total', {'origin': {'0': 'ipad', '1': 'web', '2': 'iphone'}}]
  unique_fields = ['slug', 'date']
  fields_to_db = ['slug']
  event_pull_fields = ['text']
  indexes = [(dict.fromkeys(unique_fields, 1), {'unique': True}), ]

  def __init__(self, uid, cls_dict={}, **kwargs):
    self.uid = uid
    date = py_time.now().date()
    self.date = py_time(year=date.year, month=date.month, day=date.day)
    kwargs.pop('uid', '')
    kwargs.pop('date', '')
    self.__dict__.update(kwargs)
    self.__class__.__dict__.update(cls_dict)

  def dict_to_db(self):
    record_dict = {}
    for field in self.fields_to_db:
      record_dict.update({field: getattr(self, field, '')})
    record_dict.update({'date': self.date})
    return record_dict

  @property
  def unique(self):
    unique_dict = {}
    for field in self.unique_fields:
      value = getattr(self, field)
      unique_dict.update({field: value})
    return unique_dict

  @classmethod
  def cls_unique(cls, **kwargs):
    unique_dict = {}
    for field in cls.unique_fields:
      value = kwargs.pop(field, '')
      unique_dict.update({field: value})
    return unique_dict

  def time_record_fields(self):
    return StatTemplateInterpreter.parse(self, self.record_time_fields)

  def total_record_fields(self):
    return StatTemplateInterpreter.parse(self, self.record_total_fields)

  def pushable_fields(self):
    return self.event_pull_fields

  def update_objs(self):
    update_time_objs = self.time_record()
    update_total_objs = self.total_record()
    merged_dict = merge_dicts(update_time_objs, update_total_objs)
    return merged_dict

  def after_update(self):
    if not (self.event_pull_fields and hasattr(self, 'uid')):
      return

    pull_data = {}
    for field in self.event_pull_fields:
      value = getattr(self, field, '')
      pull_data.update({field: value})

    pull_data.update({'datetime': py_time.now() if not hasattr(self, 'datetime') else self.datetime})
    u = UserEventFalls(uid=self.uid, **pull_data)
    u.record()

class UserHashTable(Mongoable):
  app_name = 'easter'
  collection_name = 'user_hash'
  indexes = [({'user_hash': 1}, {'unique': True})]


  class MD5Hash:
    def hexdigest(self, info):
      import md5
      m = md5.new(info)
      return m.hexdigest()

  @property
  def unique(self):
    return {'user_hash': self.user_hash}

  def __init__(self, cookie, uid=None):
    m = self.MD5Hash()
    self.user_hash = m.hexdigest(cookie)
    self.uid = uid

  def dict_to_db(self):
    return {
      'user_hash': self.user_hash,
      'uid': self.uid
    }

  def is_exists_and_registered(self):
    try:
      info = self.get_one_query(query=self.unique)
    except Exception, err:
      logger.info(err)
      return False, None

    else:
      return bool(info), info.get('uid', '')

  def register(self):
    if not self.uid:
      return

    self.update({'$set': {'uid': self.uid}})

  def get_uid(self):
    return self.uid if self.uid else self.user_hash

class RegisteredEvents(Mongoable):
  app_name = 'easter'
  collection_name = 'register_events'

  unique_fields = ['event_app', 'event_collection']
  indexes = [(dict.fromkeys(unique_fields, 1), {'unique': True}), ]

  def __init__(self, event_app, event_collection, time_stat=[],
               total_stat=[], event_unique=[], event_fields_to_db=[],
               event_fields_to_feeds=[], event_indexes=[]):
    self.event_app = event_app
    self.event_collection = event_collection
    self.time_stat = time_stat
    self.total_stat = total_stat
    self.event_unique = event_unique
    self.event_fields_to_db = event_fields_to_db
    self.event_fields_to_feeds = event_fields_to_feeds
    self.event_indexes = event_indexes

  @classmethod
  def get_by_name(cls, event_app, event_collection):
    json_data = cls.get_one_query({'event_app': event_app, 'event_collection': event_collection})
    if not json_data:
      raise NotExistsException("Not exists app_name %s, collection_name %s" %(event_app, event_collection))

    data = cls.format_data(json_data)
    return data


  @classmethod
  def format_data(cls, json_data):
    app_name = json_data['event_app']
    collection_name = json_data['event_collection']
    record_time_fields = json_data.get('time_stat', [])
    record_total_fields = json_data.get('total_stat', [])
    unique_fields = json_data.get('event_unique', [])
    fields_to_db = json_data.get('event_fields_to_db', [])
    event_pull_fields = json_data.get('event_fields_to_feeds', [])
    indexes = json_data.get('event_indexes', [])

    return {
      'app_name': app_name,
      'collection_name': collection_name,
      'record_time_fields': record_time_fields,
      'record_total_fields': record_total_fields,
      'unique_fields': unique_fields,
      'fields_to_db': fields_to_db,
      'event_pull_fields': event_pull_fields,
      'indexes': indexes
    }