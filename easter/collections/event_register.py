# -*- coding: utf-8 -*-
# __author__ = chenchiyuan

from __future__ import division, unicode_literals, print_function
from easter.mixins.mongoable import Mongoable
from easter.utils.exceptions import NotExistsException

class RegisteredEvents(Mongoable):
  app_name = 'easter'
  collection_name = 'register_events'

  unique_fields = ['event_app', 'event_collection']
  indexes = [(dict.fromkeys(unique_fields, 1), {'unique': True}), ]

  def __init__(self, event_app, event_collection, time_stat=[],
               total_stat=[], event_unique=[], event_fields_to_db=[],
               event_fields_to_feeds=[], event_indexes=[], event_alias={}):
    self.event_app = event_app
    self.event_collection = event_collection
    self.time_stat = time_stat
    self.total_stat = total_stat
    self.event_unique = event_unique
    self.event_fields_to_db = event_fields_to_db
    self.event_fields_to_feeds = event_fields_to_feeds
    self.event_indexes = event_indexes
    self.event_alias = event_alias

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
    alias = json_data.get('event_alias', {})

    return {
      'app_name': app_name,
      'collection_name': collection_name,
      'record_time_fields': record_time_fields,
      'record_total_fields': record_total_fields,
      'unique_fields': unique_fields,
      'fields_to_db': fields_to_db,
      'event_pull_fields': event_pull_fields,
      'indexes': indexes,
      'alias': alias
    }