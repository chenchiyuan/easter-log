# -*- coding: utf-8 -*-
# __author__ = chenchiyuan

from __future__ import division, unicode_literals, print_function
from django.conf import settings

TRUST_IPS = settings.TRUST_IPS or ['127.0.0.1', ]

class Verification(object):
  def verify_info(self, sig, info):
    import md5
    m = md5.new(info)
    return sig == m.hexdigest()

  def verify_ip(self, ip):
    return bool((ip in TRUST_IPS))