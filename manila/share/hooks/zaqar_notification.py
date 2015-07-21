# Copyright (c) 2015 Mirantis, Inc.
# All Rights Reserved.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

from oslo_config import cfg
from oslo_log import log
from oslo_utils import timeutils
import six
from zaqarclient.queues import client as zaqar

from manila.share import hook

zaqar_notification_opts = [
    cfg.StrOpt(
        "test",
        default="test",
        help="test help string",
        deprecated_group='DEFAULT',
    ),
]

CONF = cfg.CONF
CONF.register_opts(zaqar_notification_opts)
LOG = log.getLogger(__name__)

HOST = "172.18.198.52"
ZAQARCLIENTS = [zaqar.Client(
    url="http://%s:8888" % HOST,
    version=1.1,
    conf={
        "auth_opts": {
            "backend": "keystone",
            "options": {
                "os_username": conf[0],
                "os_password": conf[2],
                "os_project_name": conf[1],
                "os_project_id": conf[3],
                "os_auth_url": "http://%s:35357/v2.0/" % HOST,
                "insecure": True,
            },
        },
    },
) for conf in (('admin', 'admin', 'rengen', '2e05a19040e3488889f8589b90077f2b'),
               ('demo', 'demo', 'rengen', 'da8b6d069b124921b3df5c9854556cb7'))]


class ZaqarNotification(hook.HookBase):

    def _execute_pre_hook(self, context, func_name, *args, **kwargs):
        LOG.critical("\n\n PRE zaqar notification has been called for "
                     "method '%s'.\n\n" % func_name)

    def _execute_post_hook(self, context, func_name, pre_hook_data,
                           driver_action_results, *args, **kwargs):
        LOG.critical("\n\n POST zaqar notification has been called for "
                     "method '%s'.\n\n" % func_name)

    def _execute_periodic_hook(self, context, periodic_hook_data,
                               *args, **kwargs):
        LOG.critical("\n\nPeriodic zaqar notification has been called.\n\n")
        for client in ZAQARCLIENTS:
            user = client.conf['auth_opts']['options']['os_username']
            project = client.conf['auth_opts']['options']['os_project_name']
            client.queue_name = '3queue_' + user + '_' + project
            message = {
                'body': {
                    'user': user,
                    'secret_message': "message generated at '%s'" % timeutils.utcnow(),
                }
            }
            LOG.critical(
                "\n Sending message %(m)s to '%(q)s' queue using '%(u)s' user "
                "and '%(p)s' project." % {
                    'm': message,
                    'q': client.queue_name,
                    'u': user,
                    'p': project,
                }
            )
            queue = client.queue(client.queue_name)
            queue.post(message)
