# Copyright (c) 2011 OpenStack, LLC.
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

"""Policy Engine For Manila"""

import functools
import os.path

from oslo.config import cfg

from manila import exception
from manila.openstack.common import policy
from manila import utils

CONF = cfg.CONF

_ENFORCER = None
_POLICY_PATH = None
_POLICY_CACHE = {}


def reset():
    global _POLICY_PATH
    global _POLICY_CACHE
    global _ENFORCER
    _POLICY_PATH = None
    _POLICY_CACHE = {}
    _ENFORCER = None


def init():
    global _POLICY_PATH
    global _POLICY_CACHE
    global _ENFORCER
    if not _POLICY_PATH:
        _POLICY_PATH = CONF.policy_file
        if not os.path.exists(_POLICY_PATH):
            _POLICY_PATH = utils.find_config(_POLICY_PATH)
    if not _ENFORCER:
        _ENFORCER = policy.Enforcer(policy_file=_POLICY_PATH)
    utils.read_cached_file(_POLICY_PATH, _POLICY_CACHE, reload_func=_set_rules)


def _set_rules(data):
    global _ENFORCER
    default_rule = CONF.policy_default_rule
    _ENFORCER.set_rules(policy.Rules.load_json(
        data, default_rule))


def enforce(context, action, target, do_raise=True):
    """Verifies that the action is valid on the target in this context.

       :param context: manila context
       :param action: string representing the action to be checked
           this should be colon separated for clarity.
           i.e. ``compute:create_instance``,
           ``compute:attach_volume``,
           ``volume:attach_volume``

       :param object: dictionary representing the object of the action
           for object creation this should be a dictionary representing the
           location of the object e.g. ``{'project_id': context.project_id}``

       :raises manila.exception.PolicyNotAuthorized: if verification fails.

    """
    init()
    if not isinstance(context, dict):
        context = context.to_dict()

    # Add the exception arguments if asked to do a raise
    extra = {}
    if do_raise:
        extra.update(exc=exception.PolicyNotAuthorized, action=action,
                     do_raise=do_raise)
    return _ENFORCER.enforce(action, target, context, **extra)


def check_is_admin(roles):
    """Whether or not roles contains 'admin' role according to policy setting.

    """
    init()

    # include project_id on target to avoid KeyError if context_is_admin
    # policy definition is missing, and default admin_or_owner rule
    # attempts to apply.  Since our credentials dict does not include a
    # project_id, this target can never match as a generic rule.
    target = {'project_id': ''}
    credentials = {'roles': roles}
    return _ENFORCER.enforce("context_is_admin", target, credentials)


def wrap_check_policy(resource):
    """Check policy corresponding to the wrapped methods prior to execution.
    """
    def check_policy_wraper(func):
        @functools.wraps(func)
        def wrapped(self, context, target_obj, *args, **kwargs):
            check_policy(context, resource, func.__name__, target_obj)
            return func(self, context, target_obj, *args, **kwargs)

        return wrapped
    return check_policy_wraper


def check_policy(context, resource, action, target_obj=None):
    target = {
        'project_id': context.project_id,
        'user_id': context.user_id,
    }
    target.update(target_obj or {})
    _action = '%s:%s' % (resource, action)
    enforce(context, _action, target)
