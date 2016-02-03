# Copyright (c) 2016 Mirantis, Inc.
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

import ddt
import mock
from oslo_config import cfg

from manila import context
from manila import exception
from manila.share.drivers.ganesha import utils as ganesha_utils
from manila.share.drivers.zfsonlinux import driver as zfs_driver
from manila import test

CONF = cfg.CONF


class FakeConfig(object):
    def __init__(self, *args, **kwargs):
        self.driver_handles_share_servers = False
        self.share_backend_name = 'FAKE_BACKEND_NAME'
        self.zfs_share_export_ip = kwargs.get(
            "zfs_share_export_ip", "1.1.1.1")
        self.zfs_service_ip = kwargs.get("zfs_service_ip", "2.2.2.2")
        self.zfs_zpool_list = kwargs.get(
            "zfs_zpool_list", ["foo", "bar/subbar", "quuz"])
        self.zfs_use_ssh = kwargs.get("zfs_use_ssh", False)
        self.zfs_share_export_ip = kwargs.get(
            "zfs_share_export_ip", "240.241.242.243")
        self.zfs_service_ip = kwargs.get("zfs_service_ip", "240.241.242.244")
        self.ssh_conn_timeout = kwargs.get("ssh_conn_timeout", 123)
        self.zfs_ssh_username = kwargs.get(
            "zfs_ssh_username", 'fake_username')
        self.zfs_ssh_user_password = kwargs.get(
            "zfs_ssh_user_password", 'fake_pass')
        self.zfs_ssh_private_key_path = kwargs.get(
            "zfs_ssh_private_key_path", '/fake/path')
        self.zfs_replica_snapshot_prefix = kwargs.get(
            "zfs_replica_snapshot_prefix", "tmp_snapshot_for_replication_")
        self.zfs_dataset_creation_options = kwargs.get(
            "zfs_dataset_creation_options", ["fook=foov", "bark=barv"])
        self.network_config_group = kwargs.get(
            "network_config_group", "fake_network_config_group")
        self.admin_network_config_group = kwargs.get(
            "admin_network_config_group", "fake_admin_network_config_group")
        self.config_group = kwargs.get("config_group", "fake_config_group")
        self.reserved_share_percentage = kwargs.get(
            "reserved_share_percentage", 0)

    def safe_get(self, key):
        return getattr(self, key)

    def append_config_values(self, *args, **kwargs):
        pass


class FakeDriverPrivateStorage(object):

    def __init__(self):
        self.storage = {}

    def update(self, entity_id, data):
        if entity_id not in self.storage:
            self.storage[entity_id] = {}
        self.storage[entity_id].update(data)

    def get(self, entity_id, key):
        return self.storage.get(entity_id, {}).get(key)

    def delete(self, entity_id):
        self.storage.pop(entity_id, None)


@ddt.ddt
class ZFSonLinuxShareDriverTestCase(test.TestCase):

    def setUp(self):
        self.mock_object(zfs_driver.CONF, '_check_required_opts')
        super(self.__class__, self).setUp()
        self._context = context.get_admin_context()
        self.ssh_executor = self.mock_object(ganesha_utils, 'SSHExecutor')
        self.configuration = FakeConfig()
        self.private_storage = FakeDriverPrivateStorage()
        self.driver = zfs_driver.ZFSonLinuxShareDriver(
            configuration=self.configuration,
            private_storage=self.private_storage)

    def test_init(self):
        self.assertTrue(hasattr(self.driver, 'replica_snapshot_prefix'))
        self.assertEqual(
            self.driver.replica_snapshot_prefix,
            self.configuration.zfs_replica_snapshot_prefix)
        self.assertEqual(
            self.driver.backend_name,
            self.configuration.share_backend_name)
        self.assertEqual(
            self.driver.zpool_list, ['foo', 'bar', 'quuz'])
        self.assertEqual(
            self.driver.dataset_creation_options,
            self.configuration.zfs_dataset_creation_options)
        self.assertEqual(
            self.driver.share_export_ip,
            self.configuration.zfs_share_export_ip)
        self.assertEqual(
            self.driver.service_ip,
            self.configuration.zfs_service_ip)
        self.assertEqual(
            self.driver.private_storage,
            self.private_storage)
        self.assertTrue(hasattr(self.driver, '_helpers'))
        self.assertEqual(self.driver._helpers, {})
        for attr_name in ('execute', 'execute_with_retry', 'parse_zfs_answer',
                          'get_zpool_option', 'get_zfs_option', 'zfs',
                          'zfs_with_retry'):
            self.assertTrue(hasattr(self.driver, attr_name))

    def test_init_error_with_duplicated_zpools(self):
        configuration = FakeConfig(
            zfs_zpool_list=['foo', 'bar', 'foo/quuz'])
        self.assertRaises(
            exception.ManilaException,
            zfs_driver.ZFSonLinuxShareDriver,
            configuration=configuration,
            private_storage=self.private_storage
        )

    def test__setup_helpers(self):
        mock_import_class = self.mock_object(
            zfs_driver.importutils, 'import_class')
        self.configuration.zfs_share_helpers = ['FOO=foo.module.WithHelper']

        result = self.driver._setup_helpers()

        self.assertIsNone(result)
        mock_import_class.assert_called_once_with('foo.module.WithHelper')
        mock_import_class.return_value.assert_called_once_with(
            self.configuration)
        self.assertEqual(
            self.driver._helpers,
            {'FOO': mock_import_class.return_value.return_value})

    def test__setup_helpers_error(self):
        self.configuration.zfs_share_helpers = []
        self.assertRaises(
            exception.ManilaException, self.driver._setup_helpers)

    def test__get_share_helper(self):
        self.driver._helpers = {'FOO': 'BAR'}

        result = self.driver._get_share_helper('FOO')

        self.assertEqual('BAR', result)

    @ddt.data({}, {'foo': 'bar'})
    def test__get_share_helper_error(self, share_proto):
        self.assertRaises(
            exception.InvalidShare, self.driver._get_share_helper, 'NFS')

    @ddt.data(True, False)
    def test_do_setup(self, use_ssh):
        self.mock_object(self.driver, '_setup_helpers')
        self.mock_object(self.driver, 'ssh_executor')
        self.configuration.zfs_use_ssh = use_ssh

        self.driver.do_setup('fake_context')

        self.driver._setup_helpers.assert_called_once_with()
        if use_ssh:
            self.driver.ssh_executor.assert_called_once_with('whoami')
        else:
            self.assertEqual(0, self.driver.ssh_executor.call_count)

    @ddt.data(
        ('foo', '127.0.0.1'),
        ('127.0.0.1', 'foo'),
        ('256.0.0.1', '127.0.0.1'),
        ('::1/128', '127.0.0.1'),
        ('127.0.0.1', '::1/128'),
    )
    @ddt.unpack
    def test_do_setup_error_on_ip_addresses_configuration(
            self, share_export_ip, service_ip):
        self.mock_object(self.driver, '_setup_helpers')
        self.driver.share_export_ip = share_export_ip
        self.driver.service_ip = service_ip

        self.assertRaises(
            exception.ManilaException, self.driver.do_setup, 'fake_context')

        self.driver._setup_helpers.assert_called_once_with()

    @ddt.data([], '', None)
    def test_do_setup_no_zpools_configured(self, zpool_list):
        self.mock_object(self.driver, '_setup_helpers')
        self.driver.zpool_list = zpool_list

        self.assertRaises(
            exception.ManilaException, self.driver.do_setup, 'fake_context')

        self.driver._setup_helpers.assert_called_once_with()

    @ddt.data(None, '', 'foo_replication_domain')
    def test__get_pools_info(self, replication_domain):
        self.mock_object(
            self.driver, 'get_zpool_option',
            mock.Mock(side_effect=['2G', '3G', '5G', '4G']))
        self.configuration.replication_domain = replication_domain
        self.driver.zpool_list = ['foo', 'bar']
        expected = [
            {'pool_name': 'foo', 'total_capacity_gb': 3.0,
             'free_capacity_gb': 2.0, 'reserved_percentage': 0},
            {'pool_name': 'bar', 'total_capacity_gb': 4.0,
             'free_capacity_gb': 5.0, 'reserved_percentage': 0},
        ]
        if replication_domain:
            for pool in expected:
                pool['replication_type'] = 'readable'

        result = self.driver._get_pools_info()

        self.assertEqual(expected, result)
        self.driver.get_zpool_option.assert_has_calls([
            mock.call('foo', 'free'),
            mock.call('foo', 'size'),
            mock.call('bar', 'free'),
            mock.call('bar', 'size'),
        ])

    @ddt.data(None, '', 'foo_replication_domain')
    def test__update_share_stats(self, replication_domain):
        self.configuration.replication_domain = replication_domain
        self.mock_object(self.driver, '_get_pools_info')
        self.assertEqual({}, self.driver._stats)
        expected = {
            'consistency_group_support': None,
            'driver_handles_share_servers': False,
            'driver_name': 'ZFS',
            'driver_version': '1.0',
            'free_capacity_gb': 'unknown',
            'pools': self.driver._get_pools_info.return_value,
            'qos': False,
            'replication_domain': replication_domain,
            'reserved_percentage': 0,
            'share_backend_name': self.driver.backend_name,
            'snapshot_support': True,
            'storage_protocol': 'NFS',
            'total_capacity_gb': 'unknown',
            'vendor_name': 'Open Source',
        }
        if replication_domain:
            expected['replication_type'] = 'readable'

        self.driver._update_share_stats()

        self.assertEqual(expected, self.driver._stats)
        self.driver._get_pools_info.assert_called_once_with()

    @ddt.data('', 'foo', 'foo-bar', 'foo_bar', 'foo-bar_quuz')
    def test__get_share_name(self, share_id):
        prefix = 'fake_prefix_'
        self.configuration.zfs_dataset_name_prefix = prefix
        self.configuration.zfs_dataset_snapshot_name_prefix = 'quuz'
        expected = prefix + share_id.replace('-', '_')

        result = self.driver._get_share_name(share_id)

        self.assertEqual(expected, result)

    @ddt.data('', 'foo', 'foo-bar', 'foo_bar', 'foo-bar_quuz')
    def test__get_snapshot_name(self, snapshot_id):
        prefix = 'fake_prefix_'
        self.configuration.zfs_dataset_name_prefix = 'quuz'
        self.configuration.zfs_dataset_snapshot_name_prefix = prefix
        expected = prefix + snapshot_id.replace('-', '_')

        result = self.driver._get_snapshot_name(snapshot_id)

        self.assertEqual(expected, result)

    def test__get_dataset_creation_options_not_set(self):
        self.driver.dataset_creation_options = []

        result = self.driver._get_dataset_creation_options(share={})

        self.assertEqual([], result)

    @ddt.data(True, False)
    def test__get_dataset_creation_options(self, is_readonly):
        self.driver.dataset_creation_options = [
            'readonly=quuz', 'sharenfs=foo', 'sharesmb=bar', 'k=v', 'q=w',
        ]
        share = {'size': 5}
        readonly = 'readonly=%s' % ('on' if is_readonly else 'off')
        expected = [readonly, 'k=v', 'q=w', 'quota=5G']

        result = self.driver._get_dataset_creation_options(
            share=share, is_readonly=is_readonly)

        self.assertEqual(sorted(expected), sorted(result))

    @ddt.data('bar/quuz', 'bar')
    def test__get_dataset_name(self, second_zpool):
        self.configuration.zfs_zpool_list = ['foo', second_zpool]
        prefix = 'fake_prefix_'
        self.configuration.zfs_dataset_name_prefix = prefix
        share = {'id': 'foo-bar_quuz', 'host': 'hostname@backend_name#bar'}
        expected = '%s/%sfoo_bar_quuz' % (second_zpool, prefix)

        result = self.driver._get_dataset_name(share)

        self.assertEqual(expected, result)

    def test_create_share(self):
        mock_get_helper = self.mock_object(self.driver, '_get_share_helper')
        self.mock_object(self.driver, 'zfs')
        context = 'fake_context'
        share = {
            'id': 'fake_share_id',
            'host': 'hostname@backend_name#bar',
            'share_proto': 'NFS',
            'size': 4,
        }
        self.configuration.zfs_dataset_name_prefix = 'some_prefix_'
        self.configuration.zfs_ssh_username = 'someuser'
        self.driver.share_export_ip = '1.1.1.1'
        self.driver.service_ip = '2.2.2.2'
        dataset_name = 'bar/subbar/some_prefix_fake_share_id'

        result = self.driver.create_share(context, share, share_server=None)

        self.assertEqual(
            mock_get_helper.return_value.create_exports.return_value,
            result,
        )
        self.assertEqual(
            'share',
            self.driver.private_storage.get(share['id'], 'entity_type'))
        self.assertEqual(
            dataset_name,
            self.driver.private_storage.get(share['id'], 'dataset_name'))
        self.assertEqual(
            'someuser@2.2.2.2',
            self.driver.private_storage.get(share['id'], 'ssh_cmd'))
        self.assertEqual(
            'bar',
            self.driver.private_storage.get(share['id'], 'pool_name'))
        self.driver.zfs.assert_called_once_with(
            'create', '-o', 'fook=foov', '-o', 'bark=barv',
            '-o', 'readonly=off', '-o', 'quota=4G',
            'bar/subbar/some_prefix_fake_share_id')
        mock_get_helper.assert_has_calls([
            mock.call('NFS'), mock.call().create_exports(dataset_name)
        ])

    def test_create_share_with_share_server(self):
        self.assertRaises(
            exception.ManilaException,
            self.driver.create_share,
            'fake_context', 'fake_share', share_server={'id': 'fake_server'},
        )

    def test_delete_share(self):
        dataset_name = 'bar/subbar/some_prefix_fake_share_id'
        mock_delete = self.mock_object(
            self.driver, '_delete_dataset_or_snapshot_with_retry')
        self.mock_object(self.driver, '_get_share_helper')
        self.mock_object(zfs_driver.LOG, 'warning')
        self.mock_object(
            self.driver, 'zfs', mock.Mock(return_value=('a', 'b')))
        snap_name = '%s@%s' % (
            dataset_name, self.driver.replica_snapshot_prefix)
        self.mock_object(
            self.driver, 'parse_zfs_answer',
            mock.Mock(
                side_effect=[
                    [{'NAME': 'fake_dataset_name'}, {'NAME': dataset_name}],
                    [{'NAME': 'snap_name'},
                     {'NAME': '%s@foo' % dataset_name},
                     {'NAME': snap_name}],
                ]))
        context = 'fake_context'
        share = {
            'id': 'fake_share_id',
            'host': 'hostname@backend_name#bar',
            'share_proto': 'NFS',
            'size': 4,
        }
        self.configuration.zfs_dataset_name_prefix = 'some_prefix_'
        self.configuration.zfs_ssh_username = 'someuser'
        self.driver.share_export_ip = '1.1.1.1'
        self.driver.service_ip = '2.2.2.2'
        self.driver.private_storage.update(
            share['id'],
            {'pool_name': 'bar', 'dataset_name': dataset_name}
        )

        self.driver.delete_share(context, share, share_server=None)

        self.driver.zfs.assert_has_calls([
            mock.call('list', '-r', 'bar'),
            mock.call('list', '-r', '-t', 'snapshot', 'bar'),
        ])
        self.driver._get_share_helper.assert_has_calls([
            mock.call('NFS'), mock.call().remove_exports(dataset_name)])
        self.driver.parse_zfs_answer.assert_has_calls([
            mock.call('a'), mock.call('a')])
        mock_delete.assert_has_calls([
            mock.call(snap_name),
            mock.call(dataset_name),
        ])
        self.assertEqual(0, zfs_driver.LOG.warning.call_count)

    def test_delete_share_absent(self):
        dataset_name = 'bar/subbar/some_prefix_fake_share_id'
        mock_delete = self.mock_object(
            self.driver, '_delete_dataset_or_snapshot_with_retry')
        self.mock_object(self.driver, '_get_share_helper')
        self.mock_object(zfs_driver.LOG, 'warning')
        self.mock_object(
            self.driver, 'zfs', mock.Mock(return_value=('a', 'b')))
        snap_name = '%s@%s' % (
            dataset_name, self.driver.replica_snapshot_prefix)
        self.mock_object(
            self.driver, 'parse_zfs_answer',
            mock.Mock(side_effect=[[], [{'NAME': snap_name}]]))
        context = 'fake_context'
        share = {
            'id': 'fake_share_id',
            'host': 'hostname@backend_name#bar',
            'size': 4,
        }
        self.configuration.zfs_dataset_name_prefix = 'some_prefix_'
        self.configuration.zfs_ssh_username = 'someuser'
        self.driver.share_export_ip = '1.1.1.1'
        self.driver.service_ip = '2.2.2.2'
        self.driver.private_storage.update(share['id'], {'pool_name': 'bar'})

        self.driver.delete_share(context, share, share_server=None)

        self.assertEqual(0, self.driver._get_share_helper.call_count)
        self.assertEqual(0, mock_delete.call_count)
        self.driver.zfs.assert_called_once_with('list', '-r', 'bar')
        self.driver.parse_zfs_answer.assert_called_once_with('a')
        zfs_driver.LOG.warning.assert_called_once_with(
            mock.ANY, {'id': share['id'], 'name': dataset_name})

    def test_delete_share_with_share_server(self):
        self.assertRaises(
            exception.ManilaException,
            self.driver.delete_share,
            'fake_context', 'fake_share', share_server={'id': 'fake_server'},
        )

    def test_create_snapshot(self):
        self.configuration.zfs_dataset_snapshot_name_prefix = 'prfx_'
        self.mock_object(self.driver, 'zfs')
        snapshot = {
            'id': 'fake_snapshot_id',
            'host': 'hostname@backend_name#bar',
            'size': 4,
            'share_id': 'fake_share_id'
        }
        snapshot_name = 'foo_data_set_name@prfx_fake_snapshot_id'
        self.driver.private_storage.update(
            snapshot['share_id'], {'dataset_name': 'foo_data_set_name'})

        self.driver.create_snapshot('fake_context', snapshot)

        self.driver.zfs.assert_called_once_with(
            'snapshot', snapshot_name)
        self.assertEqual(
            snapshot_name,
            self.driver.private_storage.get(
                snapshot['id'], 'snapshot_name'))
