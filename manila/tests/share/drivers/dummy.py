# Copyright 2016 Mirantis inc.
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

"""
Dummy share driver for testing Manila APIs and other interfaces.

This driver simulates support of:
- Both available driver modes: DHSS=True/False
- NFS and CIFS protocols
- IP access for NFS shares and USER access for CIFS shares
- CIFS shares in DHSS=True driver mode (requires 'active_directory' be set up)
- Creation and deletion of share snapshots
- Share replication (readable)
- Share migration
- Consistency groups
- Resize of a share (extend/shrink)

"""

from oslo_log import log

from manila.common import constants
from manila import exception
from manila.i18n import _
from manila.share import driver
from manila.share import utils as share_utils

LOG = log.getLogger(__name__)


class DummyDriver(driver.ShareDriver):
    """Dummy share driver that implements all share driver interfaces."""

    def __init__(self, *args, **kwargs):
        """Do initialization."""
        super(self.__class__, self).__init__([False, True], *args, **kwargs)
        self.private_storage = kwargs.get('private_storage')
        self.backend_name = self.configuration.safe_get(
            "share_backend_name") or "DummyDriver"
        self.migration_progress = {}

    def _get_share_name(self, share):
        return "share_%(s_id)s_%(si_id)s" % {
            "s_id": share["share_id"].replace("-", "_"),
            "si_id": share["id"].replace("-", "_")}

    def _get_snapshot_name(self, snapshot):
        return "snapshot_%(s_id)s_%(si_id)s" % {
            "s_id": snapshot["snapshot_id"].replace("-", "_"),
            "si_id": snapshot["id"].replace("-", "_")}

    def _generate_export_locations(self, mountpoint):
        return [
            {
                "path": "%(ip)s:%(mp)s" % {"ip": ip, "mp": mountpoint},
                "metadata": {
                    "preferred": preferred,
                },
                "is_admin_only": is_admin_only,
            } for ip, is_admin_only, preferred in (
                (self.primary_public_ip, False, True),
                (self.secondary_public_ip, False, False),
                (self.service_ip, True, False))
        ]

    def _create_share(self, share):
        share_proto = share["share_proto"]
        if share_proto not in ("NFS", "CIFS"):
            msg = _("Unsupported share protocol provided - %s.") % share_proto
            raise exception.InvalidShareAccess(reason=msg)

        share_name = self._get_share_name(share)
        mountpoint = "/path/to/fake/share/%s" % share_name
        self.private_storage.update(
            share["id"], {
                "fake_provider_share_name": share_name,
                "fake_provider_location": mountpoint,
            }
        )
        return self._generate_export_locations(mountpoint)

    def create_share(self, context, share, share_server=None):
        """Is called to create share."""
        return self._create_share(share)

    def create_share_from_snapshot(self, context, share, snapshot,
                                   share_server=None):
        """Is called to create share from snapshot."""
        return self._create_share(share)

    def _create_snapshot(self, snapshot):
        snapshot_name = self._get_snapshot_name(snapshot)
        self.private_storage.update(
            snapshot["id"], {
                "fake_provider_snapshot_name": snapshot_name,
            }
        )

    def create_snapshot(self, context, snapshot, share_server=None):
        """Is called to create snapshot."""
        return self._create_snapshot(snapshot)

    def delete_share(self, context, share, share_server=None):
        """Is called to remove share."""
        self.private_storage.delete(share["id"])

    def delete_snapshot(self, context, snapshot, share_server=None):
        """Is called to remove snapshot."""
        self.private_storage.delete(snapshot["id"])

    def get_pool(self, share):
        """Return pool name where the share resides on."""
        pool_name = share_utils.extract_host(share["host"], level="pool")
        return pool_name

    def ensure_share(self, context, share, share_server=None):
        """Invoked to ensure that share is exported."""

    def update_access(self, context, share, access_rules, add_rules,
                      delete_rules, share_server=None):
        """Update access rules for given share."""
        for rule in access_rules:
            share_proto = share["share_proto"]
            access_type = rule["access_type"].lower()
            if share_proto == "NFS" and access_type != "ip":
                msg = _("Only IP access type allowed for NFS protocol.")
                raise exception.InvalidShareAccess(reason=msg)
            elif share_proto == "CIFS" and access_type != "user":
                msg = _("Only USER access type allowed for CIFS protocol.")
                raise exception.InvalidShareAccess(reason=msg)
            else:
                msg = _("Unsupported access type provided - %s.") % access_type
                raise exception.InvalidShareAccess(reason=msg)

    def do_setup(self, context):
        """Any initialization the share driver does while starting."""

    def manage_existing(self, share, driver_options):
        """Brings an existing share under Manila management."""
        return self._create_share(share)

    def unmanage(self, share):
        """Removes the specified share from Manila management."""

    def manage_existing_snapshot(self, snapshot, driver_options):
        """Brings an existing snapshot under Manila management."""
        return self._create_snapshot(snapshot)

    def unmanage_snapshot(self, snapshot):
        """Removes the specified snapshot from Manila management."""

    def extend_share(self, share, new_size, share_server=None):
        """Extends size of existing share."""

    def shrink_share(self, share, new_size, share_server=None):
        """Shrinks size of existing share."""

    def get_network_allocations_number(self):
        """Returns number of network allocations for creating VIFs."""
        return 2

    def get_admin_network_allocations_number(self):
        return 1

    def _setup_server(self, network_info, metadata=None):
        """Sets up and configures share server with given network parameters.

        Redefine it within share driver when it is going to handle share
        servers.
        """
        server_details = {
            "primary_public_ip": network_info[
                "network_allocations"][0]["ip_address"],
            "secondary_public_ip": network_info[
                "network_allocations"][1]["ip_address"],
            "service_ip": network_info[
                "admin_network_allocations"][0]["ip_address"],
            "username": "fake_username",
        }
        return server_details

    def _teardown_server(self, server_details, security_services=None):
        """Tears down share server."""

    def _get_pools_info(self):
        pools = [{
            "pool_name": "the_dummiest_pool_ever",
            "total_capacity_gb": 1230.0,
            "free_capacity_gb": 1210.0,
            "reserved_percentage": self.configuration.reserved_share_percentage
        }]
        if self.configuration.replication_domain:
            pools[0]["replication_type"] = "readable"
        return pools

    def _update_share_stats(self, data=None):
        """Retrieve stats info from share group."""
        data = {
            "share_backend_name": self.backend_name,
            "storage_protocol": "NFS_CIFS",
            "reserved_percentage":
                self.configuration.reserved_share_percentage,
            "consistency_group_support": "pool",
            "snapshot_support": True,
            "driver_name": "Dummy",
            "pools": self._get_pools_info(),
        }
        if self.configuration.replication_domain:
            data["replication_type"] = "readable"
        super(self.__class__, self)._update_share_stats(data)

    def get_share_server_pools(self, share_server):
        """Return list of pools related to a particular share server."""
        return []

    def create_consistency_group(self, context, cg_dict, share_server=None):
        """Create a consistency group."""
        LOG.debug(
            "Successfully created dummy Consistency Group with ID: %s.",
            cg_dict["id"])

    def delete_consistency_group(self, context, cg_dict, share_server=None):
        """Delete a consistency group."""
        LOG.debug(
            "Successfully deleted dummy consistency group with ID %s.",
            cg_dict["id"])

    def create_cgsnapshot(self, context, snap_dict, share_server=None):
        """Create a consistency group snapshot."""
        LOG.debug("Successfully created CG snapshot %s." % snap_dict["id"])
        return None, None

    def delete_cgsnapshot(self, context, snap_dict, share_server=None):
        """Delete a consistency group snapshot."""
        LOG.debug("Successfully deleted CG snapshot %s." % snap_dict["id"])
        return None, None

    def create_consistency_group_from_cgsnapshot(
            self, context, cg_dict, cgsnapshot_dict, share_server=None):
        """Create a consistency group from a cgsnapshot."""
        LOG.debug(
            ("Successfully created dummy Consistency Group (%(cg_id)s) "
             "from CG snapshot (%(cg_snap_id)s)."),
            {"cg_id": cg_dict["id"], "cg_snap_id": cgsnapshot_dict["id"]})
        return None, []

    def migration_start(self, context, share_ref, share_server, host,
                        dest_driver_migration_info, notify):
        """Migrates share to another host."""
        LOG.debug(
            "Migration of dummy share with ID '%s' has been started." %
            share_ref["id"])
        return None, None

    def migration_complete(self, context, share_ref, share_server,
                           dest_driver_migration_info):
        LOG.debug(
            "Migration of dummy share with ID '%s' has been completed." %
            share_ref["id"])

    def migration_cancel(self, context, share_ref, share_server,
                         dest_driver_migration_info):
        LOG.debug(
            "Migration of dummy share with ID '%s' has been canceled." %
            share_ref["id"])

    def migration_get_progress(self, context, share_ref, share_server,
                               dest_driver_migration_info):
        # Simulate migration progress.
        if share_ref["id"] not in self.migration_progress:
            self.migration_progress[share_ref["id"]] = total_progress = 25
        elif self.migration_progress[share_ref["id"]] == 100:
            total_progress = self.migration_progress.pop(share_ref["id"])
        else:
            self.migration_progress[share_ref["id"]] += 25
            total_progress = self.migration_progress[share_ref["id"]]
        LOG.debug(
            ("Progress of current dummy share migration "
             "with ID '%(id)s' is %(progress).") % {
                "id": share_ref["id"], "progress": total_progress})
        return {"total_progress": total_progress}

    def create_replica(self, context, replica_list, new_replica,
                       access_rules, replica_snapshots, share_server=None):
        """Replicate the active replica to a new replica on this backend."""
        replica_name = self._get_share_name(new_replica)
        mountpoint = "/path/to/fake/share/%s" % replica_name
        self.private_storage.update(
            new_replica["id"], {
                "fake_provider_replica_name": replica_name,
                "fake_provider_location": mountpoint,
            }
        )
        return {
            "export_locations": self._generate_export_locations(mountpoint),
            "replica_state": constants.REPLICA_STATE_IN_SYNC,
            "access_rules_status": constants.STATUS_ACTIVE,
        }

    def delete_replica(self, context, replica_list, replica_snapshots,
                       replica, share_server=None):
        """Delete a replica."""
        self.private_storage.delete(replica["id"])

    def promote_replica(self, context, replica_list, replica, access_rules,
                        share_server=None):
        """Promote a replica to 'active' replica state."""
        return_replica_list = []
        for r in replica_list:
            if r["id"] == replica["id"]:
                r["replica_state"] = constants.REPLICA_STATE_ACTIVE
            else:
                r["replica_state"] = constants.REPLICA_STATE_IN_SYNC
            return_replica_list.append(r)
        return return_replica_list

    def update_replica_state(self, context, replica_list, replica,
                             access_rules, replica_snapshots,
                             share_server=None):
        """Update the replica_state of a replica."""
        return constants.REPLICA_STATE_IN_SYNC

    def create_replicated_snapshot(self, context, replica_list,
                                   replica_snapshots, share_server=None):
        """Create a snapshot on active instance and update across the replicas.

        """
        return_replica_snapshots = []
        for r in replica_snapshots:
            return_replica_snapshots.append(
                {"id": r["id"], "status": constants.STATUS_AVAILABLE})
        return return_replica_snapshots

    def delete_replicated_snapshot(self, context, replica_list,
                                   replica_snapshots, share_server=None):
        """Delete a snapshot by deleting its instances across the replicas."""
        return_replica_snapshots = []
        for r in replica_snapshots:
            return_replica_snapshots.append(
                {"id": r["id"], "status": constants.STATUS_DELETED})
        return return_replica_snapshots

    def update_replicated_snapshot(self, context, replica_list,
                                   share_replica, replica_snapshots,
                                   replica_snapshot, share_server=None):
        """Update the status of a snapshot instance that lives on a replica."""
        return {
            "id": replica_snapshot["id"], "status": constants.STATUS_AVAILABLE}
