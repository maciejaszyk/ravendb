﻿import activeDatabaseTracker from "common/shell/activeDatabaseTracker";
import { globalDispatch } from "components/storeCompat";
import databasesManager from "common/shell/databasesManager";
import clusterTopologyManager from "common/shell/clusterTopologyManager";
import { databaseActions } from "components/common/shell/databaseSliceActions";
import { clusterActions } from "components/common/shell/clusterSlice";

let initialized = false;

function updateReduxStore() {
    const dtos = databasesManager.default.databases().map((x) => x.toDto());
    globalDispatch(databaseActions.databasesLoaded(dtos));
}

const throttledUpdateReduxStore = _.throttle(() => updateReduxStore(), 200);

export function initRedux() {
    if (initialized) {
        return;
    }

    initialized = true;

    databasesManager.default.onUpdateCallback = throttledUpdateReduxStore;
    activeDatabaseTracker.default.database.subscribe((db) =>
        globalDispatch(databaseActions.activeDatabaseChanged(db?.name ?? null))
    );

    clusterTopologyManager.default.localNodeTag.subscribe((tag) => {
        globalDispatch(clusterActions.localNodeTagLoaded(tag));
    });

    const onClusterTopologyChanged = () => {
        const nodes =
            clusterTopologyManager.default
                .topology()
                ?.nodes()
                .map((x) => x.tag()) ?? [];
        globalDispatch(clusterActions.nodeTagsLoaded(nodes));
    };

    clusterTopologyManager.default.topology.subscribe((topology) => {
        onClusterTopologyChanged();
        topology.nodes.subscribe(onClusterTopologyChanged);
    });
}