﻿import React, { useState } from "react";
import { Label } from "reactstrap";
import { NodeSet, NodeSetItem, NodeSetLabel, NodeSetListCard } from "./NodeSet";
import { Radio } from "./Radio";

interface SingleDatabaseLocationSelectorProps {
    locations: databaseLocationSpecifier[];
    selectedLocation: databaseLocationSpecifier;
    setSelectedLocation: (location: databaseLocationSpecifier) => void;
}

export function SingleDatabaseLocationSelector(props: SingleDatabaseLocationSelectorProps) {
    const { locations, selectedLocation, setSelectedLocation } = props;
    const [uniqId] = useState(() => _.uniqueId("single-location-selector-"));

    const uniqueNodeTags = [...new Set(locations.map((x) => x.nodeTag))];
    const isNonSharded = uniqueNodeTags.length === locations.length;

    return (
        <div className="bs5">
            {isNonSharded ? (
                <>
                    {locations.map((location, idx) => {
                        const locationId = uniqId + idx;

                        return (
                            <div key={locationId}>
                                <Label className="m-0">
                                    <NodeSet
                                        color="secondary"
                                        className="my-1 p-1 d-flex flex-column align-items-center"
                                    >
                                        <NodeSetItem icon="node" color="node">
                                            {location.nodeTag}
                                        </NodeSetItem>
                                        <Radio
                                            selected={selectedLocation === location}
                                            toggleSelection={() => setSelectedLocation(location)}
                                        />
                                    </NodeSet>
                                </Label>
                            </div>
                        );
                    })}
                </>
            ) : (
                <>
                    {uniqueNodeTags.map((nodeTag, idx) => {
                        const nodeId = uniqId + idx;

                        return (
                            <div key={nodeId}>
                                <NodeSet color="shard" className="my-1">
                                    <NodeSetLabel color="node" icon="node">
                                        {nodeTag}
                                    </NodeSetLabel>

                                    <NodeSetListCard>
                                        {locations
                                            .filter((x) => x.nodeTag === nodeTag)
                                            .map((location) => (
                                                <Label
                                                    key={nodeId + "-shard-" + location.shardNumber}
                                                    className="m-0 p-1 d-flex flex-column align-items-center"
                                                >
                                                    <NodeSetItem color="shard" icon="shard">
                                                        {location.shardNumber}
                                                    </NodeSetItem>
                                                    <Radio
                                                        selected={selectedLocation === location}
                                                        toggleSelection={() => setSelectedLocation(location)}
                                                    ></Radio>
                                                </Label>
                                            ))}
                                    </NodeSetListCard>
                                </NodeSet>
                            </div>
                        );
                    })}
                </>
            )}
        </div>
    );
}