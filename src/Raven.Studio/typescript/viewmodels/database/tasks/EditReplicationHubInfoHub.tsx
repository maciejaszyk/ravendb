﻿import AboutViewFloating, {AboutViewAnchored, AccordionItemWrapper} from "components/common/AboutView";
import AccordionLicenseNotIncluded from "components/common/AccordionLicenseNotIncluded";
import { licenseSelectors } from "components/common/shell/licenseSlice";
import { useAppSelector } from "components/store";
import React from "react";
import {Icon} from "components/common/Icon";

export function EditReplicationHubInfoHub() {
    const isEnterpriseOrDeveloper = useAppSelector(licenseSelectors.isEnterpriseOrDeveloper());

    return (
        <AboutViewFloating defaultOpen={!isEnterpriseOrDeveloper}>
            <AccordionItemWrapper
                targetId="about"
                icon="about"
                color="info"
                heading="About this view"
                description="Get additional info on this feature"
            >
                <p>
                    Text
                </p>
                <hr />
                <div className="small-label mb-2">useful links</div>
                <a href="https://ravendb.net/l/NIH5LN/latest" target="_blank">
                    <Icon icon="newtab" /> Docs - Replication Hub
                </a>
            </AccordionItemWrapper>
            <AboutViewAnchored
                className="mt-2"
                defaultOpen={isEnterpriseOrDeveloper ? null : "licensing"}
            >
                <AccordionLicenseNotIncluded
                    targetId="licensing"
                    featureName="Replication Hub"
                    featureIcon="pull-replication-hub"
                    checkedLicenses={["Enterprise"]}
                    isLimited={!isEnterpriseOrDeveloper}
                />
            </AboutViewAnchored>
        </AboutViewFloating>
    );
}