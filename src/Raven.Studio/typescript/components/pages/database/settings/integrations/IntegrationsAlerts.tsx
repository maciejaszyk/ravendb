import { Icon } from "components/common/Icon";
import { useRavenLink } from "components/hooks/useRavenLink";
import React from "react";
import { Alert } from "reactstrap";

interface IntegrationsAlertsProps {
    isLicenseUpgradeRequired: boolean;
    isPostgreSqlSupportEnabled: boolean;
}

export default function IntegrationsAlerts(props: IntegrationsAlertsProps) {
    const { isLicenseUpgradeRequired, isPostgreSqlSupportEnabled } = props;

    const postgreSqlDocsLink = useRavenLink({ hash: "HDTCH7" });

    if (isLicenseUpgradeRequired) {
        return (
            <Alert color="warning">
                <Icon icon="warning" />
                <span>
                    To use this feature, your license must include either of the following features: PostgreSQL
                    integration or Power BI.
                </span>
            </Alert>
        );
    }

    if (!isLicenseUpgradeRequired && !isPostgreSqlSupportEnabled) {
        return (
            <Alert color="warning" className="mt-3">
                PostgreSQL support must be explicitly enabled in your <code>settings.json</code> file. Learn more{" "}
                <a href={postgreSqlDocsLink}>here</a>.
            </Alert>
        );
    }

    return null;
}
