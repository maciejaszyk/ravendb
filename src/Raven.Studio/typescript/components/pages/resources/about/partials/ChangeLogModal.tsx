﻿import { Button, Modal, ModalBody, ModalFooter, UncontrolledPopover } from "reactstrap";
import { Icon } from "components/common/Icon";
import { FlexGrow } from "components/common/FlexGrow";
import React, { ReactNode } from "react";
import { aboutPageUrls } from "components/pages/resources/about/partials/common";
import { useServices } from "hooks/useServices";
import { useAsync } from "react-async-hook";
import { LazyLoad } from "components/common/LazyLoad";
import { LoadError } from "components/common/LoadError";
import genUtils from "common/generalUtils";

interface ChangelogModalProps {
    mode: "whatsNew" | "changeLog" | "hidden";
    onClose: () => void;
}

export function ChangeLogModal(props: ChangelogModalProps) {
    const { mode, onClose } = props;

    const { licenseService } = useServices();

    //TODO: pagining?
    const asyncGetChangeLog = useAsync(() => licenseService.getChangeLog(), []);

    if (asyncGetChangeLog.loading) {
        return (
            <ModalWrapper onClose={onClose} mode={mode}>
                <LazyLoad active>
                    <h3>VERSION</h3>
                </LazyLoad>

                <LazyLoad active>
                    <div style={{ height: 200 }}></div>
                </LazyLoad>
            </ModalWrapper>
        );
    }

    if (asyncGetChangeLog.status === "error") {
        return (
            <ModalWrapper onClose={onClose} mode={mode}>
                <div className="m-3">
                    <LoadError />
                </div>
            </ModalWrapper>
        );
    }

    if (asyncGetChangeLog.status === "success" && asyncGetChangeLog.result.ErrorMessage) {
        return (
            <ModalWrapper onClose={onClose} mode={mode}>
                <div className="m-3">
                    <LoadError error={asyncGetChangeLog.result.ErrorMessage} />
                </div>
            </ModalWrapper>
        );
    }

    const canUpgrade = asyncGetChangeLog.result.IsLicenseEligibleForUpgrade;
    const versionsList =
        mode === "whatsNew"
            ? asyncGetChangeLog.result.BuildCompatibilitiesForLatestMajorMinor
            : asyncGetChangeLog.result.BuildCompatibilitiesForUserMajorMinor;

    return (
        <ModalWrapper onClose={onClose} mode={mode}>
            <div>
                {mode === "whatsNew" && (
                    <div key="updateLicenseInfo">
                        <div className="well px-3 py-1 small rounded-pill" id="updateLicenseInfo">
                            {canUpgrade ? (
                                <>
                                    <Icon icon="check" color="success" /> License compatible{" "}
                                </>
                            ) : (
                                <>
                                    <Icon icon="license" color="warning" /> Requires License Upgrade{" "}
                                </>
                            )}
                        </div>
                        <UncontrolledPopover trigger="hover" className="bs5" placement="top" target="updateLicenseInfo">
                            <div className="px-2 py-1">
                                {canUpgrade ? (
                                    <>
                                        This update is compatible with your license. In order to upgrade to the latest
                                        version
                                    </>
                                ) : (
                                    <>LatestVersion your license must be updated</>
                                )}
                            </div>
                        </UncontrolledPopover>
                    </div>
                )}

                {versionsList.map((build) => {
                    return (
                        <div key={build.FullVersion}>
                            <h3>
                                {mode === "whatsNew" && (
                                    <>
                                        <strong className="text-warning">NEW</strong> -{" "}
                                    </>
                                )}
                                {build.FullVersion} -{" "}
                                {genUtils.formatUtcDateAsLocal(build.ReleasedAt, genUtils.basicDateFormat)}{" "}
                            </h3>
                            <div className="d-flex gap-3">
                                {build.CanDowngradeFollowingUpgrade && (
                                    <React.Fragment key="updateDowngradeInfo">
                                        <div className="well px-3 py-1 small rounded-pill" id="updateDowngradeInfo">
                                            <Icon icon="check" color="success" /> Can downgrade
                                        </div>
                                        <UncontrolledPopover
                                            trigger="hover"
                                            className="bs5"
                                            placement="top"
                                            target="updateDowngradeInfo"
                                        >
                                            <div className="px-2 py-1">
                                                This update is safe to revert to current version
                                            </div>
                                        </UncontrolledPopover>
                                    </React.Fragment>
                                )}
                            </div>
                            <div
                                className="mt-4 vstack gap-2"
                                dangerouslySetInnerHTML={{ __html: build.ChangelogHtml }}
                            ></div>
                        </div>
                    );
                })}
            </div>
        </ModalWrapper>
    );
}

function ModalWrapper(props: { children: ReactNode } & ChangelogModalProps) {
    const { onClose, children, mode } = props;
    return (
        <Modal
            isOpen
            toggle={onClose}
            wrapClassName="bs5"
            centered
            size="lg"
            contentClassName="modal-border bulge-warning"
        >
            <ModalBody className="vstack gap-4 position-relative">
                <div className="text-center">
                    <Icon icon="logs" color="warning" className="fs-1" margin="m-0" />
                </div>

                <div className="position-absolute m-2 end-0 top-0">
                    <Button close onClick={onClose} />
                </div>
                <div className="text-center lead">{mode === "whatsNew" ? "What's New" : "Changelog"}</div>
                {children}
            </ModalBody>
            <ModalFooter>
                <Button color="secondary" outline onClick={onClose} className="rounded-pill px-3">
                    Close
                </Button>

                {mode === "whatsNew" && (
                    <React.Fragment key="footer-part">
                        <FlexGrow />
                        <Button color="primary" className="rounded-pill px-3" href={aboutPageUrls.updateInstructions}>
                            Update instructions <Icon icon="newtab" margin="m-0" />
                        </Button>
                    </React.Fragment>
                )}
            </ModalFooter>
        </Modal>
    );
}
