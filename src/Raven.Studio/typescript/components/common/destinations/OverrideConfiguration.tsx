﻿import React, { useState } from "react";
import { InputGroup, InputGroupText, Label } from "reactstrap";
import { FormInput } from "components/common/Form";
import { useForm } from "react-hook-form";

const OverrideConfiguration = () => {
    const { control } = useForm<any>({});
    return (
        <>
            <div>
                <Label className="mb-0 md-label">Exec</Label>
                <FormInput
                    name="exec"
                    control={control}
                    placeholder="Path to executable"
                    className="mb-2"
                    type="text"
                />
            </div>
            <div>
                <Label className="mb-0 md-label">Arguments</Label>
                <FormInput
                    type="text"
                    name="arguments"
                    control={control}
                    placeholder="Command line arguments passed to exec"
                    className="mb-2"
                />
            </div>
            <div>
                <Label className="mb-0 md-label">Timeout</Label>
                <InputGroup>
                    <FormInput name="arguments" control={control} placeholder="10000 (default)" type="number" />
                    <InputGroupText>ms</InputGroupText>
                </InputGroup>
            </div>
        </>
    );
};

export default OverrideConfiguration;