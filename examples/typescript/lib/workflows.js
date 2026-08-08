"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.greetingWorkflow = greetingWorkflow;
const workflow_1 = require("@temporalio/workflow");
const { composeGreeting } = (0, workflow_1.proxyActivities)({
    startToCloseTimeout: '10 seconds',
});
async function greetingWorkflow(name) {
    return await composeGreeting(name);
}
