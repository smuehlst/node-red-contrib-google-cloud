// Copyright 2019 Google Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
/* jshint esversion: 8 */
module.exports = function(RED) {
    "use strict";

    const STATUS_CONNECTED = {
        fill:  "green",
        shape: "dot",
        text:  "connected"
    };

    const STATUS_DISCONNECTED = {
        fill:  "red",
        shape: "dot",
        text:  "disconnected"
    };

    const STATUS_CONNECTING = {
        fill:  "yellow",
        shape: "dot",
        text:  "connecting"
    };

    const STATUS_PUBLISHING = {
        fill:  "green",
        shape: "ring",
        text:  "publishing"
    };

    const {PubSub} = require("@google-cloud/pubsub");

    /**
     * Extract JSON service account key from "google-cloud-credentials" config node.
     */
    function GetCredentials(node) {
        return JSON.parse(RED.nodes.getCredentials(node).account);
    }


    function GoogleCloudPubSubOutNode(config) {
        let topic  = null;
        let pubsub = null;
        const messageQueue = [];
        RED.nodes.createNode(this, config);

        const node = this;

        let credentials = null;
        if (config.account) {
            credentials = GetCredentials(config.account);
        }
        const keyFilename = config.keyFilename;

        if (!config.topic) {
            node.error('No topic supplied!');
            return;
        }

        node.status(STATUS_DISCONNECTED);

        // Called to publish a new message.
        async function OnInput(msg) {
            if (msg == null || !msg.payload || msg.payload == "") {
                return;
            }

            // If we don't have a topic, then this is likely because we have received a request to
            // publish a message while we are still waiting for access to the topic from GCP.
            if (topic === null) {
                messageQueue.push(msg);
                return;
            }
            node.status(STATUS_PUBLISHING);
            try {
                await topic.publish(RED.util.ensureBuffer(msg.payload));
                node.status(STATUS_CONNECTED);
            }
            catch(e) {
                node.status(STATUS_DISCONNECTED);
                node.error(e);
            }
        }

        function OnClose() {
            node.status(STATUS_DISCONNECTED);
            pubsub = null;
        }

        // We must have EITHER credentials or a keyFilename.  If neither are supplied, that
        // is an error.  If both are supplied, then credentials will be used.
        if (credentials) {
            pubsub = new PubSub({
                "credentials": credentials
            });
        } else if (keyFilename) {
            pubsub = new PubSub({
                "keyFilename": keyFilename
            });
        } else {
            node.error('Missing credentials or keyFilename.');
            return;
        }

        node.status(STATUS_CONNECTING);

        pubsub.topic(config.topic).get().then((data) => {
            topic = data[0];
            node.status(STATUS_CONNECTED);

            // We may have been asked to process messages BEFORE the topic access has been received.  In this case,
            // the messages will have been queued.  We pull the messages from the queue and publish them one at
            // a time.
            while(messageQueue.length > 0) {
                const msg = messageQueue.shift();
                topic.publish(RED.util.ensureBuffer(msg.payload));
            }
        }).catch((reason) => {
            node.status(STATUS_DISCONNECTED);
            node.error(reason);
        });

        node.on('close', OnClose);
        node.on('input', OnInput);
    } // GoogleCloudPubSubOutNode

    RED.nodes.registerType("google-cloud-pubsub out", GoogleCloudPubSubOutNode);

    function GoogleCloudPubSubCommandNode(config) {
        RED.nodes.createNode(this, config);

        const node = this,
            credentials = GetCredentials(config.account),
            state = {
                client: null,
                deviceId: config.deviceId,
                registryId: config.registryId,
                projectId: config.projectId,
                cloudRegion: config.cloudRegion,
            };

        node.status(STATUS_CONNECTING);

        // sends a command to a specified device subscribed to the commands topic
        function sendCommand(
            deviceId,
            registryId,
            projectId,
            cloudRegion,
            commandMessage
        ) {
            // [START iot_send_command]
            const parentName = `projects/${projectId}/locations/${cloudRegion}`;
            const registryName = `${parentName}/registries/${registryId}`;

            const binaryData = Buffer.from(commandMessage).toString('base64');

            // NOTE: The device must be subscribed to the wildcard subfolder
            // or you should pass a subfolder
            const request = {
                name: `${registryName}/devices/${deviceId}`,
                binaryData: binaryData,
                //subfolder: <your-subfolder>
            };

            state.client.projects.locations.registries.devices.sendCommandToDevice(
                request,
                (err, data) => {
                    if (err) {
                        node.error('Could not send command: ' + JSON.stringify(request));
                        node.error('Error: ' + err);
                    }
                }
            );
            // [END iot_send_command]
        }

        function OnInput(message) {
            if (message == null || !message.payload || message.payload == "")
                return;

            sendCommand(
                message.deviceId || state.deviceId,
                message.registryId || state.registryId,
                message.projectId || state.projectId,
                message.cloudRegion || state.cloudRegion,
                message.payload
            );
        }

        function OnClose() {
            state.client = null;
            state.deviceId = null;
            state.registryId = null;
            state.projectId = null;
            state.cloudRegion = null;
            node.removeListener("input", OnInput);
            node.status(STATUS_DISCONNECTED);
        }

        if (credentials) {
            google.auth
                .getClient({
                    credentials: credentials,
                    scopes: ['https://www.googleapis.com/auth/cloud-platform'],
                })
                .then(authClient => {
                    const discoveryUrl = `${DISCOVERY_API}?version=${API_VERSION}`;

                    google.options({
                        auth: authClient,
                    });

                    google
                        .discoverAPI(discoveryUrl)
                        .then(client => {
                            state.client = client;
                            node.status(STATUS_CONNECTED);
                        })
                        .catch(err => {
                            node.log('Error during API discovery.' + err);
                            node.status(STATUS_DISCONNECTED);
                        });
                });
        } else {
            node.error("missing credentials");
            node.status(STATUS_DISCONNECTED);
        }

        node.on("input", OnInput);
        node.on("close", OnClose);
    }

    RED.nodes.registerType("google-cloud-iot-command out", GoogleCloudPubSubCommandNode);
};
