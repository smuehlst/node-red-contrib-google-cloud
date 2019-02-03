// Copyright 2017 Google Inc.
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

module.exports = function(RED) {
    "use strict";

    const STATUS_CONNECTED = {
        fill: "green",
        shape: "dot",
        text: "connected"
    };

    const STATUS_DISCONNECTED = {
        fill: "red",
        shape: "dot",
        text: "disconnected"
    };

    const STATUS_CONNECTING = {
        fill: "yellow",
        shape: "dot",
        text: "connecting"
    };

    const STATUS_PUBLISHING = {
        fill: "green",
        shape: "ring",
        text: "publishing"
    };

    const {PubSub} = require("@google-cloud/pubsub");
    const {google} = require('googleapis');

    const API_VERSION = 'v1';
    const DISCOVERY_API = 'https://cloudiot.googleapis.com/$discovery/rest';

    /**
     * Extract JSON service account key from "google-cloud-credentials" config node.
     */
    function GetCredentials(node) {
        return JSON.parse(RED.nodes.getCredentials(node).account);
    }

    /**
     * Attempt to translate MQTT-like messages to PubSub.
     */
    function MqttToPubSub(message) {
        const date = new Date(message.time || Date.now());
        return Buffer.from(
            JSON.stringify({
                data: message.payload,
                attributes: {
                    timestamp: date.toISOString()
                }
        }));
    }

    /**
     * Attempt to translate PubSub messages to MQTT-like.
     */
    function PubSubToMqtt(subscription, topic, message) {
        const path = subscription.name.split("/");
        return {
            payload: message.data,
            attributes: message.attributes,
            time: Date.parse(message.publishTime),
            project: path[path.length - 3],
            topic: topic,
            subscription: path[path.length - 1],
            resource: subscription.name
        };
    }

    function GoogleCloudPubSubInNode(config) {
        RED.nodes.createNode(this, config);

        const node = this,
              credentials = GetCredentials(config.account),
              state = {
                  pubsub: null,
                  topic: null,
                  subscription: null,
                  autogenerated: null,
                  done: null
              };

        node.status(STATUS_DISCONNECTED);

        function OnMessage(message) {
            if (message == null)
                return;
            node.send(PubSubToMqtt(state.subscription, config.topic, message));
            message.ack();
        }

        function OnError(error) {
            if (error == null)
                return;
            node.status(STATUS_DISCONNECTED);
            state.subscription.removeListener("message", OnMessage);
            state.subscription.removeListener("error", OnError);
            node.error(error);
        }

        function OnDelete(error) {
            if (error != null) {
                node.error(error);
            }
            if (state.done) {
                state.done();
            }
        }

        function OnClose(done) {
            node.status(STATUS_DISCONNECTED);
            if (state.subscription) {
                state.subscription.removeListener("message", OnMessage);
                state.subscription.removeListener("error", OnError);
                if (state.autogenerated === true) {
                    state.subscription.delete(OnDelete);
                    state.done = done;
                }
                state.subscription = null;
            }
            state.topic = null;
            state.pubsub = null;
            if (state.autogenerated !== true) {
                done();
            }
        }

        function OnTopic(error, topic, apiResponse) {
            if (error == null) {
                state.topic = topic;
                var options = {};
                if (config.ackDeadlineSeconds) {
                    options.ackDeadlineSeconds = config.ackDeadlineSeconds;
                }
                if (config.encoding) {
                    options.encoding = config.encoding;
                }
                if (config.interval) {
                    options.interval = config.interval;
                }
                if (config.timeout) {
                    options.timeout = config.timeout;
                }
                if (config.subscription && config.subscription != "") {
                    state.autogenerated = false;
                    node.status(STATUS_CONNECTED);
                    state.subscription = state.topic.subscription(config.subscription);
                    state.subscription.on("message", OnMessage);
                    state.subscription.on("error", OnError);
                } else {
                    state.autogenerated = true;
                    // TODO how to deal with autogenerated subscription?
                    node.status(STATUS_DISCONNECTED);
                    node.error("TODO: No autogenerated subscription");
                }
            } else if (error.code === 409) {
                state.pubsub.topic(config.topic).get({
                    autoCreate: true
                }, OnTopic);
            } else {
                node.status(STATUS_DISCONNECTED);
                node.error(error);
            }
        }

        if (credentials) {
            state.pubsub = new PubSub({
                credentials: credentials
            });
            node.status(STATUS_CONNECTING);
            state.pubsub.topic(config.topic).get({
                autoCreate: true
            }, OnTopic);
        } else {
            node.error("missing credentials");
        }

        node.on("close", OnClose);
    }
    RED.nodes.registerType("google-cloud-pubsub in", GoogleCloudPubSubInNode);

    function GoogleCloudPubSubOutNode(config) {
        RED.nodes.createNode(this, config);

        const node = this,
              credentials = GetCredentials(config.account),
              state = {
                  pubsub: null,
                  topic: null,
                  done: null,
                  pending: 0
              };

        node.status(STATUS_DISCONNECTED);

        function OnInput(message) {
            if (message == null || !message.payload || message.payload == "")
                return;
            state.topic
                .publisher()
                .publish(MqttToPubSub(message))
                .then(messageId => {
                    state.pending -= 1;

                    if (state.pending == 0) {
                        node.status(STATUS_CONNECTED);
                    }

                    if (state.done && state.pending == 0) {
                        node.status(STATUS_DISCONNECTED);
                        state.done();
                    }
                })
                .catch(err => {
                    node.error(error);
                });
            if (state.pending == 0)
                node.status(STATUS_PUBLISHING);
            state.pending += 1;
        }

        function OnClose(done) {
            state.pubsub = null;
            state.topic = null;
            node.removeListener("input", OnInput);
            if (state.pending == 0) {
                node.status(STATUS_DISCONNECTED);
                done();
            } else {
                state.done = done;
            }
        }

        function OnTopic(error, topic) {
            if (error == null) {
                state.topic = topic;
                node.status(STATUS_CONNECTED);
                node.on("input", OnInput);
            } else if (error.code === 409) {
                state.pubsub.topic(config.topic).get({
                    autoCreate: true
                }, OnTopic);
            } else {
                node.status(STATUS_DISCONNECTED);
                node.error(error);
            }
        }

        if (credentials) {
            state.pubsub = new PubSub({
                credentials: credentials
            });
            node.status(STATUS_CONNECTING);
            state.pubsub.topic(config.topic).get({
                autoCreate: true
            }, OnTopic);
        } else {
            node.error("missing credentials");
        }

        node.on("close", OnClose);
    }
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
}
